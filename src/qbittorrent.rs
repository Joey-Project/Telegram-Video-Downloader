//! Bounded, local-only qBittorrent WebUI API client.

use std::fmt;
use std::path::{Component, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail, ensure};
use futures_util::StreamExt;
use reqwest::header::{COOKIE, ORIGIN, REFERER, SET_COOKIE};
use reqwest::{Method, Response, StatusCode, Url};
use serde_json::Value;
use tokio::sync::Mutex;

use crate::torrent_types::{
    DEFAULT_MAX_FILES, DEFAULT_MAX_TORRENT_BYTES, MAX_API_TORRENTS, RemoteTorrentFile,
    SubmitDisposition, SubmitResult, TorrentIdentity, TorrentRemoteSnapshot, TorrentSettings,
};

const MAX_AUTH_RESPONSE_BYTES: usize = 1_024;
const MAX_VERSION_RESPONSE_BYTES: usize = 128;
const MAX_API_STRING_BYTES: usize = 4_096;
const MAX_API_FILE_NAME_BYTES: usize = 4_096;

#[derive(Clone)]
pub struct QbittorrentClient {
    client: reqwest::Client,
    base_url: Url,
    referer: String,
    username: Option<Arc<str>>,
    password: Option<Arc<str>>,
    timeout: Duration,
    max_torrent_bytes: usize,
    max_api_response_bytes: usize,
    max_files: usize,
    sid: Arc<Mutex<Option<String>>>,
    major_version: Arc<Mutex<Option<u64>>>,
}

impl fmt::Debug for QbittorrentClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QbittorrentClient")
            .field("base_url", &self.base_url)
            .field("credentials_configured", &self.username.is_some())
            .field("timeout", &self.timeout)
            .field("max_torrent_bytes", &self.max_torrent_bytes)
            .field("max_api_response_bytes", &self.max_api_response_bytes)
            .field("max_files", &self.max_files)
            .finish()
    }
}

impl QbittorrentClient {
    pub fn new(settings: &TorrentSettings) -> Result<Self> {
        validate_settings(settings)?;
        let base_url = Url::parse(&settings.base_url).context("qBittorrent URL is invalid")?;
        let referer = format!("{}/", base_url.origin().ascii_serialization());
        let timeout = Duration::from_secs(settings.request_timeout_seconds);
        let client = reqwest::Client::builder()
            .connect_timeout(timeout)
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .user_agent("telegram-video-downloader/0.1")
            .build()
            .context("could not initialize qBittorrent HTTP client")?;
        Ok(Self {
            client,
            base_url,
            referer,
            username: settings.username.clone().map(Arc::from),
            password: settings.password.clone().map(Arc::from),
            timeout,
            max_torrent_bytes: settings.max_torrent_bytes,
            max_api_response_bytes: settings.max_api_response_bytes,
            max_files: settings.max_files,
            sid: Arc::new(Mutex::new(None)),
            major_version: Arc::new(Mutex::new(None)),
        })
    }

    /// Read-only lookup by every full identifier and, for v2-only hashes, its qBittorrent
    /// 40-hex torrent ID prefix. A prefix-only result is accepted only for one unique candidate;
    /// full v2 hashes from response fields or a `btmh` magnet identifier take precedence.
    pub async fn lookup(&self, identity: &TorrentIdentity) -> Result<Vec<TorrentRemoteSnapshot>> {
        ensure!(
            identity.v1.is_some() || identity.v2.is_some(),
            "torrent identity is empty"
        );
        let mut query_hashes = Vec::new();
        if let Some(v1) = identity.v1.as_deref() {
            query_hashes.push(v1.to_ascii_lowercase());
        }
        if let Some(v2) = identity.v2.as_deref() {
            query_hashes.push(v2.to_ascii_lowercase());
            if let Some(prefix) = v2.get(..40) {
                query_hashes.push(prefix.to_ascii_lowercase());
            }
        }
        query_hashes.sort();
        query_hashes.dedup();
        let hashes = query_hashes.join("|");
        let values = self
            .api_json(
                Method::GET,
                "torrents/info",
                Some(&[("hashes", hashes.as_str())]),
            )
            .await?;
        let records = values
            .as_array()
            .ok_or_else(|| anyhow!("qBittorrent returned an invalid torrent list"))?;
        ensure!(
            records.len() <= MAX_API_TORRENTS,
            "qBittorrent returned too many torrents"
        );

        let mut candidates = Vec::new();
        for record in records {
            let parsed = parse_torrent_info(record)?;
            if parsed.matches_identity(identity) {
                candidates.push(parsed);
            }
        }
        candidates.sort_by(|left, right| left.hash.cmp(&right.hash));
        candidates.dedup_by(|left, right| left.hash == right.hash);

        let mut snapshots = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            snapshots.push(self.finish_snapshot(candidate).await?);
        }
        Ok(snapshots)
    }

    /// Submit exactly once. Callers must reconcile the identity with `lookup` regardless of
    /// response status or transport outcome; an HTTP success alone never proves admission.
    pub async fn submit(&self, raw_torrent: Vec<u8>, file_name: String) -> Result<SubmitResult> {
        ensure!(!raw_torrent.is_empty(), "torrent metainfo is empty");
        ensure!(
            raw_torrent.len() <= self.max_torrent_bytes,
            "torrent metainfo exceeds the configured byte limit"
        );
        let file_name = safe_upload_file_name(&file_name);
        let major = self.version_major().await?;
        let part = reqwest::multipart::Part::bytes(raw_torrent)
            .file_name(file_name)
            .mime_str("application/x-bittorrent")
            .context("could not construct qBittorrent upload")?;
        let mut form = reqwest::multipart::Form::new().part("torrents", part);
        if major >= 5 {
            // qBittorrent 5's API calls active tasks "stopped"; false explicitly preserves
            // normal immediate-start behavior without changing the client's save path/category.
            form = form.text("stopped", "false");
        } else {
            form = form.text("paused", "false");
        }
        let mut form = Some(form);
        let response = self
            .request_with_auth(
                move |sid| {
                    self.request(Method::POST, "torrents/add")
                        .header(REFERER, &self.referer)
                        .header(ORIGIN, self.base_url.origin().ascii_serialization())
                        .maybe_cookie(sid)
                        .multipart(form.take().expect("submit request is sent at most once"))
                },
                false,
            )
            .await?;
        let status = response.status();
        let body = read_limited(response, MAX_AUTH_RESPONSE_BYTES).await?;
        let body_text = String::from_utf8_lossy(&body);
        let disposition = match status {
            StatusCode::OK if body_text.trim().eq_ignore_ascii_case("ok.") => {
                SubmitDisposition::Accepted
            }
            StatusCode::OK | StatusCode::ACCEPTED => SubmitDisposition::Unknown,
            StatusCode::CONFLICT => SubmitDisposition::Conflict,
            StatusCode::UNSUPPORTED_MEDIA_TYPE => SubmitDisposition::InvalidMetainfo,
            _ => SubmitDisposition::Unknown,
        };
        Ok(SubmitResult {
            http_status: status.as_u16(),
            disposition,
        })
    }

    pub async fn refresh(&self, hash: &str) -> Result<TorrentRemoteSnapshot> {
        validate_hash_parameter(hash)?;
        let response = self
            .api_json(Method::GET, "torrents/info", Some(&[("hashes", hash)]))
            .await?;
        let records = response
            .as_array()
            .ok_or_else(|| anyhow!("qBittorrent returned an invalid torrent list"))?;
        let record = records
            .iter()
            .find(|record| {
                record
                    .get("hash")
                    .and_then(Value::as_str)
                    .is_some_and(|value| value.eq_ignore_ascii_case(hash))
            })
            .ok_or_else(|| anyhow!("qBittorrent torrent was not found"))?;
        self.finish_snapshot(parse_torrent_info(record)?).await
    }

    async fn finish_snapshot(
        &self,
        mut snapshot: TorrentRemoteSnapshot,
    ) -> Result<TorrentRemoteSnapshot> {
        let response = self
            .api_json(
                Method::GET,
                "torrents/files",
                Some(&[("hash", snapshot.hash.as_str())]),
            )
            .await?;
        let files = response
            .as_array()
            .ok_or_else(|| anyhow!("qBittorrent returned an invalid file list"))?;
        ensure!(
            files.len() <= self.max_files,
            "qBittorrent returned too many files"
        );
        let mut parsed = Vec::with_capacity(files.len());
        for file in files {
            parsed.push(parse_torrent_file(file)?);
        }
        parsed.sort_by_key(|file| file.index);
        for (expected, file) in parsed.iter().enumerate() {
            ensure!(
                usize::try_from(file.index).ok() == Some(expected),
                "qBittorrent file indexes are not contiguous"
            );
        }
        snapshot.files = parsed;
        Ok(snapshot)
    }

    async fn version_major(&self) -> Result<u64> {
        if let Some(major) = *self.major_version.lock().await {
            return Ok(major);
        }
        let response = self
            .request_with_auth(
                |sid| self.request(Method::GET, "app/version").maybe_cookie(sid),
                true,
            )
            .await?;
        ensure!(
            response.status().is_success(),
            "qBittorrent version request failed"
        );
        let body = read_limited(response, MAX_VERSION_RESPONSE_BYTES).await?;
        let version = String::from_utf8(body).context("qBittorrent version response is invalid")?;
        let major = version
            .trim()
            .trim_start_matches(['v', 'V'])
            .split('.')
            .next()
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| anyhow!("qBittorrent version response is unsupported"))?;
        ensure!(
            (4..=9).contains(&major),
            "qBittorrent version is unsupported"
        );
        *self.major_version.lock().await = Some(major);
        Ok(major)
    }

    async fn api_json(
        &self,
        method: Method,
        endpoint: &str,
        query: Option<&[(&str, &str)]>,
    ) -> Result<Value> {
        let response = self
            .request_with_auth(
                |sid| {
                    let mut request = self.request(method.clone(), endpoint).maybe_cookie(sid);
                    if let Some(query) = query {
                        request = request.query(query);
                    }
                    request
                },
                true,
            )
            .await?;
        ensure!(
            response.status().is_success(),
            "qBittorrent API request failed with HTTP {}",
            response.status().as_u16()
        );
        let body = read_limited(response, self.max_api_response_bytes).await?;
        serde_json::from_slice(&body).context("qBittorrent returned invalid JSON")
    }

    fn request(&self, method: Method, endpoint: &str) -> reqwest::RequestBuilder {
        let url = self.api_url(endpoint);
        self.client
            .request(method, url)
            .header(REFERER, &self.referer)
            .header(ORIGIN, self.base_url.origin().ascii_serialization())
    }

    fn api_url(&self, endpoint: &str) -> Url {
        let mut url = self.base_url.clone();
        url.set_path(&format!("/api/v2/{endpoint}"));
        url
    }

    async fn request_with_auth<F>(
        &self,
        mut make_request: F,
        retry_after_login: bool,
    ) -> Result<Response>
    where
        F: FnMut(Option<&str>) -> reqwest::RequestBuilder,
    {
        let sid = self.sid.lock().await.clone();
        let mut response = make_request(sid.as_deref())
            .send()
            .await
            .map_err(|_| anyhow!("qBittorrent request failed"))?;
        if retry_after_login
            && matches!(
                response.status(),
                StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
            )
            && self.username.is_some()
            && self.password.is_some()
        {
            let mut current = self.sid.lock().await;
            if current.as_deref() == sid.as_deref() {
                *current = None;
            }
            drop(current);
            self.login().await?;
            let new_sid = self.sid.lock().await.clone();
            response = make_request(new_sid.as_deref())
                .send()
                .await
                .map_err(|_| anyhow!("qBittorrent request failed after login"))?;
        }
        Ok(response)
    }

    async fn login(&self) -> Result<()> {
        let username = self
            .username
            .as_deref()
            .ok_or_else(|| anyhow!("qBittorrent credentials are not configured"))?;
        let password = self
            .password
            .as_deref()
            .ok_or_else(|| anyhow!("qBittorrent credentials are not configured"))?;
        let response = self
            .request(Method::POST, "auth/login")
            .form(&[("username", username), ("password", password)])
            .send()
            .await
            .map_err(|_| anyhow!("qBittorrent login request failed"))?;
        ensure!(
            response.status().is_success(),
            "qBittorrent login failed with HTTP {}",
            response.status().as_u16()
        );
        let cookies = response.headers().get_all(SET_COOKIE);
        let mut sid = None;
        for cookie in cookies {
            let Some(value) = cookie
                .to_str()
                .ok()
                .and_then(|cookie| cookie.split(';').next())
                .and_then(|pair| pair.strip_prefix("SID="))
            else {
                continue;
            };
            ensure!(
                !value.is_empty() && value.bytes().all(is_safe_cookie_byte),
                "qBittorrent login returned an invalid session cookie"
            );
            sid = Some(format!("SID={value}"));
            break;
        }
        let body = read_limited(response, MAX_AUTH_RESPONSE_BYTES).await?;
        let accepted = String::from_utf8_lossy(&body)
            .trim()
            .eq_ignore_ascii_case("ok.");
        ensure!(
            accepted && sid.is_some(),
            "qBittorrent rejected authentication"
        );
        *self.sid.lock().await = sid;
        Ok(())
    }
}

fn is_safe_cookie_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'=' | b'-' | b'_' | b'.' | b'+' | b'/')
}

pub(crate) fn validate_settings(settings: &TorrentSettings) -> Result<()> {
    let url = Url::parse(&settings.base_url).context("qBittorrent URL is invalid")?;
    ensure!(
        matches!(url.scheme(), "http" | "https"),
        "qBittorrent URL must use HTTP or HTTPS"
    );
    ensure!(
        url.username().is_empty() && url.password().is_none(),
        "qBittorrent URL must not contain credentials"
    );
    ensure!(
        url.query().is_none() && url.fragment().is_none(),
        "qBittorrent URL must not contain a query or fragment"
    );
    ensure!(
        matches!(url.path(), "" | "/"),
        "qBittorrent URL must point to the local WebUI root"
    );
    ensure!(
        url.host().is_some_and(is_loopback_host),
        "qBittorrent URL must resolve to a loopback host"
    );
    match (&settings.username, &settings.password) {
        (Some(username), Some(password)) => {
            ensure!(
                !username.is_empty() && username.len() <= 256,
                "qBittorrent username is invalid"
            );
            ensure!(password.len() <= 4_096, "qBittorrent password is too long");
            ensure!(
                !username.chars().any(char::is_control),
                "qBittorrent username is invalid"
            );
            ensure!(
                !password.chars().any(char::is_control),
                "qBittorrent password is invalid"
            );
        }
        (None, None) => {}
        _ => bail!("qBittorrent username and password must be configured together"),
    }
    ensure!(
        (1..=DEFAULT_MAX_TORRENT_BYTES).contains(&settings.max_torrent_bytes),
        "qBittorrent metainfo byte limit is invalid"
    );
    ensure!(
        (1..=DEFAULT_MAX_FILES).contains(&settings.max_files),
        "qBittorrent file limit is invalid"
    );
    ensure!(
        (1..=300).contains(&settings.poll_seconds),
        "qBittorrent polling interval is invalid"
    );
    ensure!(
        (1..=60).contains(&settings.request_timeout_seconds),
        "qBittorrent request timeout is invalid"
    );
    ensure!(
        (1_024..=16 * 1024 * 1024).contains(&settings.max_api_response_bytes),
        "qBittorrent API response limit is invalid"
    );
    if settings.enabled {
        ensure!(
            !settings.state_dir.as_os_str().is_empty(),
            "qBittorrent state directory is required when enabled"
        );
    }
    Ok(())
}

fn is_loopback_host(host: url::Host<&str>) -> bool {
    match host {
        url::Host::Domain(domain) => domain.eq_ignore_ascii_case("localhost"),
        url::Host::Ipv4(address) => address.is_loopback(),
        url::Host::Ipv6(address) => address.is_loopback(),
    }
}

fn safe_upload_file_name(file_name: &str) -> String {
    if file_name.len() <= 80
        && file_name.ends_with(".torrent")
        && file_name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    {
        file_name.to_string()
    } else {
        "upload.torrent".to_string()
    }
}

fn validate_hash_parameter(hash: &str) -> Result<()> {
    ensure!(
        matches!(hash.len(), 40 | 64),
        "qBittorrent torrent hash is invalid"
    );
    ensure!(
        hash.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "qBittorrent torrent hash is invalid"
    );
    Ok(())
}

fn parse_torrent_info(value: &Value) -> Result<TorrentRemoteSnapshot> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("qBittorrent torrent record is invalid"))?;
    let hash = required_string(object, &["hash"], MAX_API_STRING_BYTES)?;
    validate_hash_parameter(&hash)?;
    let name = required_string(object, &["name"], MAX_API_STRING_BYTES)?;
    let state = required_string(object, &["state"], 128)?;
    let progress = required_number(object, &["progress"])?;
    ensure!(
        (0.0..=1.0).contains(&progress),
        "qBittorrent torrent progress is invalid"
    );
    let save_path = required_path(object, &["save_path", "savePath"])?;
    let content_path = optional_path(object, &["content_path", "contentPath"])?;
    let total_size = optional_u64(object, &["total_size", "totalSize", "size"])?
        .ok_or_else(|| anyhow!("qBittorrent total size is missing"))?;
    let amount_left = optional_u64(object, &["amount_left", "amountLeft"])?.unwrap_or_else(|| {
        total_size.saturating_sub(
            (progress * total_size as f64)
                .round()
                .clamp(0.0, u64::MAX as f64) as u64,
        )
    });
    let mut info_hash_v1 = optional_hash(object, &["infohash_v1", "info_hash_v1", "infoHashV1"])?;
    let mut info_hash_v2 = optional_hash(object, &["infohash_v2", "info_hash_v2", "infoHashV2"])?;
    if info_hash_v1.is_none() && hash.len() == 40 {
        info_hash_v1 = Some(hash.to_ascii_lowercase());
    } else if info_hash_v2.is_none() && hash.len() == 64 {
        info_hash_v2 = Some(hash.to_ascii_lowercase());
    }
    if let Some(magnet) =
        optional_string(object, &["magnet_uri", "magnetUri"], MAX_API_STRING_BYTES)?
        && let Some((v1, v2)) = magnet_hashes(&magnet)
    {
        info_hash_v1 = info_hash_v1.or(v1);
        info_hash_v2 = info_hash_v2.or(v2);
    }
    Ok(TorrentRemoteSnapshot {
        hash,
        info_hash_v1,
        info_hash_v2,
        name,
        state,
        progress,
        save_path,
        content_path,
        total_size,
        amount_left,
        files: Vec::new(),
    })
}

fn parse_torrent_file(value: &Value) -> Result<RemoteTorrentFile> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("qBittorrent file record is invalid"))?;
    let index = optional_u64(object, &["index"])?
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| anyhow!("qBittorrent file index is invalid"))?;
    let name = required_string(object, &["name"], MAX_API_FILE_NAME_BYTES)?;
    let relative_path = safe_relative_path(&name)?;
    let size = optional_u64(object, &["size"])?
        .ok_or_else(|| anyhow!("qBittorrent file size is missing"))?;
    let progress = required_number(object, &["progress"])?;
    ensure!(
        (0.0..=1.0).contains(&progress),
        "qBittorrent file progress is invalid"
    );
    let priority = optional_i64(object, &["priority"])?
        .ok_or_else(|| anyhow!("qBittorrent file priority is missing"))?;
    Ok(RemoteTorrentFile {
        index,
        relative_path,
        size,
        progress,
        priority,
    })
}

fn required_string(
    object: &serde_json::Map<String, Value>,
    keys: &[&str],
    max_bytes: usize,
) -> Result<String> {
    optional_string(object, keys, max_bytes)?
        .ok_or_else(|| anyhow!("qBittorrent response field is missing"))
}

fn optional_string(
    object: &serde_json::Map<String, Value>,
    keys: &[&str],
    max_bytes: usize,
) -> Result<Option<String>> {
    for key in keys {
        if let Some(value) = object.get(*key) {
            let value = value
                .as_str()
                .ok_or_else(|| anyhow!("qBittorrent response field is invalid"))?;
            ensure!(
                value.len() <= max_bytes,
                "qBittorrent response field is too long"
            );
            ensure!(
                !value.chars().any(char::is_control),
                "qBittorrent response field is invalid"
            );
            return Ok(Some(value.to_string()));
        }
    }
    Ok(None)
}

fn required_number(object: &serde_json::Map<String, Value>, keys: &[&str]) -> Result<f64> {
    for key in keys {
        if let Some(value) = object.get(*key) {
            return value
                .as_f64()
                .filter(|value| value.is_finite())
                .ok_or_else(|| anyhow!("qBittorrent response number is invalid"));
        }
    }
    bail!("qBittorrent response number is missing")
}

fn optional_u64(object: &serde_json::Map<String, Value>, keys: &[&str]) -> Result<Option<u64>> {
    for key in keys {
        if let Some(value) = object.get(*key) {
            let number = value
                .as_u64()
                .or_else(|| value.as_i64().and_then(|value| u64::try_from(value).ok()))
                .ok_or_else(|| anyhow!("qBittorrent response size is invalid"))?;
            return Ok(Some(number));
        }
    }
    Ok(None)
}

fn optional_i64(object: &serde_json::Map<String, Value>, keys: &[&str]) -> Result<Option<i64>> {
    for key in keys {
        if let Some(value) = object.get(*key) {
            return value
                .as_i64()
                .map(Some)
                .ok_or_else(|| anyhow!("qBittorrent response integer is invalid"));
        }
    }
    Ok(None)
}

fn optional_hash(object: &serde_json::Map<String, Value>, keys: &[&str]) -> Result<Option<String>> {
    let Some(hash) = optional_string(object, keys, 64)? else {
        return Ok(None);
    };
    ensure!(
        matches!(hash.len(), 40 | 64),
        "qBittorrent info hash is invalid"
    );
    ensure!(
        hash.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "qBittorrent info hash is invalid"
    );
    Ok(Some(hash.to_ascii_lowercase()))
}

fn required_path(object: &serde_json::Map<String, Value>, keys: &[&str]) -> Result<PathBuf> {
    let value = required_string(object, keys, MAX_API_STRING_BYTES)?;
    validated_absolute_path(value)
}

fn optional_path(
    object: &serde_json::Map<String, Value>,
    keys: &[&str],
) -> Result<Option<PathBuf>> {
    optional_string(object, keys, MAX_API_STRING_BYTES)?
        .map(validated_absolute_path)
        .transpose()
}

fn validated_absolute_path(value: String) -> Result<PathBuf> {
    let path = PathBuf::from(value);
    ensure!(
        path.is_absolute(),
        "qBittorrent returned a non-absolute path"
    );
    ensure!(
        path.components().all(|part| matches!(
            part,
            Component::RootDir | Component::Prefix(_) | Component::Normal(_)
        )),
        "qBittorrent returned an unsafe path"
    );
    Ok(path)
}

fn safe_relative_path(value: &str) -> Result<PathBuf> {
    ensure!(!value.is_empty(), "qBittorrent returned an empty file path");
    ensure!(
        value.len() <= MAX_API_FILE_NAME_BYTES,
        "qBittorrent file path is too long"
    );
    ensure!(
        !value.starts_with('/') && !value.starts_with('\\'),
        "qBittorrent returned an absolute file path"
    );
    ensure!(
        !value
            .chars()
            .any(|ch| ch.is_control() || matches!(ch, '\\' | ':' | '\0')),
        "qBittorrent returned an unsafe file path"
    );
    let mut result = PathBuf::new();
    for component in value.split('/') {
        ensure!(
            !component.is_empty() && component != "." && component != "..",
            "qBittorrent returned a path traversal"
        );
        result.push(component);
    }
    ensure!(
        result.is_relative()
            && result
                .components()
                .all(|part| matches!(part, Component::Normal(_))),
        "qBittorrent returned an unsafe file path"
    );
    Ok(result)
}

fn magnet_hashes(magnet: &str) -> Option<(Option<String>, Option<String>)> {
    let parsed = Url::parse(magnet).ok()?;
    if parsed.scheme() != "magnet" {
        return None;
    }
    let mut v1 = None;
    let mut v2 = None;
    for (key, value) in parsed.query_pairs() {
        if key != "xt" {
            continue;
        }
        if let Some(hash) = value.strip_prefix("urn:btih:")
            && hash.len() == 40
            && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            v1 = Some(hash.to_ascii_lowercase());
        } else if let Some(hash) = value.strip_prefix("urn:btmh:1220")
            && hash.len() == 64
            && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            v2 = Some(hash.to_ascii_lowercase());
        }
    }
    (v1.is_some() || v2.is_some()).then_some((v1, v2))
}

async fn read_limited(response: Response, max_bytes: usize) -> Result<Vec<u8>> {
    if let Some(length) = response.content_length() {
        ensure!(
            length <= max_bytes as u64,
            "qBittorrent response exceeds the configured byte limit"
        );
    }
    let mut stream = response.bytes_stream();
    let mut output = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| anyhow!("qBittorrent response body could not be read"))?;
        ensure!(
            output.len().saturating_add(chunk.len()) <= max_bytes,
            "qBittorrent response exceeds the configured byte limit"
        );
        output.extend_from_slice(&chunk);
    }
    Ok(output)
}

trait HeaderRequestBuilderExt {
    fn maybe_cookie(self, sid: Option<&str>) -> Self;
}

impl HeaderRequestBuilderExt for reqwest::RequestBuilder {
    fn maybe_cookie(self, sid: Option<&str>) -> Self {
        match sid {
            Some(cookie) => self.header(COOKIE, cookie),
            None => self,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[derive(Debug)]
    struct MockRequest {
        line: String,
        headers: String,
        body: Vec<u8>,
    }

    async fn serve(
        handler: impl Fn(MockRequest) -> (u16, Vec<(String, String)>, Vec<u8>) + Send + Sync + 'static,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let handler = Arc::new(handler);
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let handler = handler.clone();
                tokio::spawn(async move {
                    let mut received = Vec::new();
                    let mut chunk = [0_u8; 8192];
                    let header_end = loop {
                        let count = stream.read(&mut chunk).await.unwrap_or(0);
                        if count == 0 {
                            return;
                        }
                        received.extend_from_slice(&chunk[..count]);
                        if let Some(index) =
                            received.windows(4).position(|bytes| bytes == b"\r\n\r\n")
                        {
                            break index + 4;
                        }
                        if received.len() > 32 * 1024 {
                            return;
                        }
                    };
                    let header_text = String::from_utf8_lossy(&received[..header_end]).to_string();
                    let first_line = header_text
                        .lines()
                        .next()
                        .unwrap_or_default()
                        .trim()
                        .to_string();
                    let content_length = header_text
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .unwrap_or(0);
                    while received.len() < header_end.saturating_add(content_length) {
                        let count = stream.read(&mut chunk).await.unwrap_or(0);
                        if count == 0 {
                            return;
                        }
                        received.extend_from_slice(&chunk[..count]);
                    }
                    let request = MockRequest {
                        line: first_line,
                        headers: header_text,
                        body: received[header_end..header_end + content_length].to_vec(),
                    };
                    let (status, headers, body) = handler(request);
                    let reason = match status {
                        200 => "OK",
                        202 => "Accepted",
                        302 => "Found",
                        403 => "Forbidden",
                        409 => "Conflict",
                        415 => "Unsupported Media Type",
                        _ => "Error",
                    };
                    let has_content_length = headers
                        .iter()
                        .any(|(name, _)| name.eq_ignore_ascii_case("content-length"));
                    let mut response =
                        format!("HTTP/1.1 {status} {reason}\r\nConnection: close\r\n");
                    if !has_content_length {
                        response.push_str(&format!("Content-Length: {}\r\n", body.len()));
                    }
                    for (name, value) in headers {
                        response.push_str(&format!("{name}: {value}\r\n"));
                    }
                    response.push_str("\r\n");
                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.write_all(&body).await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        (format!("http://{address}"), task)
    }

    fn settings(base_url: String) -> TorrentSettings {
        TorrentSettings {
            enabled: true,
            base_url,
            username: None,
            password: None,
            state_dir: PathBuf::from("/tmp/torrent-test-state"),
            max_torrent_bytes: DEFAULT_MAX_TORRENT_BYTES,
            max_files: DEFAULT_MAX_FILES,
            poll_seconds: 10,
            request_timeout_seconds: 3,
            max_api_response_bytes: 64 * 1024,
        }
    }

    #[tokio::test]
    async fn login_referer_cookie_and_v5_add_parameters_are_bounded_and_local() {
        // synthetic-token-fixtures: joey-private-v3 / access-a and api-key-a, active.
        const SYNTHETIC_SESSION: &str = "codex_synth_v1_access_a";
        const SYNTHETIC_PASSWORD: &str = "codex_synth_v1_api_key_a";
        let add_seen = Arc::new(AtomicUsize::new(0));
        let add_seen_copy = add_seen.clone();
        let (base_url, server) = serve(move |request| {
            if request.line.starts_with("GET /api/v2/app/version") {
                if request
                    .headers
                    .to_ascii_lowercase()
                    .contains(&format!("cookie: sid={SYNTHETIC_SESSION}"))
                {
                    (200, vec![], b"v5.0.0".to_vec())
                } else {
                    (403, vec![], b"Forbidden".to_vec())
                }
            } else if request.line.starts_with("POST /api/v2/auth/login") {
                (
                    200,
                    vec![(
                        "Set-Cookie".to_string(),
                        format!("SID={SYNTHETIC_SESSION}; path=/"),
                    )],
                    b"Ok.".to_vec(),
                )
            } else if request.line.starts_with("POST /api/v2/torrents/add") {
                add_seen_copy.fetch_add(1, Ordering::Relaxed);
                assert!(request.headers.to_ascii_lowercase().contains("referer: "));
                assert!(
                    request
                        .body
                        .windows(b"stopped".len())
                        .any(|window| window == b"stopped")
                );
                assert!(
                    request
                        .body
                        .windows(b"false".len())
                        .any(|window| window == b"false")
                );
                assert!(
                    !request
                        .body
                        .windows(b"savepath".len())
                        .any(|window| window == b"savepath")
                );
                assert!(
                    !request
                        .body
                        .windows(b"category".len())
                        .any(|window| window == b"category")
                );
                (200, vec![], b"Ok.".to_vec())
            } else {
                (404, vec![], Vec::new())
            }
        })
        .await;
        let mut config = settings(base_url);
        config.username = Some("test-user".to_string());
        config.password = Some(SYNTHETIC_PASSWORD.to_string());
        let client = QbittorrentClient::new(&config).unwrap();
        let result = client
            .submit(b"d4:infoe".to_vec(), "abc.torrent".to_string())
            .await
            .unwrap();
        assert_eq!(result.disposition, SubmitDisposition::Accepted);
        assert_eq!(add_seen.load(Ordering::Relaxed), 1);
        server.abort();
    }

    #[tokio::test]
    async fn v4_add_uses_paused_false_and_invalid_metainfo_is_reported() {
        let (base_url, server) = serve(|request| {
            if request.line.starts_with("GET /api/v2/app/version") {
                (200, vec![], b"v4.3.9".to_vec())
            } else if request.line.starts_with("POST /api/v2/torrents/add") {
                assert!(
                    request
                        .body
                        .windows(b"paused".len())
                        .any(|window| window == b"paused")
                );
                assert!(
                    request
                        .body
                        .windows(b"false".len())
                        .any(|window| window == b"false")
                );
                (415, vec![], b"not logged or echoed".to_vec())
            } else {
                (404, vec![], Vec::new())
            }
        })
        .await;
        let client = QbittorrentClient::new(&settings(base_url)).unwrap();
        let result = client
            .submit(b"bad".to_vec(), "unsafe name.torrent".to_string())
            .await
            .unwrap();
        assert_eq!(result.http_status, 415);
        assert_eq!(result.disposition, SubmitDisposition::InvalidMetainfo);
        server.abort();
    }

    #[tokio::test]
    async fn refuses_redirect_and_oversized_api_response() {
        let (base_url, server) = serve(|request| {
            if request.line.starts_with("GET /api/v2/torrents/info") {
                (
                    302,
                    vec![(
                        "Location".to_string(),
                        "http://example.com/steal".to_string(),
                    )],
                    Vec::new(),
                )
            } else {
                (404, vec![], Vec::new())
            }
        })
        .await;
        let client = QbittorrentClient::new(&settings(base_url)).unwrap();
        assert!(client.refresh(&"a".repeat(40)).await.is_err());
        server.abort();

        let (base_url, server) = serve(|request| {
            if request.line.starts_with("GET /api/v2/torrents/info") {
                (
                    200,
                    vec![("Content-Length".to_string(), "70000".to_string())],
                    Vec::new(),
                )
            } else {
                (404, vec![], Vec::new())
            }
        })
        .await;
        let client = QbittorrentClient::new(&settings(base_url)).unwrap();
        assert!(client.refresh(&"a".repeat(40)).await.is_err());
        server.abort();
    }

    #[tokio::test]
    async fn v2_prefix_requires_unique_full_identity_evidence() {
        let full_v2 =
            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string();
        let short = full_v2[..40].to_string();
        let full_v2_for_server = full_v2.clone();
        let (base_url, server) = serve(move |request| {
            if request.line.starts_with("GET /api/v2/torrents/info") {
                let record = serde_json::json!({
                    "hash": short,
                    "name": "torrent",
                    "state": "downloading",
                    "progress": 0.5,
                    "save_path": "/tmp/downloads",
                    "content_path": "/tmp/downloads/torrent",
                    "total_size": 2,
                    "amount_left": 1,
                    "magnet_uri": format!("magnet:?xt=urn:btmh:1220{full_v2_for_server}")
                });
                (200, vec![], serde_json::to_vec(&vec![record]).unwrap())
            } else if request.line.starts_with("GET /api/v2/torrents/files") {
                let file = serde_json::json!({"index": 0, "name": "folder/track.mp3", "size": 2, "progress": 0.5, "priority": 1});
                (200, vec![], serde_json::to_vec(&vec![file]).unwrap())
            } else {
                (404, vec![], Vec::new())
            }
        })
        .await;
        let client = QbittorrentClient::new(&settings(base_url)).unwrap();
        let matches = client
            .lookup(&TorrentIdentity {
                v1: None,
                v2: Some(full_v2),
            })
            .await
            .unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(
            matches[0].files[0].relative_path,
            PathBuf::from("folder/track.mp3")
        );
        server.abort();
    }

    #[test]
    fn settings_reject_nonlocal_urls_and_redact_credentials() {
        // synthetic-token-fixtures: joey-private-v3 / api-key-a, active.
        const SYNTHETIC_PASSWORD: &str = "codex_synth_v1_api_key_a";
        let mut config = settings("https://example.com".to_string());
        config.username = Some("secret-user".to_string());
        config.password = Some(SYNTHETIC_PASSWORD.to_string());
        assert!(validate_settings(&config).is_err());
        let debug = format!("{config:?}");
        assert!(!debug.contains("secret-user"));
        assert!(!debug.contains(SYNTHETIC_PASSWORD));
        config.base_url = "http://127.0.0.1:8080".to_string();
        config.password = None;
        assert!(validate_settings(&config).is_err());
    }
}
