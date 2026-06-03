use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use image::{DynamicImage, ImageFormat, Luma};
use qrcode::QrCode;
use reqwest::Client;
use reqwest::header::{COOKIE, HeaderMap, SET_COOKIE, USER_AGENT};
use serde::{Deserialize, Serialize};
use url::Url;

const USER_AGENT_VALUE: &str = "Mozilla/5.0";
const QRCODE_GENERATE_URL: &str =
    "https://passport.bilibili.com/x/passport-login/web/qrcode/generate";
const QRCODE_POLL_URL: &str = "https://passport.bilibili.com/x/passport-login/web/qrcode/poll";
const NAV_URL: &str = "https://api.bilibili.com/x/web-interface/nav";
static AUTH_FILE_LOCK: Mutex<()> = Mutex::new(());
static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);
const LOGIN_URL_COOKIE_NAMES: &[&str] = &[
    "SESSDATA",
    "bili_jct",
    "DedeUserID",
    "DedeUserID__ckMd5",
    "sid",
    "buvid3",
    "buvid4",
    "b_nut",
];

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthState {
    pub cookie: String,
    pub mid: u64,
    pub uname: String,
    pub stored_at_unix: u64,
}

impl fmt::Debug for AuthState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthState")
            .field("cookie", &"<redacted>")
            .field("mid", &self.mid)
            .field("uname", &self.uname)
            .field("stored_at_unix", &self.stored_at_unix)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoginQr {
    pub url: String,
    pub qrcode_key: String,
    pub png: Vec<u8>,
}

#[derive(Clone, PartialEq, Eq)]
pub enum LoginPoll {
    Waiting,
    Scanned,
    Expired,
    Success { cookie: String },
}

impl fmt::Debug for LoginPoll {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Waiting => formatter.write_str("Waiting"),
            Self::Scanned => formatter.write_str("Scanned"),
            Self::Expired => formatter.write_str("Expired"),
            Self::Success { .. } => formatter
                .debug_struct("Success")
                .field("cookie", &"<redacted>")
                .finish(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct BilibiliApiResponse<T> {
    code: i64,
    message: String,
    data: T,
}

#[derive(Debug, Deserialize)]
struct QrGenerateData {
    url: String,
    qrcode_key: String,
}

#[derive(Debug, Deserialize)]
struct QrPollData {
    #[serde(default)]
    code: i64,
    #[serde(default)]
    message: String,
    #[serde(default)]
    url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct NavData {
    #[serde(rename = "isLogin")]
    is_login: bool,
    mid: Option<u64>,
    uname: Option<String>,
}

pub async fn generate_login_qr(client: &Client) -> Result<LoginQr> {
    let response = client
        .get(QRCODE_GENERATE_URL)
        .header(USER_AGENT, USER_AGENT_VALUE)
        .send()
        .await
        .context("failed to request Bilibili login QR")?
        .error_for_status()
        .context("Bilibili login QR request returned HTTP error")?
        .json::<BilibiliApiResponse<QrGenerateData>>()
        .await
        .context("failed to decode Bilibili login QR response")?;

    if response.code != 0 {
        bail!(
            "Bilibili login QR request failed: {} ({})",
            response.message,
            response.code
        );
    }

    let png = render_qr_png(&response.data.url)?;
    Ok(LoginQr {
        url: response.data.url,
        qrcode_key: response.data.qrcode_key,
        png,
    })
}

pub async fn poll_login(client: &Client, qrcode_key: &str) -> Result<LoginPoll> {
    let response = client
        .get(QRCODE_POLL_URL)
        .query(&[("qrcode_key", qrcode_key)])
        .header(USER_AGENT, USER_AGENT_VALUE)
        .send()
        .await
        .context("failed to poll Bilibili login QR")?
        .error_for_status()
        .context("Bilibili login poll returned HTTP error")?;

    let cookie = extract_cookie_header(response.headers());
    let body = response
        .json::<BilibiliApiResponse<QrPollData>>()
        .await
        .context("failed to decode Bilibili login poll response")?;

    login_poll_from_response(body, cookie)
}

pub async fn verify_cookie(client: &Client, cookie: &str) -> Result<AuthState> {
    let response = client
        .get(NAV_URL)
        .header(USER_AGENT, USER_AGENT_VALUE)
        .header(COOKIE, cookie)
        .send()
        .await
        .context("failed to verify Bilibili login")?
        .error_for_status()
        .context("Bilibili login verification returned HTTP error")?
        .json::<BilibiliApiResponse<NavData>>()
        .await
        .context("failed to decode Bilibili login verification response")?;

    auth_state_from_nav_response(response, cookie)
}

fn login_poll_from_response(
    response: BilibiliApiResponse<QrPollData>,
    cookie: Option<String>,
) -> Result<LoginPoll> {
    if response.code != 0 {
        bail!(
            "Bilibili login poll failed: {} ({})",
            response.message,
            response.code
        );
    }

    match response.data.code {
        0 => {
            let cookie =
                cookie.or_else(|| response.data.url.as_deref().and_then(cookie_from_login_url));
            let Some(cookie) = cookie else {
                bail!("Bilibili login succeeded without returning cookies");
            };
            Ok(LoginPoll::Success { cookie })
        }
        86_101 => Ok(LoginPoll::Waiting),
        86_090 => Ok(LoginPoll::Scanned),
        86_038 => Ok(LoginPoll::Expired),
        code => bail!(
            "Bilibili login poll returned unexpected status: {} ({})",
            response.data.message,
            code
        ),
    }
}

fn auth_state_from_nav_response(
    response: BilibiliApiResponse<NavData>,
    cookie: &str,
) -> Result<AuthState> {
    if response.code != 0 || !response.data.is_login {
        bail!(
            "Bilibili account is not logged in: {} ({})",
            response.message,
            response.code
        );
    }

    Ok(AuthState {
        cookie: cookie.to_string(),
        mid: response
            .data
            .mid
            .ok_or_else(|| anyhow!("Bilibili verification response did not include mid"))?,
        uname: response
            .data
            .uname
            .filter(|name| !name.trim().is_empty())
            .ok_or_else(|| anyhow!("Bilibili verification response did not include uname"))?,
        stored_at_unix: now_unix_seconds(),
    })
}

pub fn render_qr_png(text: &str) -> Result<Vec<u8>> {
    let code = QrCode::new(text.as_bytes()).context("failed to encode QR data")?;
    let image = code
        .render::<Luma<u8>>()
        .quiet_zone(true)
        .module_dimensions(8, 8)
        .build();

    let mut output = Cursor::new(Vec::new());
    DynamicImage::ImageLuma8(image)
        .write_to(&mut output, ImageFormat::Png)
        .context("failed to encode QR PNG")?;
    Ok(output.into_inner())
}

pub fn extract_cookie_header(headers: &HeaderMap) -> Option<String> {
    set_cookie_values_to_cookie(
        headers
            .get_all(SET_COOKIE)
            .iter()
            .filter_map(|value| value.to_str().ok()),
    )
}

pub fn set_cookie_values_to_cookie<'a>(
    values: impl IntoIterator<Item = &'a str>,
) -> Option<String> {
    let pairs = values
        .into_iter()
        .filter_map(|value| value.split(';').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();

    if pairs.is_empty() {
        None
    } else {
        Some(pairs.join("; "))
    }
}

pub fn cookie_from_login_url(value: &str) -> Option<String> {
    let parsed = Url::parse(value).ok()?;
    let mut pairs = Vec::new();
    append_login_cookie_pairs(parsed.query(), &mut pairs);

    if let Some(fragment) = parsed.fragment() {
        let fragment_query = fragment.split_once('?').map(|(_, query)| query);
        append_login_cookie_pairs(fragment_query, &mut pairs);
    }

    if pairs.is_empty() {
        None
    } else {
        Some(pairs.join("; "))
    }
}

fn append_login_cookie_pairs(query: Option<&str>, pairs: &mut Vec<String>) {
    let Some(query) = query else {
        return;
    };

    for (name, value) in url::form_urlencoded::parse(query.as_bytes()) {
        if LOGIN_URL_COOKIE_NAMES.contains(&name.as_ref()) && !value.trim().is_empty() {
            pairs.push(format!("{name}={}", encode_cookie_value_for_bbdown(&value)));
        }
    }
}

fn encode_cookie_value_for_bbdown(value: &str) -> String {
    value.replace(',', "%2C")
}

pub fn load_auth_state(path: &Path) -> Result<Option<AuthState>> {
    let _guard = AUTH_FILE_LOCK
        .lock()
        .expect("auth file lock should not poison");
    load_auth_state_unlocked(path)
}

fn load_auth_state_unlocked(path: &Path) -> Result<Option<AuthState>> {
    match fs::read(path) {
        Ok(content) => serde_json::from_slice(&content)
            .with_context(|| format!("failed to parse Bilibili auth state {}", path.display()))
            .map(Some),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err)
            .with_context(|| format!("failed to read Bilibili auth state {}", path.display())),
    }
}

pub fn save_auth_state(path: &Path, state: &AuthState) -> Result<()> {
    let _guard = AUTH_FILE_LOCK
        .lock()
        .expect("auth file lock should not poison");
    save_auth_state_unlocked(path, state)
}

fn save_auth_state_unlocked(path: &Path, state: &AuthState) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        create_private_dir_if_missing(parent).with_context(|| {
            format!("failed to create auth state directory {}", parent.display())
        })?;
    }

    let content =
        serde_json::to_vec_pretty(state).context("failed to encode Bilibili auth state")?;
    let temp_path = temp_state_path(path);
    let _ = fs::remove_file(&temp_path);
    {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&temp_path)
            .with_context(|| format!("failed to create temp auth state {}", temp_path.display()))?;
        std::io::Write::write_all(&mut file, &content)
            .with_context(|| format!("failed to write temp auth state {}", temp_path.display()))?;
        std::io::Write::flush(&mut file)
            .with_context(|| format!("failed to flush temp auth state {}", temp_path.display()))?;
    }
    set_file_private(&temp_path);
    fs::rename(&temp_path, path)
        .with_context(|| format!("failed to replace auth state {}", path.display()))?;
    set_file_private(path);
    Ok(())
}

pub fn delete_auth_state(path: &Path) -> Result<bool> {
    let _guard = AUTH_FILE_LOCK
        .lock()
        .expect("auth file lock should not poison");
    let config_path = bbdown_config_path(path);
    let legacy_config_path = legacy_bbdown_config_path(path);
    let mut removed = false;
    match fs::remove_file(path) {
        Ok(()) => removed = true,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err).with_context(|| {
                format!("failed to delete Bilibili auth state {}", path.display())
            });
        }
    }

    for path in [config_path, legacy_config_path] {
        match fs::remove_file(&path) {
            Ok(()) => removed = true,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to delete BBDown auth config {}", path.display())
                });
            }
        }
    }

    Ok(removed)
}

pub fn ensure_bbdown_config_file(path: &Path) -> Result<Option<PathBuf>> {
    let _guard = AUTH_FILE_LOCK
        .lock()
        .expect("auth file lock should not poison");
    let Some(state) = load_auth_state_unlocked(path)? else {
        return Ok(None);
    };
    if state.cookie.trim().is_empty() {
        return Ok(None);
    }

    let config_path = temp_state_path(&bbdown_config_path(path));
    write_bbdown_config(&config_path, &state.cookie)?;
    Ok(Some(config_path))
}

pub fn bbdown_config_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".bbdown.config");
    PathBuf::from(value)
}

fn legacy_bbdown_config_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".bbdown.config.json");
    PathBuf::from(value)
}

fn write_bbdown_config(path: &Path, cookie: &str) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        create_private_dir_if_missing(parent).with_context(|| {
            format!(
                "failed to create BBDown auth config directory {}",
                parent.display()
            )
        })?;
    }

    let content = format!("--cookie {cookie}\n").into_bytes();
    let temp_path = temp_state_path(path);
    {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temp_path).with_context(|| {
            format!(
                "failed to create temp BBDown auth config {}",
                temp_path.display()
            )
        })?;
        std::io::Write::write_all(&mut file, &content).with_context(|| {
            format!(
                "failed to write temp BBDown auth config {}",
                temp_path.display()
            )
        })?;
        std::io::Write::flush(&mut file).with_context(|| {
            format!(
                "failed to flush temp BBDown auth config {}",
                temp_path.display()
            )
        })?;
    }
    set_file_private(&temp_path);
    fs::rename(&temp_path, path)
        .with_context(|| format!("failed to replace BBDown auth config {}", path.display()))?;
    set_file_private(path);
    Ok(())
}

fn temp_state_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    value.push(format!(".{}.{}.{}.tmp", std::process::id(), counter, nanos));
    PathBuf::from(value)
}

fn now_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn create_private_dir_if_missing(path: &Path) -> Result<()> {
    let existed = path.exists();
    fs::create_dir_all(path)?;
    if !existed {
        set_dir_private(path);
    }
    Ok(())
}

#[cfg(unix)]
fn set_dir_private(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o700));
}

#[cfg(not(unix))]
fn set_dir_private(_path: &Path) {}

#[cfg(unix)]
fn set_file_private(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
}

#[cfg(not(unix))]
fn set_file_private(_path: &Path) {}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    fn temp_state_file(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir()
            .join(format!("telegram-video-downloader-tests-{name}-{unique}"))
            .join("state.json")
    }

    fn test_state() -> AuthState {
        AuthState {
            cookie: "SESSDATA=secret; bili_jct=csrf".to_string(),
            mid: 123,
            uname: "Joey".to_string(),
            stored_at_unix: 1_717_171_717,
        }
    }

    #[test]
    fn extracts_cookie_pairs_from_set_cookie_headers() {
        assert_eq!(
            set_cookie_values_to_cookie([
                "SESSDATA=abc; Path=/; HttpOnly",
                "bili_jct=def; Path=/",
                "",
            ]),
            Some("SESSDATA=abc; bili_jct=def".to_string())
        );
    }

    #[test]
    fn returns_none_for_empty_cookie_headers() {
        assert_eq!(set_cookie_values_to_cookie(["", "   "]), None);
    }

    #[test]
    fn extracts_cookie_from_login_url_query() {
        assert_eq!(
            cookie_from_login_url(
                "https://passport.bilibili.com/account/security?SESSDATA=secret%2Fvalue&bili_jct=csrf&DedeUserID=123&Expires=999&gourl=https%3A%2F%2Fexample.com",
            ),
            Some("SESSDATA=secret/value; bili_jct=csrf; DedeUserID=123".to_string())
        );
    }

    #[test]
    fn preserves_login_url_cookie_commas_for_bbdown() {
        assert_eq!(
            cookie_from_login_url(
                "https://passport.bilibili.com/account/security?SESSDATA=secret%2Cvalue&bili_jct=csrf",
            ),
            Some("SESSDATA=secret%2Cvalue; bili_jct=csrf".to_string())
        );
    }

    #[test]
    fn extracts_cookie_from_login_url_fragment_query() {
        assert_eq!(
            cookie_from_login_url(
                "https://passport.bilibili.com/account/security#/home?SESSDATA=secret&bili_jct=csrf&DedeUserID__ckMd5=hash",
            ),
            Some("SESSDATA=secret; bili_jct=csrf; DedeUserID__ckMd5=hash".to_string())
        );
    }

    #[test]
    fn renders_qr_png() {
        let png = render_qr_png("https://example.com/login").expect("QR should render");
        assert!(png.starts_with(b"\x89PNG\r\n\x1a\n"));
        assert!(png.len() > 100);
    }

    #[test]
    fn parses_login_poll_states() {
        assert_eq!(test_poll(86_101, None), LoginPoll::Waiting);
        assert_eq!(test_poll(86_090, None), LoginPoll::Scanned);
        assert_eq!(test_poll(86_038, None), LoginPoll::Expired);
        assert_eq!(
            test_poll(0, Some("SESSDATA=secret".to_string())),
            LoginPoll::Success {
                cookie: "SESSDATA=secret".to_string()
            }
        );
    }

    #[test]
    fn parses_successful_login_poll_cookie_from_url() {
        let poll = login_poll_from_response(
            test_poll_response_with_url(
                0,
                "https://passport.bilibili.com/account/security#/home?SESSDATA=secret&bili_jct=csrf",
            ),
            None,
        )
        .expect("success URL cookies should parse");

        assert_eq!(
            poll,
            LoginPoll::Success {
                cookie: "SESSDATA=secret; bili_jct=csrf".to_string()
            }
        );
    }

    #[test]
    fn rejects_successful_login_poll_without_cookie() {
        assert!(
            login_poll_from_response(test_poll_response(0), None)
                .expect_err("success without cookies should fail")
                .to_string()
                .contains("without returning cookies")
        );
    }

    #[test]
    fn parses_nav_account() {
        let state = auth_state_from_nav_response(
            BilibiliApiResponse {
                code: 0,
                message: "OK".to_string(),
                data: NavData {
                    is_login: true,
                    mid: Some(123),
                    uname: Some("Joey".to_string()),
                },
            },
            "SESSDATA=secret",
        )
        .expect("nav should parse");

        assert_eq!(state.cookie, "SESSDATA=secret");
        assert_eq!(state.mid, 123);
        assert_eq!(state.uname, "Joey");
    }

    #[test]
    fn rejects_logged_out_nav_account() {
        assert!(
            auth_state_from_nav_response(
                BilibiliApiResponse {
                    code: -101,
                    message: "账号未登录".to_string(),
                    data: NavData {
                        is_login: false,
                        mid: None,
                        uname: None,
                    },
                },
                "SESSDATA=secret",
            )
            .expect_err("logged out nav should fail")
            .to_string()
            .contains("not logged in")
        );
    }

    #[test]
    fn saves_loads_and_deletes_auth_state() {
        let path = temp_state_file("state-roundtrip");
        let state = test_state();

        save_auth_state(&path, &state).expect("state should save");
        assert_eq!(
            load_auth_state(&path).expect("state should load"),
            Some(state)
        );
        assert!(delete_auth_state(&path).expect("state should delete"));
        assert_eq!(
            load_auth_state(&path).expect("state should be missing"),
            None
        );
        assert!(!delete_auth_state(&path).expect("missing delete should be ok"));

        if let Some(parent) = path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    fn test_poll(code: i64, cookie: Option<String>) -> LoginPoll {
        login_poll_from_response(test_poll_response(code), cookie).expect("poll should parse")
    }

    fn test_poll_response(code: i64) -> BilibiliApiResponse<QrPollData> {
        BilibiliApiResponse {
            code: 0,
            message: "OK".to_string(),
            data: QrPollData {
                code,
                message: "message".to_string(),
                url: None,
            },
        }
    }

    fn test_poll_response_with_url(code: i64, url: &str) -> BilibiliApiResponse<QrPollData> {
        BilibiliApiResponse {
            code: 0,
            message: "OK".to_string(),
            data: QrPollData {
                code,
                message: "message".to_string(),
                url: Some(url.to_string()),
            },
        }
    }

    #[test]
    fn creates_and_deletes_bbdown_config_file() {
        let path = temp_state_file("bbdown-config");
        save_auth_state(&path, &test_state()).expect("state should save");

        let config_path = ensure_bbdown_config_file(&path)
            .expect("BBDown config should save")
            .expect("BBDown config should be present");
        assert!(
            config_path
                .display()
                .to_string()
                .contains(".bbdown.config.")
        );
        let legacy_config_path = legacy_bbdown_config_path(&path);
        fs::write(&legacy_config_path, "--cookie legacy\n").expect("legacy config should write");
        let content = fs::read_to_string(&config_path).expect("BBDown config should be readable");
        assert_eq!(content, "--cookie SESSDATA=secret; bili_jct=csrf\n");
        assert!(delete_auth_state(&path).expect("auth delete should succeed"));
        assert!(!path.exists());
        assert!(config_path.exists());
        fs::remove_file(&config_path).expect("per-command config should delete");
        assert!(!legacy_config_path.exists());

        if let Some(parent) = path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[test]
    fn temp_state_paths_are_unique_per_call() {
        let path = temp_state_file("temp-state-unique");
        assert_ne!(temp_state_path(&path), temp_state_path(&path));
    }

    #[cfg(unix)]
    #[test]
    fn saves_auth_state_with_private_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let path = temp_state_file("state-permissions");
        save_auth_state(&path, &test_state()).expect("state should save");

        let mode = fs::metadata(&path)
            .expect("state metadata should exist")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);

        if let Some(parent) = path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[cfg(unix)]
    #[test]
    fn does_not_chmod_existing_auth_parent_directory() {
        use std::os::unix::fs::PermissionsExt;

        let path = temp_state_file("state-existing-parent-permissions");
        let parent = path.parent().expect("state should have parent");
        fs::create_dir_all(parent).expect("parent should be created");
        fs::set_permissions(parent, fs::Permissions::from_mode(0o755))
            .expect("parent permissions should be set");

        save_auth_state(&path, &test_state()).expect("state should save");

        let parent_mode = fs::metadata(parent)
            .expect("parent metadata should exist")
            .permissions()
            .mode()
            & 0o777;
        let file_mode = fs::metadata(&path)
            .expect("state metadata should exist")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(parent_mode, 0o755);
        assert_eq!(file_mode, 0o600);

        let _ = fs::remove_dir_all(parent);
    }
}
