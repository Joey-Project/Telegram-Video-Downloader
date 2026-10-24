use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::path::{Component, Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::config::CloudConfig;
use crate::file_manager::{PreviewCleanupGate, PreviewCleanupOutcome};
use crate::file_provider::{
    QueueFileProvider, classify_deadlock_error, is_deadlock_error, platform_queue_file_provider,
};
use crate::safe_fs::{
    BoundDirectoryCursor, BoundDirectoryCursorEntry, BoundDirectoryEntrySetStamp, BoundEntry,
    BoundFile, EntryIdentity, RootedFs,
};
use anyhow::{Context, Result, anyhow, bail};
use futures_util::{SinkExt, StreamExt, stream};
use reqwest::Client;
use reqwest::header::{AUTHORIZATION, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION as WS_AUTHORIZATION;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

const CLOUD_INBOX_DIRECTORY: &str = ".telegram-video-downloader-cloud";
const CLOUD_INBOX_OWNER_LOCK: &str = ".owner.lock";
const CLOUD_INBOX_EPOCHS_DIRECTORY: &str = "epochs";
const CLOUD_STATE_VERSION_FILE: &str = ".state-version.json";
const CLOUD_REQUEST_PREFIX: &str = "request-";
const CLOUD_REQUEST_SUFFIX: &str = ".json";
const INBOX_RECORD_VERSION: u32 = 1;
const MAX_JS_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
const MAX_PREVIEW_CANCEL_ID_BYTES: usize = 128;
const MAX_PREVIEW_CANCEL_REVISION_BYTES: usize = 256;
pub(crate) const MAX_CLOUD_STATE_VERSION: u64 = MAX_JS_SAFE_INTEGER;
const MAX_REQUEST_BYTES: usize = 1024 * 1024;
const MAX_LEGACY_IMPORT_BYTES: usize = 256 * 1024;
const MAX_MANAGEMENT_STATE_RECORDS: usize = 100;
const MAX_COMPLETED_LEGACY_IMPORT_RECORDS: usize = 100;
const CLOUD_STATE_CHUNK_BYTES: usize = 32 * 1024;
const MAX_CLOUD_STATE_CHUNKS: usize = 386;
const MAX_CLOUD_STATE_PARTITION_BYTES: usize = 12 * 1024 * 1024;
const CLOUD_STATE_CHUNK_ACK_CACHE_ENTRIES: usize = 768;
const CLOUD_STATE_CHUNK_ACK_CACHE_TTL: Duration = Duration::from_secs(60 * 60);
const CLOUD_STATE_CHUNK_UPLOAD_CONCURRENCY: usize = 4;
const MAX_CLOUD_STATE_JSON_DEPTH: usize = 48;
const MAX_CLOUD_STATE_JSON_NODES: usize = 1_000_000;
pub(crate) const CLOUD_REQUEST_PAGE_SIZE: usize = 100;
// Keep terminal local receipts through the same replay window as D1 ACK tombstones.
const CLOUD_INBOX_TERMINAL_REPLAY_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);
// Abandoned confirmations expire with the persisted preview manifests in file_manager.rs.
const CLOUD_INBOX_ABANDONED_PREVIEW_RETENTION: Duration = CLOUD_INBOX_TERMINAL_REPLAY_RETENTION;
// Each inbox API call makes bounded recovery/cleanup progress instead of draining history at once.
const CLOUD_INBOX_SCAN_MAX_NAMES: usize = 32;
const CLOUD_INBOX_SCAN_MAX_BYTES: usize = 8 * 1024 * 1024;
static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
struct CloudStateComplexityExceeded;

impl std::fmt::Display for CloudStateComplexityExceeded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "cloud state JSON exceeds local complexity limits"
        )
    }
}

impl std::error::Error for CloudStateComplexityExceeded {}

pub type CloudWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug)]
pub(crate) struct CloudStateVersionExhausted;

impl std::fmt::Display for CloudStateVersionExhausted {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "cloud state version is outside JavaScript's safe integer range"
        )
    }
}

impl std::error::Error for CloudStateVersionExhausted {}

#[derive(Debug)]
pub(crate) struct CloudInboxRecoveryIncomplete;

impl std::fmt::Display for CloudInboxRecoveryIncomplete {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "cloud inbox startup recovery is still in progress"
        )
    }
}

impl std::error::Error for CloudInboxRecoveryIncomplete {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CloudDatabaseEpochChanged {
    pub(crate) expected: String,
    pub(crate) actual: String,
}

impl std::fmt::Display for CloudDatabaseEpochChanged {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "cloud database epoch changed from {} to {}",
            self.expected, self.actual
        )
    }
}

impl std::error::Error for CloudDatabaseEpochChanged {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CloudInboxSequenceConflict {
    pub(crate) seq: u64,
}

impl std::fmt::Display for CloudInboxSequenceConflict {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "cloud request sequence {} was reused with a different envelope",
            self.seq
        )
    }
}

impl std::error::Error for CloudInboxSequenceConflict {}

fn valid_database_epoch(epoch: &str) -> bool {
    epoch.len() == 32
        && epoch
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
}

fn database_epoch_from_header(headers: &reqwest::header::HeaderMap) -> Option<String> {
    let epoch = headers
        .get("X-Cloud-Database-Epoch")?
        .to_str()
        .ok()?
        .to_string();
    valid_database_epoch(&epoch).then_some(epoch)
}

pub(crate) fn is_state_version_exhausted(error: &anyhow::Error) -> bool {
    error.downcast_ref::<CloudStateVersionExhausted>().is_some()
}

#[cfg(test)]
// Catalog fixture: pool joey-private-v3, token ID bearer-a, role bearer, state active.
pub(crate) const TEST_CLOUD_SECRET: &str = "codex_synth_v1_bearer_a";
#[cfg(test)]
pub(crate) const TEST_DATABASE_EPOCH: &str = "0123456789abcdef0123456789abcdef";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CloudRequest {
    pub seq: u64,
    pub kind: String,
    pub payload: Value,
    /// Epoch milliseconds, matching the Cloud Worker `Date.now()` envelope field.
    pub created_at: u64,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

impl CloudRequest {
    pub fn validate(&self) -> Result<()> {
        if self.seq == 0 || self.seq > MAX_JS_SAFE_INTEGER {
            bail!("cloud request sequence is outside JavaScript's safe integer range");
        }
        if !self.payload.is_object() {
            bail!("cloud request payload must be an object");
        }
        if self.kind == "legacy_import" {
            let text = self
                .payload
                .get("text")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("legacy_import payload must contain text"))?;
            if text.len() > MAX_LEGACY_IMPORT_BYTES {
                bail!("legacy_import payload exceeds the 256 KiB limit");
            }
        }
        let bytes = serde_json::to_vec(self).context("failed to encode cloud request")?;
        if bytes.len() > MAX_REQUEST_BYTES {
            bail!("cloud request exceeds the local persistence limit");
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct RequestList {
    database_epoch: String,
    requests: Vec<CloudRequest>,
}

#[derive(Debug, Deserialize)]
struct EpochRotationResponse {
    rotated: bool,
    database_epoch: String,
}

#[derive(Debug, Serialize)]
struct EpochRotationBody<'a> {
    seq: u64,
    request_id: &'a str,
    payload_hash: &'a str,
    kind: &'a str,
    created_at: u64,
}

#[derive(Debug, Serialize)]
struct AckReceipt<'a> {
    seq: u64,
    request_id: &'a str,
    payload_hash: &'a str,
    kind: &'a str,
    created_at: u64,
}

#[derive(Debug, Serialize)]
struct AckBody<'a> {
    receipts: Vec<AckReceipt<'a>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CloudState<'a> {
    pub state_version: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision: Option<&'a str>,
    pub reported_at: u64,
    pub library: &'a Value,
    pub previews: &'a [Value],
    pub operations: &'a [Value],
    pub tasks: &'a [Value],
    pub settings: CloudSettings<'a>,
}

impl CloudState<'_> {
    /// Encode stable JSON partitions locally and divide them into content-addressed chunks.
    /// The Worker validates each bounded chunk and commits a small manifest.
    fn chunked_snapshot(&self, enforce_complexity_limits: bool) -> Result<PreparedCloudState> {
        if enforce_complexity_limits {
            validate_cloud_json_complexity(self.library)?;
        }
        let library_json = serde_json::to_vec(self.library)
            .context("failed to serialize the cloud library partition")?;
        let mut contents = serde_json::json!({
            "operations": self.operations,
            "previews": self.previews,
            "settings": self.settings,
            "tasks": self.tasks,
        });
        if let Some(revision) = self.revision {
            contents["revision"] = serde_json::Value::String(revision.to_string());
        }
        if enforce_complexity_limits {
            validate_cloud_json_complexity(&contents)?;
        }
        let state_json = serde_json::to_vec(&contents)
            .context("failed to serialize the cloud state partition")?;

        let library_revision = match self.library.get("revision").and_then(Value::as_str) {
            Some(revision) => revision,
            None if !enforce_complexity_limits => "",
            None => bail!("cloud library revision must be a string"),
        };
        let mut unique_chunks = BTreeMap::<String, Vec<u8>>::new();
        let library_refs = chunk_cloud_partition(&library_json, &mut unique_chunks)?;
        let state_refs = chunk_cloud_partition(&state_json, &mut unique_chunks)?;
        let total_partition_bytes = library_json.len().saturating_add(state_json.len());
        let total_chunk_references = library_refs.len().saturating_add(state_refs.len());
        let manifest = serde_json::to_vec(&serde_json::json!({
            "format": "chunked-v1",
            "state_version": self.state_version,
            "reported_at": self.reported_at,
            "library_revision": library_revision,
            "library": { "bytes": library_json.len(), "chunks": library_refs },
            "state": { "bytes": state_json.len(), "chunks": state_refs },
        }))
        .context("failed to serialize the chunked cloud state manifest")?;
        // Budget logical partition bytes rather than only unique uploads. Repeated content can
        // reuse an ACKed chunk on the wire, but the Worker still has to represent every byte in
        // the two committed partitions.
        let wire_size = total_partition_bytes.saturating_add(manifest.len());
        Ok(PreparedCloudState {
            manifest,
            chunks: unique_chunks
                .into_iter()
                .map(|(sha256, bytes)| CloudStateChunk { sha256, bytes })
                .collect(),
            total_partition_bytes,
            total_chunk_references,
            wire_size,
        })
    }

    pub(crate) fn encoded_wire_size(&self) -> Result<usize> {
        // Keep sizing available to the compactor even when the upload admission complexity cap
        // is exceeded; post_state performs the bounded admission check after compaction has had
        // a chance to reduce the snapshot.
        Ok(self.chunked_snapshot(false)?.wire_size)
    }
}

#[derive(Debug)]
struct CloudStateChunk {
    sha256: String,
    bytes: Vec<u8>,
}

#[derive(Debug)]
struct PreparedCloudState {
    manifest: Vec<u8>,
    chunks: Vec<CloudStateChunk>,
    total_partition_bytes: usize,
    total_chunk_references: usize,
    wire_size: usize,
}

fn validate_cloud_json_complexity(value: &Value) -> Result<()> {
    let mut pending = vec![(value, 0_usize)];
    let mut nodes = 0_usize;
    while let Some((current, depth)) = pending.pop() {
        nodes = nodes.saturating_add(1);
        if nodes > MAX_CLOUD_STATE_JSON_NODES {
            return Err(CloudStateComplexityExceeded.into());
        }
        if depth > MAX_CLOUD_STATE_JSON_DEPTH {
            return Err(CloudStateComplexityExceeded.into());
        }
        match current {
            Value::Array(items) => {
                pending.extend(items.iter().map(|item| (item, depth + 1)));
            }
            Value::Object(fields) => {
                pending.extend(fields.values().map(|item| (item, depth + 1)));
            }
            _ => {}
        }
    }
    Ok(())
}

fn chunk_cloud_partition(
    bytes: &[u8],
    unique_chunks: &mut BTreeMap<String, Vec<u8>>,
) -> Result<Vec<Value>> {
    let text = std::str::from_utf8(bytes).context("serialized cloud state must be UTF-8")?;
    let mut references = Vec::new();
    let mut start = 0_usize;
    while start < text.len() {
        let mut end = start
            .saturating_add(CLOUD_STATE_CHUNK_BYTES)
            .min(text.len());
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        // A UTF-8 code point is at most four bytes, so this cannot produce an empty chunk.
        anyhow::ensure!(
            end > start,
            "failed to split UTF-8 cloud state at a character boundary"
        );
        let chunk = text.as_bytes()[start..end].to_vec();
        let sha256 = format!("{:x}", Sha256::digest(&chunk));
        match unique_chunks.get(&sha256) {
            Some(existing) if existing != &chunk => {
                bail!("cloud state SHA-256 collision detected while preparing chunks");
            }
            Some(_) => {}
            None => {
                unique_chunks.insert(sha256.clone(), chunk.clone());
            }
        }
        references.push(serde_json::json!({
            "sha256": sha256,
            "bytes": chunk.len(),
        }));
        start = end;
    }
    Ok(references)
}

#[derive(Debug, Clone, Serialize)]
pub struct CloudSettings<'a> {
    pub download_dir: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applied: Option<&'a Value>,
}

#[derive(Clone)]
pub struct CloudClient {
    base_url: url::Url,
    client: Client,
    authorization: HeaderValue,
    database_epoch: Option<String>,
    acknowledged_state_chunks: Arc<Mutex<BTreeMap<(String, String), Instant>>>,
}

fn parse_cloud_state_version_hint(bytes: &[u8]) -> Option<u64> {
    if bytes.is_empty() || bytes[0] == b'0' || !bytes.iter().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let version = std::str::from_utf8(bytes).ok()?.parse::<u64>().ok()?;
    (version <= MAX_CLOUD_STATE_VERSION).then_some(version)
}

#[derive(Debug)]
pub enum CloudStatePostError {
    Transport(anyhow::Error),
    HttpStatus(reqwest::StatusCode),
    Conflict { stored_version: u64 },
    DatabaseEpochChanged { expected: String, actual: String },
}

impl CloudStatePostError {
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Transport(_) | Self::Conflict { .. } => true,
            Self::DatabaseEpochChanged { .. } => false,
            Self::HttpStatus(status) => {
                *status == reqwest::StatusCode::UNAUTHORIZED
                    || *status == reqwest::StatusCode::FORBIDDEN
                    || *status == reqwest::StatusCode::CONFLICT
                    || *status == reqwest::StatusCode::REQUEST_TIMEOUT
                    || *status == reqwest::StatusCode::PRECONDITION_REQUIRED
                    || *status == reqwest::StatusCode::TOO_MANY_REQUESTS
                    || status.is_server_error()
            }
        }
    }

    pub fn is_payload_too_large(&self) -> bool {
        matches!(self, Self::HttpStatus(status) if *status == reqwest::StatusCode::PAYLOAD_TOO_LARGE)
    }

    pub fn stored_version_hint(&self) -> Option<u64> {
        match self {
            Self::Conflict { stored_version } => Some(*stored_version),
            Self::Transport(_) | Self::HttpStatus(_) | Self::DatabaseEpochChanged { .. } => None,
        }
    }
}

impl std::fmt::Display for CloudStatePostError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(error) => write!(formatter, "{error:#}"),
            Self::HttpStatus(status) => write!(formatter, "HTTP {status}"),
            Self::Conflict { stored_version } => write!(
                formatter,
                "HTTP 409; server cloud state version is {stored_version}"
            ),
            Self::DatabaseEpochChanged { expected, actual } => write!(
                formatter,
                "HTTP 409; cloud database epoch changed from {expected} to {actual}"
            ),
        }
    }
}

impl std::error::Error for CloudStatePostError {}

impl CloudClient {
    pub fn new(config: &CloudConfig) -> Result<Self> {
        let base_url = url::Url::parse(config.worker_url.trim_end_matches('/'))
            .context("cloud.worker_url must be a valid URL")?;
        if base_url.scheme() != "https" || base_url.host_str().is_none() {
            bail!("cloud.worker_url must use HTTPS");
        }
        Self::with_base_url(base_url, config.resolved_shared_secret()?)
    }

    fn with_base_url(base_url: url::Url, secret: String) -> Result<Self> {
        let authorization = HeaderValue::from_str(&format!("Bearer {secret}"))
            .context("cloud shared secret is not a valid HTTP bearer token")?;
        Ok(Self {
            base_url,
            client: Client::builder()
                .timeout(Duration::from_secs(20))
                .build()
                .context("failed to initialize local cloud client")?,
            authorization,
            database_epoch: None,
            acknowledged_state_chunks: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    pub async fn bootstrap_database_epoch(&self) -> Result<String> {
        let request_list = self.fetch_request_list().await?;
        Ok(request_list.database_epoch)
    }

    pub fn with_database_epoch(mut self, epoch: String) -> Result<Self> {
        if !valid_database_epoch(&epoch) {
            bail!("cloud database epoch must be 32 lowercase hexadecimal characters");
        }
        self.database_epoch = Some(epoch);
        Ok(self)
    }

    fn state_chunk_is_acknowledged(&self, epoch: &str, sha256: &str) -> bool {
        let now = Instant::now();
        let Ok(mut cache) = self.acknowledged_state_chunks.lock() else {
            return false;
        };
        cache.retain(|_, acknowledged_at| {
            now.saturating_duration_since(*acknowledged_at) < CLOUD_STATE_CHUNK_ACK_CACHE_TTL
        });
        cache.contains_key(&(epoch.to_string(), sha256.to_string()))
    }

    fn remember_acknowledged_state_chunk(&self, epoch: &str, sha256: &str) {
        let now = Instant::now();
        let Ok(mut cache) = self.acknowledged_state_chunks.lock() else {
            return;
        };
        cache.retain(|_, acknowledged_at| {
            now.saturating_duration_since(*acknowledged_at) < CLOUD_STATE_CHUNK_ACK_CACHE_TTL
        });
        let key = (epoch.to_string(), sha256.to_string());
        if let Some(acknowledged_at) = cache.get_mut(&key) {
            *acknowledged_at = now;
            return;
        }
        while cache.len() >= CLOUD_STATE_CHUNK_ACK_CACHE_ENTRIES {
            let Some(oldest) = cache
                .iter()
                .min_by_key(|(_, acknowledged_at)| **acknowledged_at)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            cache.remove(&oldest);
        }
        cache.insert(key, now);
    }

    fn clear_acknowledged_state_chunks(&self, epoch: &str) {
        if let Ok(mut cache) = self.acknowledged_state_chunks.lock() {
            cache.retain(|(cached_epoch, _), _| cached_epoch != epoch);
        }
    }

    async fn upload_state_chunk(
        &self,
        expected_epoch: &str,
        chunk: CloudStateChunk,
    ) -> std::result::Result<(), CloudStatePostError> {
        if self.state_chunk_is_acknowledged(expected_epoch, &chunk.sha256) {
            return Ok(());
        }
        let url = self
            .api_url(&format!("api/local/state/chunks/{}", chunk.sha256))
            .map_err(CloudStatePostError::Transport)?;
        let response = self
            .client
            .post(url)
            .header(AUTHORIZATION, self.authorization.clone())
            .header("X-Cloud-Database-Epoch", expected_epoch)
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(chunk.bytes)
            .send()
            .await
            .map_err(|error| {
                CloudStatePostError::Transport(
                    anyhow!(error).context("failed to upload local cloud state chunk"),
                )
            })?;
        let status = response.status();
        if status.is_success() {
            self.remember_acknowledged_state_chunk(expected_epoch, &chunk.sha256);
            Ok(())
        } else {
            if status == reqwest::StatusCode::CONFLICT
                && let Some(actual) = database_epoch_from_header(response.headers())
                && actual != expected_epoch
            {
                return Err(CloudStatePostError::DatabaseEpochChanged {
                    expected: expected_epoch.to_string(),
                    actual,
                });
            }
            if status == reqwest::StatusCode::PRECONDITION_REQUIRED {
                self.clear_acknowledged_state_chunks(expected_epoch);
            }
            Err(CloudStatePostError::HttpStatus(status))
        }
    }

    pub(crate) fn database_epoch(&self) -> Result<&str> {
        self.database_epoch
            .as_deref()
            .ok_or_else(|| anyhow!("cloud client database epoch has not been bootstrapped"))
    }

    #[cfg(test)]
    pub(crate) fn with_test_base_url(base_url: &str) -> Result<Self> {
        Self::with_base_url(url::Url::parse(base_url)?, TEST_CLOUD_SECRET.to_string())?
            .with_database_epoch(TEST_DATABASE_EPOCH.to_string())
    }

    fn api_url(&self, path: &str) -> Result<url::Url> {
        let mut base = self.base_url.clone();
        if !base.path().ends_with('/') {
            let path = format!("{}/", base.path());
            base.set_path(&path);
        }
        base.join(path)
            .context("failed to build local cloud API URL")
    }

    async fn fetch_request_list(&self) -> Result<RequestList> {
        let expected = self.database_epoch.as_deref();
        let page_size = if expected.is_some() {
            CLOUD_REQUEST_PAGE_SIZE
        } else {
            1
        }
        .to_string();
        let mut request = self
            .client
            .get(self.api_url("api/local/requests")?)
            .query(&[("limit", page_size.as_str())])
            .header(AUTHORIZATION, self.authorization.clone());
        if let Some(expected) = expected {
            request = request.header(
                "X-Cloud-Database-Epoch",
                HeaderValue::from_str(expected).context("invalid cloud database epoch header")?,
            );
        }
        let response = request
            .send()
            .await
            .context("failed to fetch local cloud requests")?;
        if response.status() == reqwest::StatusCode::CONFLICT
            && let Some(expected) = expected
            && let Some(actual) = database_epoch_from_header(response.headers())
            && actual != expected
        {
            return Err(CloudDatabaseEpochChanged {
                expected: expected.to_string(),
                actual,
            }
            .into());
        }
        let response = response
            .error_for_status()
            .context("local cloud request fetch returned HTTP error")?;
        let request_list = response
            .json::<RequestList>()
            .await
            .context("failed to decode local cloud request list")?;
        if !valid_database_epoch(&request_list.database_epoch) {
            bail!("local cloud request page omitted a valid database epoch");
        }
        for request in &request_list.requests {
            request.validate()?;
        }
        Ok(request_list)
    }

    pub async fn fetch_requests(&self) -> Result<Vec<CloudRequest>> {
        let expected = self.database_epoch()?.to_string();
        let request_list = self.fetch_request_list().await?;
        if request_list.database_epoch != expected {
            return Err(CloudDatabaseEpochChanged {
                expected,
                actual: request_list.database_epoch,
            }
            .into());
        }
        Ok(request_list.requests)
    }

    pub async fn acknowledge(&self, requests: &[CloudRequest]) -> Result<()> {
        if requests.is_empty() {
            return Ok(());
        }
        let mut receipts = Vec::with_capacity(requests.len());
        for request in requests {
            request.validate()?;
            if request.seq == 0 || request.seq > MAX_JS_SAFE_INTEGER {
                bail!("cannot acknowledge a sequence outside JavaScript's safe integer range");
            }
            let request_id = request
                .extra
                .get("request_id")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty() && value.len() <= 256)
                .ok_or_else(|| anyhow!("cloud request omitted its stable request identifier"))?;
            let payload_hash = request
                .extra
                .get("payload_hash")
                .and_then(Value::as_str)
                .filter(|value| {
                    value.len() == 64
                        && value
                            .as_bytes()
                            .iter()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
                })
                .ok_or_else(|| anyhow!("cloud request omitted its stable payload hash"))?;
            receipts.push(AckReceipt {
                seq: request.seq,
                request_id,
                payload_hash,
                kind: &request.kind,
                created_at: request.created_at,
            });
        }
        let expected = self.database_epoch()?.to_string();
        let response = self
            .client
            .post(self.api_url("api/local/ack")?)
            .header(AUTHORIZATION, self.authorization.clone())
            .header("X-Cloud-Database-Epoch", expected.as_str())
            .json(&AckBody { receipts })
            .send()
            .await
            .context("failed to acknowledge local cloud requests")?;
        if response.status() == reqwest::StatusCode::CONFLICT
            && let Some(actual) = database_epoch_from_header(response.headers())
            && actual != expected
        {
            return Err(CloudDatabaseEpochChanged { expected, actual }.into());
        }
        if response.status() == reqwest::StatusCode::CONFLICT
            && let Some(seq) = response
                .headers()
                .get("X-Cloud-Inbox-Sequence-Conflict")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|seq| *seq > 0 && *seq <= MAX_JS_SAFE_INTEGER)
        {
            return Err(CloudInboxSequenceConflict { seq }.into());
        }
        response
            .error_for_status()
            .context("local cloud acknowledgement returned HTTP error")?;
        Ok(())
    }

    pub async fn recover_sequence_conflict(&self, request: &CloudRequest) -> Result<()> {
        let expected = self.database_epoch()?.to_string();
        let request_id = request
            .extra
            .get("request_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty() && value.len() <= 256)
            .ok_or_else(|| anyhow!("cloud request omitted its stable request identifier"))?;
        let payload_hash = request
            .extra
            .get("payload_hash")
            .and_then(Value::as_str)
            .filter(|value| {
                value.len() == 64
                    && value
                        .as_bytes()
                        .iter()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
            })
            .ok_or_else(|| anyhow!("cloud request omitted its stable payload hash"))?;
        let response = self
            .client
            .post(self.api_url("api/local/epoch/rotate-on-conflict")?)
            .header(AUTHORIZATION, self.authorization.clone())
            .header("X-Cloud-Database-Epoch", expected.as_str())
            .json(&EpochRotationBody {
                seq: request.seq,
                request_id,
                payload_hash,
                kind: &request.kind,
                created_at: request.created_at,
            })
            .send()
            .await
            .context("failed to request conditional cloud epoch rotation")?;
        if response.status() == reqwest::StatusCode::CONFLICT {
            if let Some(actual) = database_epoch_from_header(response.headers())
                && actual != expected
            {
                return Err(CloudDatabaseEpochChanged { expected, actual }.into());
            }
            bail!("cloud epoch rotation conflict no longer matches the pending request");
        }
        let response = response
            .error_for_status()
            .context("conditional cloud epoch rotation returned HTTP error")?;
        let rotation = response
            .json::<EpochRotationResponse>()
            .await
            .context("failed to decode conditional cloud epoch rotation response")?;
        if !rotation.rotated || !valid_database_epoch(&rotation.database_epoch) {
            bail!("cloud epoch rotation returned an invalid response");
        }
        if rotation.database_epoch == expected {
            bail!("cloud epoch rotation did not change the database epoch");
        }
        Err(CloudDatabaseEpochChanged {
            expected,
            actual: rotation.database_epoch,
        }
        .into())
    }

    pub async fn post_state(
        &self,
        state: &CloudState<'_>,
    ) -> std::result::Result<(), CloudStatePostError> {
        let expected = self
            .database_epoch()
            .map_err(CloudStatePostError::Transport)?
            .to_string();
        let prepared = match state.chunked_snapshot(true) {
            Ok(prepared) => prepared,
            Err(error)
                if error
                    .downcast_ref::<CloudStateComplexityExceeded>()
                    .is_some() =>
            {
                return Err(CloudStatePostError::HttpStatus(
                    reqwest::StatusCode::PAYLOAD_TOO_LARGE,
                ));
            }
            Err(error) => return Err(CloudStatePostError::Transport(error)),
        };
        if prepared.total_partition_bytes > MAX_CLOUD_STATE_PARTITION_BYTES
            || prepared.total_chunk_references > MAX_CLOUD_STATE_CHUNKS
        {
            return Err(CloudStatePostError::HttpStatus(
                reqwest::StatusCode::PAYLOAD_TOO_LARGE,
            ));
        }

        let upload_results = stream::iter(prepared.chunks)
            .map(|chunk| self.upload_state_chunk(&expected, chunk))
            .buffer_unordered(CLOUD_STATE_CHUNK_UPLOAD_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
        for result in upload_results {
            result?;
        }

        let url = self
            .api_url("api/local/state")
            .map_err(CloudStatePostError::Transport)?;
        let response = self
            .client
            .post(url)
            .header(AUTHORIZATION, self.authorization.clone())
            .header("X-Cloud-Database-Epoch", expected.as_str())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(prepared.manifest)
            .send()
            .await
            .map_err(|error| {
                CloudStatePostError::Transport(
                    anyhow!(error).context("failed to publish local cloud state"),
                )
            })?;
        let status = response.status();
        if status.is_success() {
            Ok(())
        } else if status == reqwest::StatusCode::PRECONDITION_REQUIRED {
            self.clear_acknowledged_state_chunks(&expected);
            Err(CloudStatePostError::HttpStatus(status))
        } else if status == reqwest::StatusCode::CONFLICT {
            if let Some(actual) = database_epoch_from_header(response.headers())
                && actual != expected
            {
                return Err(CloudStatePostError::DatabaseEpochChanged { expected, actual });
            }
            match response
                .headers()
                .get("X-Cloud-State-Version")
                .and_then(|value| parse_cloud_state_version_hint(value.as_bytes()))
            {
                Some(stored_version) => Err(CloudStatePostError::Conflict { stored_version }),
                None => Err(CloudStatePostError::HttpStatus(status)),
            }
        } else {
            Err(CloudStatePostError::HttpStatus(status))
        }
    }

    pub async fn connect_websocket(&self) -> Result<CloudWebSocket> {
        let expected = self.database_epoch()?.to_string();
        let mut url = self.api_url("api/local/ws")?;
        match url.scheme() {
            "https" => url
                .set_scheme("wss")
                .map_err(|_| anyhow!("failed to select WSS"))?,
            "http" => url
                .set_scheme("ws")
                .map_err(|_| anyhow!("failed to select WS"))?,
            _ => bail!("local cloud WebSocket URL must use HTTPS or HTTP in tests"),
        }
        let mut request = url
            .as_str()
            .into_client_request()
            .context("failed to construct local cloud WebSocket request")?;
        request
            .headers_mut()
            .insert(WS_AUTHORIZATION, self.authorization.clone());
        request.headers_mut().insert(
            "X-Cloud-Database-Epoch",
            HeaderValue::from_str(&expected).context("invalid cloud database epoch header")?,
        );
        let result = timeout(Duration::from_secs(15), connect_async(request))
            .await
            .context("local cloud WebSocket handshake timed out")?;
        let (socket, _) = match result {
            Ok(connection) => connection,
            Err(tokio_tungstenite::tungstenite::Error::Http(response))
                if response.status()
                    == tokio_tungstenite::tungstenite::http::StatusCode::CONFLICT =>
            {
                let actual = response
                    .headers()
                    .get("X-Cloud-Database-Epoch")
                    .and_then(|value| value.to_str().ok())
                    .filter(|value| valid_database_epoch(value))
                    .map(str::to_string);
                if let Some(actual) = actual.filter(|actual| actual != &expected) {
                    return Err(CloudDatabaseEpochChanged { expected, actual }.into());
                }
                return Err(anyhow!(
                    "local cloud WebSocket returned an unbound epoch conflict"
                ));
            }
            Err(error) => {
                return Err(anyhow!(error).context("failed to connect to local cloud WebSocket"));
            }
        };
        Ok(socket)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InboxStatus {
    Pending,
    Processing,
    AwaitingConfirmation,
    Completed,
    Retryable,
    Failed,
    Uncertain,
    Stale,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InboxRecord {
    pub schema_version: u32,
    pub request: CloudRequest,
    pub status: InboxStatus,
    pub attempts: u32,
    /// Local epoch seconds when this request was first durably received.
    /// Older receipts omit this field; `None` means the original local time is unknown.
    #[serde(default)]
    pub received_at: Option<u64>,
    pub updated_at: u64,
    #[serde(default)]
    pub next_attempt_at: u64,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub result: Option<Value>,
}

#[derive(Default)]
struct InboxStateIndex {
    recent_management: BTreeSet<u64>,
    completed_legacy_imports: BTreeSet<u64>,
    awaiting_previews: BTreeSet<u64>,
    awaiting_preview_sequences: BTreeMap<String, BTreeSet<u64>>,
    awaiting_preview_id_by_sequence: BTreeMap<u64, String>,
    awaiting_preview_expiry_seconds_by_sequence: BTreeMap<u64, u64>,
    active_confirmations: BTreeMap<String, BTreeSet<(Option<u64>, u64)>>,
    active_confirmation_by_sequence: BTreeMap<u64, (String, Option<u64>)>,
}

impl InboxStateIndex {
    fn record(&mut self, record: &InboxRecord) {
        self.completed_legacy_imports.remove(&record.request.seq);
        self.unindex_preview_associations(record.request.seq);
        if record.request.kind != "telegram" {
            self.recent_management.insert(record.request.seq);
            while self.recent_management.len() > MAX_MANAGEMENT_STATE_RECORDS {
                if let Some(oldest) = self.recent_management.first().copied() {
                    self.recent_management.remove(&oldest);
                }
            }
        }
        if record.request.kind == "legacy_import" && record.status == InboxStatus::Completed {
            self.completed_legacy_imports.insert(record.request.seq);
            while self.completed_legacy_imports.len() > MAX_COMPLETED_LEGACY_IMPORT_RECORDS {
                if let Some(oldest) = self.completed_legacy_imports.first().copied() {
                    self.completed_legacy_imports.remove(&oldest);
                }
            }
        }
        if matches!(
            record.request.kind.as_str(),
            "file_preview" | "library_migration_preview"
        ) {
            if record.status == InboxStatus::AwaitingConfirmation {
                self.awaiting_previews.insert(record.request.seq);
                if let Some(preview_id) =
                    inbox_preview_id(record).filter(|id| preview_identifier_is_indexable(id))
                {
                    let preview_key = preview_index_key(&record.request.kind, preview_id);
                    self.awaiting_preview_sequences
                        .entry(preview_key.clone())
                        .or_default()
                        .insert(record.request.seq);
                    self.awaiting_preview_id_by_sequence
                        .insert(record.request.seq, preview_key);
                    self.awaiting_preview_expiry_seconds_by_sequence
                        .insert(record.request.seq, inbox_preview_expiry_seconds(record));
                }
            } else {
                self.awaiting_previews.remove(&record.request.seq);
            }
        }
        if matches!(
            record.request.kind.as_str(),
            "file_confirm" | "library_migration_confirm"
        ) && matches!(
            record.status,
            InboxStatus::Pending | InboxStatus::Processing | InboxStatus::Retryable
        ) && let Some(preview_id) = record
            .request
            .payload
            .get("preview_id")
            .and_then(Value::as_str)
            .filter(|id| preview_identifier_is_indexable(id))
        {
            let received_at_seconds = record.received_at;
            let preview_kind = match record.request.kind.as_str() {
                "file_confirm" => "file_preview",
                "library_migration_confirm" => "library_migration_preview",
                _ => unreachable!("confirmation kind was checked above"),
            };
            let preview_key = preview_index_key(preview_kind, preview_id);
            self.active_confirmations
                .entry(preview_key.clone())
                .or_default()
                .insert((received_at_seconds, record.request.seq));
            self.active_confirmation_by_sequence
                .insert(record.request.seq, (preview_key, received_at_seconds));
        }
    }

    fn forget(&mut self, seq: u64) {
        self.recent_management.remove(&seq);
        self.completed_legacy_imports.remove(&seq);
        self.awaiting_previews.remove(&seq);
        self.unindex_preview_associations(seq);
    }

    fn unindex_preview_associations(&mut self, seq: u64) {
        self.awaiting_preview_expiry_seconds_by_sequence
            .remove(&seq);
        if let Some(preview_id) = self.awaiting_preview_id_by_sequence.remove(&seq) {
            let remove_preview = self
                .awaiting_preview_sequences
                .get_mut(&preview_id)
                .is_some_and(|sequences| {
                    sequences.remove(&seq);
                    sequences.is_empty()
                });
            if remove_preview {
                self.awaiting_preview_sequences.remove(&preview_id);
            }
        }
        if let Some((preview_id, received_at_seconds)) =
            self.active_confirmation_by_sequence.remove(&seq)
        {
            let remove_preview =
                self.active_confirmations
                    .get_mut(&preview_id)
                    .is_some_and(|confirmations| {
                        confirmations.remove(&(received_at_seconds, seq));
                        confirmations.is_empty()
                    });
            if remove_preview {
                self.active_confirmations.remove(&preview_id);
            }
        }
    }

    fn awaiting_preview_sequence_for_kind(
        &self,
        preview_id: &str,
        preview_kind: &str,
    ) -> Option<u64> {
        self.awaiting_preview_sequences
            .get(&preview_index_key(preview_kind, preview_id))
            .and_then(|sequences| sequences.last().copied())
    }

    fn has_active_confirmation_before_kind(
        &self,
        preview_id: &str,
        preview_kind: &str,
        expires_at_seconds: u64,
    ) -> bool {
        self.active_confirmations
            .get(&preview_index_key(preview_kind, preview_id))
            .is_some_and(|confirmations| {
                confirmations
                    // Unknown legacy receipt time is retained conservatively. For known
                    // timestamps, only a confirmation received strictly before expiry counts.
                    .range(..(Some(expires_at_seconds), 0))
                    .next()
                    .is_some()
            })
    }

    fn has_preexpiry_confirmation_for_preview(&self, preview_id: &str) -> Option<bool> {
        let expiry_seconds = self
            .awaiting_preview_sequence_for_kind(preview_id, "file_preview")
            .and_then(|sequence| {
                self.awaiting_preview_expiry_seconds_by_sequence
                    .get(&sequence)
                    .copied()
            });
        match expiry_seconds {
            Some(expiry_seconds) => Some(self.has_active_confirmation_before_kind(
                preview_id,
                "file_preview",
                expiry_seconds,
            )),
            None if self
                .active_confirmations
                .get(&preview_index_key("file_preview", preview_id))
                .is_some_and(|confirmations| !confirmations.is_empty()) =>
            {
                None
            }
            None => Some(false),
        }
    }

    fn sequences_bounded(&self, max_awaiting_previews: usize) -> (BTreeSet<u64>, usize) {
        let omitted_previews = self
            .awaiting_previews
            .len()
            .saturating_sub(max_awaiting_previews);
        let selected_previews = self
            .awaiting_previews
            .iter()
            .rev()
            .take(max_awaiting_previews)
            .copied()
            .collect::<BTreeSet<_>>();
        let mut sequences = self.recent_management.clone();
        for omitted_seq in self.awaiting_previews.difference(&selected_previews) {
            sequences.remove(omitted_seq);
        }
        sequences.extend(selected_previews);
        (sequences, omitted_previews)
    }
}

fn set_pending_sequence(index: &mut BTreeSet<u64>, seq: u64, status: InboxStatus) {
    if matches!(status, InboxStatus::Pending | InboxStatus::Retryable) {
        index.insert(seq);
    } else {
        index.remove(&seq);
    }
}

fn cloud_request_sequence_from_name(name: &OsStr) -> Result<Option<u64>> {
    let Some(name) = name.to_str() else {
        if name.to_string_lossy().starts_with(CLOUD_REQUEST_PREFIX) {
            bail!("cloud inbox request filename is not valid UTF-8; entry was retained");
        }
        return Ok(None);
    };
    let Some(encoded_sequence) = name.strip_prefix(CLOUD_REQUEST_PREFIX) else {
        return Ok(None);
    };
    let Some(digits) = encoded_sequence.strip_suffix(CLOUD_REQUEST_SUFFIX) else {
        bail!("cloud inbox request filename has an invalid suffix; entry was retained");
    };
    if digits.len() != 20 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("cloud inbox request filename has an invalid sequence; entry was retained");
    }
    let seq = digits
        .parse::<u64>()
        .context("cloud inbox request filename sequence is not representable")?;
    if seq == 0 || seq > MAX_JS_SAFE_INTEGER {
        bail!("cloud inbox request filename sequence is outside the supported range");
    }
    let expected = format!("{CLOUD_REQUEST_PREFIX}{seq:020}{CLOUD_REQUEST_SUFFIX}");
    if name != expected {
        bail!("cloud inbox request filename is not canonical; entry was retained");
    }
    Ok(Some(seq))
}

fn inbox_terminal_record_expired(record: &InboxRecord, now: u64) -> bool {
    let terminal = matches!(
        record.status,
        InboxStatus::Completed
            | InboxStatus::Failed
            | InboxStatus::Uncertain
            | InboxStatus::Stale
            | InboxStatus::Cancelled
    );
    terminal
        && record.updated_at <= now
        && now.saturating_sub(record.updated_at) >= CLOUD_INBOX_TERMINAL_REPLAY_RETENTION.as_secs()
}

fn inbox_preview_id(record: &InboxRecord) -> Option<&str> {
    record
        .result
        .as_ref()
        .and_then(|result| match record.request.kind.as_str() {
            "file_preview" => result
                .get("preview_id")
                .and_then(Value::as_str)
                .or_else(|| {
                    result
                        .get("preview")
                        .and_then(|preview| preview.get("id"))
                        .and_then(Value::as_str)
                }),
            "library_migration_preview" => result
                .get("id")
                .and_then(Value::as_str)
                .or_else(|| result.get("preview_id").and_then(Value::as_str)),
            _ => None,
        })
}

fn inbox_preview_revision(record: &InboxRecord) -> Option<&str> {
    record.result.as_ref().and_then(|result| {
        result.get("revision").and_then(Value::as_str).or_else(|| {
            result
                .get("preview")
                .and_then(|preview| preview.get("revision"))
                .and_then(Value::as_str)
        })
    })
}

fn valid_preview_cancel_scalar(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.trim() == value
        && value.len() <= max_bytes
        && !value.chars().any(char::is_control)
}

fn preview_identifier_is_indexable(preview_id: &str) -> bool {
    !preview_id.is_empty()
        && preview_id.len() <= 96
        && preview_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn preview_index_key(preview_kind: &str, preview_id: &str) -> String {
    format!("{preview_kind}\0{preview_id}")
}

fn inbox_abandoned_preview_expired(record: &InboxRecord, now: u64) -> bool {
    matches!(
        record.request.kind.as_str(),
        "file_preview" | "library_migration_preview"
    ) && record.status == InboxStatus::AwaitingConfirmation
        && record.updated_at <= now
        && now.saturating_sub(record.updated_at)
            >= CLOUD_INBOX_ABANDONED_PREVIEW_RETENTION.as_secs()
}

fn inbox_preview_expiry_seconds(record: &InboxRecord) -> u64 {
    record
        .updated_at
        .saturating_add(CLOUD_INBOX_ABANDONED_PREVIEW_RETENTION.as_secs())
}

impl InboxRecord {
    /// Replace an oversized generated result before it can leave a durable request stuck in
    /// `Processing`. The original request remains available for exact-sequence deduplication.
    pub(crate) fn compact_oversized_result(&mut self) -> Result<bool> {
        let limit = MAX_REQUEST_BYTES + 64 * 1024;
        let safety_margin = 512;
        if serde_json::to_vec(self)?
            .len()
            .saturating_add(safety_margin)
            <= limit
        {
            return Ok(false);
        }

        if self.request.kind == "legacy_import"
            && self.compact_oversized_legacy_import_result(limit, safety_margin)?
        {
            return Ok(true);
        }

        self.status = InboxStatus::Failed;
        self.result = Some(serde_json::json!({
            "error": "The local operation result exceeded the receipt size limit."
        }));
        self.error = Some(
            "The operation result was too large to save; submit a smaller request if needed."
                .to_string(),
        );
        self.next_attempt_at = 0;
        if serde_json::to_vec(self)?.len() > limit {
            bail!("cloud inbox request cannot fit a compact terminal receipt");
        }
        Ok(true)
    }

    fn compact_oversized_legacy_import_result(
        &mut self,
        limit: usize,
        safety_margin: usize,
    ) -> Result<bool> {
        let Some(original_result) = self.result.as_ref() else {
            return Ok(false);
        };
        let Some(hints) = original_result.get("hints").and_then(Value::as_array) else {
            return Ok(false);
        };
        let Some(candidates) = original_result.get("candidates").and_then(Value::as_array) else {
            return Ok(false);
        };
        let hint_count = hints.len();
        if hint_count == 0 {
            return Ok(false);
        }

        let original_result = original_result.clone();
        let hints = hints.clone();
        let candidates = candidates.clone();
        let mut candidate_record = self.clone();
        let mut best_result = None;
        let mut low = 0_usize;
        let mut high = hint_count;

        while low <= high {
            let retained_hint_count = low + (high - low) / 2;
            let retained_hints = hints[..retained_hint_count].to_vec();
            let retained_candidates = candidates
                .iter()
                .filter(|candidate| {
                    candidate
                        .get("hint_index")
                        .and_then(Value::as_u64)
                        .and_then(|index| usize::try_from(index).ok())
                        .is_some_and(|index| index < retained_hint_count)
                })
                .cloned()
                .collect::<Vec<_>>();
            let mut result = original_result.clone();
            let Some(result_object) = result.as_object_mut() else {
                return Ok(false);
            };
            result_object.insert("hints".to_string(), Value::Array(retained_hints));
            result_object.insert("candidates".to_string(), Value::Array(retained_candidates));

            if retained_hint_count < hint_count {
                let warning = format!(
                    "Legacy import output was truncated to fit the local receipt size limit. The first {retained_hint_count} of {hint_count} hints and their candidates remain available; re-import a smaller selection to access omitted hints."
                );
                let warnings = result_object
                    .entry("warnings".to_string())
                    .or_insert_with(|| serde_json::json!([]));
                if !warnings.is_array() {
                    *warnings = serde_json::json!([]);
                }
                if let Some(warnings) = warnings.as_array_mut()
                    && !warnings
                        .iter()
                        .any(|entry| entry.as_str() == Some(warning.as_str()))
                {
                    warnings.push(Value::String(warning));
                }
            }

            candidate_record.result = Some(result.clone());
            if retained_hint_count > 0
                && serde_json::to_vec(&candidate_record)?
                    .len()
                    .saturating_add(safety_margin)
                    <= limit
            {
                best_result = Some(result);
                low = retained_hint_count.saturating_add(1);
            } else if retained_hint_count == 0 {
                break;
            } else {
                high = retained_hint_count - 1;
            }
        }

        if let Some(result) = best_result {
            self.result = Some(result);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedStateVersion {
    schema_version: u32,
    state_version: u64,
}

struct InboxRecoveryScan {
    cursor: Option<BoundDirectoryCursor>,
    initial_complete: bool,
    // A completed read-only pass may still be followed by bounded Processing rewrites.
    indexing_complete: bool,
    processing_recovery: BTreeSet<u64>,
    // Only a rescan hint for directory-entry churn; identity and access policy are checked apart.
    directory_entry_set_stamp: Option<BoundDirectoryEntrySetStamp>,
}

enum InboxRecordRead {
    Missing,
    Deferred,
    Present {
        record: Box<InboxRecord>,
        entry: BoundEntry,
        identity: EntryIdentity,
        file: BoundFile,
        bytes: Vec<u8>,
    },
}

struct CloudInboxOwnerLock {
    file: BoundFile,
    owner_process_id: u32,
}

impl Drop for CloudInboxOwnerLock {
    fn drop(&mut self) {
        // A forked child can temporarily retain a CLOEXEC duplicate until exec. Explicitly
        // releasing the flock when the last local inbox clone drops avoids extending ownership
        // past the lifetime of the local runtime while preserving exclusivity between clones.
        // Arc counts are copied, not shared, across fork; only the process that acquired the
        // lease may release it.
        if std::process::id() == self.owner_process_id {
            let _ = self.file.unlock_exclusive();
        }
    }
}

#[derive(Clone)]
pub struct CloudInbox {
    root: RootedFs,
    file_provider: Arc<dyn QueueFileProvider>,
    base_directory: PathBuf,
    base_directory_identity: EntryIdentity,
    epochs_directory: Option<(PathBuf, EntryIdentity)>,
    database_epoch: String,
    directory: PathBuf,
    directory_identity: EntryIdentity,
    _owner_lock: Arc<CloudInboxOwnerLock>,
    operation_lock: Arc<Mutex<()>>,
    pending_sequences: Arc<Mutex<BTreeSet<u64>>>,
    state_index: Arc<Mutex<InboxStateIndex>>,
    recovery_scan: Arc<Mutex<InboxRecoveryScan>>,
}

impl CloudInbox {
    pub(crate) fn download_root(&self) -> &Path {
        self.root.logical_root_path()
    }

    #[cfg(test)]
    pub fn open(root_path: &Path) -> Result<Self> {
        Self::open_with_file_provider(root_path, platform_queue_file_provider())
    }

    pub(crate) fn open_for_epoch(root_path: &Path, epoch: &str) -> Result<Self> {
        Self::open_for_epoch_with_file_provider(root_path, epoch, platform_queue_file_provider())
    }

    pub(crate) fn open_for_epoch_with_file_provider(
        root_path: &Path,
        epoch: &str,
        file_provider: Arc<dyn QueueFileProvider>,
    ) -> Result<Self> {
        if !valid_database_epoch(epoch) {
            bail!("cloud inbox epoch must be 32 lowercase hexadecimal characters");
        }
        Self::open_inner(root_path, file_provider, Some(epoch))
    }

    #[cfg(test)]
    pub(crate) fn open_with_file_provider(
        root_path: &Path,
        file_provider: Arc<dyn QueueFileProvider>,
    ) -> Result<Self> {
        Self::open_inner(root_path, file_provider, None)
    }

    fn open_inner(
        root_path: &Path,
        file_provider: Arc<dyn QueueFileProvider>,
        epoch: Option<&str>,
    ) -> Result<Self> {
        let root = coordinate_download_root(file_provider.as_ref(), root_path)?;
        let base_directory = root.logical_root_path().join(CLOUD_INBOX_DIRECTORY);
        coordinate_private_directory_creation(file_provider.as_ref(), &root, &base_directory)?;
        let (base_directory, base_directory_identity) =
            coordinate_bound_directory(file_provider.as_ref(), &root, &base_directory)?;
        let owner_lock =
            create_owner_lock(&base_directory, &base_directory, &root, &file_provider)?;
        let (directory, directory_identity, epochs_directory) = match epoch {
            Some(epoch) => {
                let epochs_path = base_directory.join(CLOUD_INBOX_EPOCHS_DIRECTORY);
                coordinate_private_directory_creation(file_provider.as_ref(), &root, &epochs_path)?;
                let (epochs_path, epochs_identity) =
                    coordinate_bound_directory(file_provider.as_ref(), &root, &epochs_path)?;
                let directory = epochs_path.join(epoch);
                coordinate_private_directory_creation(file_provider.as_ref(), &root, &directory)?;
                let (directory, directory_identity) =
                    coordinate_bound_directory(file_provider.as_ref(), &root, &directory)?;
                (
                    directory,
                    directory_identity,
                    Some((epochs_path, epochs_identity)),
                )
            }
            None => (base_directory.clone(), base_directory_identity, None),
        };
        let inbox = Self {
            root,
            file_provider,
            base_directory,
            base_directory_identity,
            epochs_directory,
            database_epoch: epoch.unwrap_or_default().to_string(),
            directory,
            directory_identity,
            _owner_lock: owner_lock,
            operation_lock: Arc::new(Mutex::new(())),
            pending_sequences: Arc::new(Mutex::new(BTreeSet::new())),
            state_index: Arc::new(Mutex::new(InboxStateIndex::default())),
            recovery_scan: Arc::new(Mutex::new(InboxRecoveryScan {
                cursor: None,
                initial_complete: false,
                indexing_complete: false,
                processing_recovery: BTreeSet::new(),
                directory_entry_set_stamp: None,
            })),
        };
        // The first recovery page is bounded; pending() continues the scan before any dispatch.
        // This avoids loading the complete request history into memory during startup.
        inbox.recover_interrupted_requests()?;
        Ok(inbox)
    }

    pub(crate) fn database_epoch(&self) -> &str {
        &self.database_epoch
    }

    #[cfg(test)]
    pub(crate) fn record_path_for_test(&self, seq: u64) -> PathBuf {
        self.record_path(seq)
    }

    pub fn persist(&self, request: CloudRequest) -> Result<bool> {
        request.validate()?;
        let _guard = self
            .operation_lock
            .lock()
            .map_err(|_| anyhow!("cloud inbox lock was poisoned"))?;
        let path = self.record_path(request.seq);
        if let Some(existing) = self.read_record_path(&path)? {
            if existing.request == request {
                let mut pending = self
                    .pending_sequences
                    .lock()
                    .map_err(|_| anyhow!("cloud inbox pending index lock was poisoned"))?;
                set_pending_sequence(&mut pending, request.seq, existing.status);
                self.state_index
                    .lock()
                    .map_err(|_| anyhow!("cloud inbox state index lock was poisoned"))?
                    .record(&existing);
                return Ok(false);
            }
            return Err(CloudInboxSequenceConflict { seq: request.seq }.into());
        }
        self.require_initial_recovery_complete_before_write()?;
        let received_at = now_seconds();
        let record = InboxRecord {
            schema_version: INBOX_RECORD_VERSION,
            request,
            status: InboxStatus::Pending,
            attempts: 0,
            received_at: Some(received_at),
            updated_at: received_at,
            next_attempt_at: 0,
            error: None,
            result: None,
        };
        self.write_record_path(&path, &record)?;
        self.pending_sequences
            .lock()
            .map_err(|_| anyhow!("cloud inbox pending index lock was poisoned"))?
            .insert(record.request.seq);
        self.state_index
            .lock()
            .map_err(|_| anyhow!("cloud inbox state index lock was poisoned"))?
            .record(&record);
        Ok(true)
    }

    #[cfg(test)]
    pub fn records(&self) -> Result<Vec<InboxRecord>> {
        let _guard = self
            .operation_lock
            .lock()
            .map_err(|_| anyhow!("cloud inbox lock was poisoned"))?;
        if !self.recover_interrupted_requests()? {
            bail!("cloud inbox startup recovery is still in progress");
        }
        let paths = self.list_record_paths()?;
        paths
            .into_iter()
            .map(|path| {
                self.read_record_path(&path)?
                    .ok_or_else(|| anyhow!("cloud inbox record disappeared: {}", path.display()))
            })
            .collect()
    }

    /// Return recent management history and the newest bounded set of active previews.
    /// The omitted count comes from the index, so older preview records are not read merely to
    /// count them for a cloud snapshot.
    pub fn state_records_bounded(
        &self,
        max_awaiting_previews: usize,
    ) -> Result<(Vec<InboxRecord>, usize)> {
        let _guard = self
            .operation_lock
            .lock()
            .map_err(|_| anyhow!("cloud inbox lock was poisoned"))?;
        if !self.recover_interrupted_requests()? {
            bail!("cloud inbox startup recovery is still in progress");
        }
        let index = self
            .state_index
            .lock()
            .map_err(|_| anyhow!("cloud inbox state index lock was poisoned"))?;
        let (sequences, omitted_previews) = index.sequences_bounded(max_awaiting_previews);
        let mut records = Vec::with_capacity(sequences.len());
        for seq in sequences {
            let mut record = self
                .read_record_path(&self.record_path(seq))?
                .ok_or_else(|| anyhow!("indexed cloud state request {seq} is missing"))?;
            if inbox_abandoned_preview_expired(&record, now_seconds()) {
                // Active confirmations protect the durable receipt for recovery, but a new
                // confirmation created after expiry is rejected. Always project the expired
                // preview as stale so snapshots cannot offer a confirmation that will fail.
                record.status = InboxStatus::Stale;
                record.error =
                    Some("This preview expired after seven days. Run a fresh preview.".to_string());
            }
            records.push(record);
        }
        Ok((records, omitted_previews))
    }

    /// Return the newest completed legacy-import receipts needed to validate visible mappings,
    /// ordered by sequence from oldest to newest. This private index is separate from the
    /// management history published to the UI.
    pub fn completed_legacy_import_records_bounded(&self) -> Result<Vec<InboxRecord>> {
        let _guard = self
            .operation_lock
            .lock()
            .map_err(|_| anyhow!("cloud inbox lock was poisoned"))?;
        if !self.recover_interrupted_requests()? {
            bail!("cloud inbox startup recovery is still in progress");
        }
        let sequences = self
            .state_index
            .lock()
            .map_err(|_| anyhow!("cloud inbox state index lock was poisoned"))?
            .completed_legacy_imports
            .iter()
            .copied()
            .collect::<Vec<_>>();

        let mut records = Vec::with_capacity(sequences.len());
        for seq in sequences {
            let record = self
                .read_record_path(&self.record_path(seq))?
                .ok_or_else(|| anyhow!("indexed completed legacy import {seq} is missing"))?;
            if record.request.seq != seq
                || record.request.kind != "legacy_import"
                || record.status != InboxStatus::Completed
            {
                bail!("indexed completed legacy import {seq} changed before it was read");
            }
            if self.inbox_record_retention_expired(&record)? {
                continue;
            }
            records.push(record);
        }
        Ok(records)
    }

    pub fn record(&self, seq: u64) -> Result<Option<InboxRecord>> {
        let _guard = self
            .operation_lock
            .lock()
            .map_err(|_| anyhow!("cloud inbox lock was poisoned"))?;
        self.read_record_path(&self.record_path(seq))
    }

    /// Resolve one active preview through the recovery-built preview index and re-read only its
    /// durable inbox records. Both the preview expiry and confirmation receipt time are local
    /// epoch seconds; the Worker-provided request creation time is deliberately not used because
    /// it may come from a skewed clock. Legacy confirmation receipts without a local receive time
    /// remain recoverable and conservatively protect their preview until they become terminal.
    pub fn awaiting_confirmation_record(
        &self,
        preview_id: &str,
        confirmation_seq: u64,
    ) -> Result<Option<InboxRecord>> {
        if !preview_identifier_is_indexable(preview_id) {
            return Ok(None);
        }
        let _guard = self
            .operation_lock
            .lock()
            .map_err(|_| anyhow!("cloud inbox lock was poisoned"))?;
        if !self.recover_interrupted_requests()? {
            bail!("cloud inbox startup recovery is still in progress");
        }
        let Some(confirmation) = self.read_record_path(&self.record_path(confirmation_seq))? else {
            return Ok(None);
        };
        let (preview_kind, confirmation_kind) = match confirmation.request.kind.as_str() {
            "file_confirm" => ("file_preview", "file_confirm"),
            "library_migration_confirm" => {
                ("library_migration_preview", "library_migration_confirm")
            }
            _ => return Ok(None),
        };
        if confirmation.request.seq != confirmation_seq
            || confirmation.request.kind != confirmation_kind
            || !matches!(
                confirmation.status,
                InboxStatus::Pending | InboxStatus::Processing | InboxStatus::Retryable
            )
            || confirmation
                .request
                .payload
                .get("preview_id")
                .and_then(Value::as_str)
                != Some(preview_id)
        {
            return Ok(None);
        }
        let sequence = self
            .state_index
            .lock()
            .map_err(|_| anyhow!("cloud inbox state index lock was poisoned"))?
            .awaiting_preview_sequence_for_kind(preview_id, preview_kind);
        let Some(sequence) = sequence else {
            return Ok(None);
        };
        let Some(record) = self.read_record_path(&self.record_path(sequence))? else {
            bail!("indexed cloud preview request {sequence} is missing");
        };
        if record.request.seq != sequence
            || record.request.kind != preview_kind
            || record.status != InboxStatus::AwaitingConfirmation
            || inbox_preview_id(&record) != Some(preview_id)
        {
            bail!("indexed cloud preview request {sequence} no longer matches its index");
        }
        if confirmation
            .received_at
            .is_some_and(|received_at| received_at >= inbox_preview_expiry_seconds(&record))
        {
            return Ok(None);
        }
        Ok(Some(record))
    }

    /// Atomically cancel the exact persisted preview if it is still awaiting confirmation.
    /// Repeating cancellation for the same already-cancelled target is safe and idempotent.
    pub(crate) fn cancel_awaiting_preview(
        &self,
        preview_kind: &str,
        preview_seq: u64,
        preview_id: &str,
        revision: &str,
    ) -> Result<Option<InboxRecord>> {
        if !matches!(preview_kind, "file_preview" | "library_migration_preview")
            || preview_seq == 0
            || preview_seq > MAX_JS_SAFE_INTEGER
            || !valid_preview_cancel_scalar(preview_id, MAX_PREVIEW_CANCEL_ID_BYTES)
            || !valid_preview_cancel_scalar(revision, MAX_PREVIEW_CANCEL_REVISION_BYTES)
        {
            return Ok(None);
        }

        let _guard = self
            .operation_lock
            .lock()
            .map_err(|_| anyhow!("cloud inbox lock was poisoned"))?;
        self.require_initial_recovery_complete_before_write()?;

        let path = self.record_path(preview_seq);
        let Some(target) = self.read_record_path(&path)? else {
            return Ok(None);
        };
        if target.request.seq != preview_seq
            || target.request.kind != preview_kind
            || inbox_preview_id(&target) != Some(preview_id)
            || inbox_preview_revision(&target) != Some(revision)
        {
            return Ok(None);
        }
        if target.status == InboxStatus::Cancelled {
            return Ok(Some(target));
        }
        if target.status != InboxStatus::AwaitingConfirmation
            || inbox_abandoned_preview_expired(&target, now_seconds())
        {
            return Ok(None);
        }

        let preview_key = preview_index_key(preview_kind, preview_id);
        if self
            .state_index
            .lock()
            .map_err(|_| anyhow!("cloud inbox state index lock was poisoned"))?
            .active_confirmations
            .get(&preview_key)
            .is_some_and(|confirmations| !confirmations.is_empty())
        {
            return Ok(None);
        }

        self.update_checked_locked(preview_seq, |record| {
            if record.request.seq != preview_seq
                || record.request.kind != preview_kind
                || record.status != InboxStatus::AwaitingConfirmation
                || inbox_abandoned_preview_expired(record, now_seconds())
                || inbox_preview_id(record) != Some(preview_id)
                || inbox_preview_revision(record) != Some(revision)
            {
                bail!("preview is no longer awaiting cancellation");
            }
            record.status = InboxStatus::Cancelled;
            record.next_attempt_at = 0;
            record.error = Some("This preview was cancelled before confirmation.".to_string());
            Ok(())
        })
        .map(Some)
    }

    pub fn pending(&self) -> Result<Vec<InboxRecord>> {
        let _guard = self
            .operation_lock
            .lock()
            .map_err(|_| anyhow!("cloud inbox lock was poisoned"))?;
        if !self.recover_interrupted_requests()? {
            return Ok(Vec::new());
        }
        let sequences = self
            .pending_sequences
            .lock()
            .map_err(|_| anyhow!("cloud inbox pending index lock was poisoned"))?
            .iter()
            .copied()
            .collect::<Vec<_>>();
        let now = now_seconds();
        let mut pending = Vec::with_capacity(sequences.len());
        for seq in sequences {
            let record = self
                .read_record_path(&self.record_path(seq))?
                .ok_or_else(|| anyhow!("indexed cloud inbox request {seq} is missing"))?;
            if matches!(record.status, InboxStatus::Pending | InboxStatus::Retryable)
                && record.next_attempt_at <= now
            {
                pending.push(record);
            }
        }
        pending.sort_by_key(|record| record.request.seq);
        Ok(pending)
    }

    pub fn update(&self, seq: u64, update: impl FnOnce(&mut InboxRecord)) -> Result<InboxRecord> {
        self.update_checked(seq, |record| {
            update(record);
            Ok(())
        })
    }

    pub(crate) fn persist_dispatch_result(
        &self,
        seq: u64,
        status: InboxStatus,
        result: Option<Value>,
        error: Option<String>,
    ) -> Result<InboxRecord> {
        self.update_checked(seq, |record| {
            record.status = status;
            record.result = result;
            record.error = error;
            record.next_attempt_at = 0;
            record.compact_oversized_result()?;
            Ok(())
        })
    }

    fn update_checked(
        &self,
        seq: u64,
        update: impl FnOnce(&mut InboxRecord) -> Result<()>,
    ) -> Result<InboxRecord> {
        let _guard = self
            .operation_lock
            .lock()
            .map_err(|_| anyhow!("cloud inbox lock was poisoned"))?;
        self.update_checked_locked(seq, update)
    }

    fn update_checked_locked(
        &self,
        seq: u64,
        update: impl FnOnce(&mut InboxRecord) -> Result<()>,
    ) -> Result<InboxRecord> {
        self.require_initial_recovery_complete_before_write()?;
        let path = self.record_path(seq);
        let mut record = self
            .read_record_path(&path)?
            .ok_or_else(|| anyhow!("cloud inbox request {seq} is missing"))?;
        update(&mut record)?;
        record.updated_at = now_seconds();
        self.write_record_path(&path, &record)?;
        let mut pending = self
            .pending_sequences
            .lock()
            .map_err(|_| anyhow!("cloud inbox pending index lock was poisoned"))?;
        let mut state_index = self
            .state_index
            .lock()
            .map_err(|_| anyhow!("cloud inbox state index lock was poisoned"))?;
        set_pending_sequence(&mut pending, seq, record.status);
        state_index.record(&record);
        Ok(record)
    }

    pub fn next_state_version(&self) -> Result<u64> {
        self.next_state_version_after(0)
    }

    pub fn next_state_version_after(&self, observed_version: u64) -> Result<u64> {
        let _guard = self
            .operation_lock
            .lock()
            .map_err(|_| anyhow!("cloud inbox lock was poisoned"))?;
        self.require_initial_recovery_complete_before_write()?;
        if observed_version > MAX_JS_SAFE_INTEGER {
            bail!("observed cloud state version is outside JavaScript's safe integer range");
        }
        let path = self.directory.join(CLOUD_STATE_VERSION_FILE);
        let current = self.read_state_version(&path)?.unwrap_or(0);
        let next = current
            .max(observed_version)
            .checked_add(1)
            .ok_or(CloudStateVersionExhausted)?;
        if next > MAX_CLOUD_STATE_VERSION {
            return Err(CloudStateVersionExhausted.into());
        }
        self.write_state_version(&path, next)?;
        Ok(next)
    }

    fn record_path(&self, seq: u64) -> PathBuf {
        self.directory.join(format!(
            "{CLOUD_REQUEST_PREFIX}{seq:020}{CLOUD_REQUEST_SUFFIX}"
        ))
    }

    #[cfg(test)]
    fn list_record_paths(&self) -> Result<Vec<PathBuf>> {
        let mut entries = None;
        let mut list = |coordinated_path: &Path| -> Result<()> {
            let coordinated = coordinated_path_under_root(&self.root, coordinated_path)?;
            if coordinated != self.directory {
                bail!("File Provider changed the cloud inbox directory path");
            }
            let entry = self.root.bind_entry(&self.directory, false)?;
            self.root
                .validate_private_bound_directory(&entry, self.directory_identity, 0o700)?;
            let listed = self
                .root
                .list_bound_directory(&entry, self.directory_identity)?;
            entries = Some(listed);
            Ok(())
        };
        self.file_provider
            .coordinate_read(&self.directory, &mut list)
            .map_err(|error| classify_deadlock_error(&self.directory, "read", error))?;
        let entries =
            entries.ok_or_else(|| anyhow!("File Provider did not provide cloud inbox listing"))?;
        let mut paths = Vec::new();
        for (name, identity) in entries {
            let Some(seq) = cloud_request_sequence_from_name(&name)? else {
                continue;
            };
            if !identity.is_file() {
                bail!(
                    "cloud inbox request path is not a regular file: {}",
                    name.to_string_lossy()
                );
            }
            paths.push(self.record_path(seq));
        }
        paths.sort();
        Ok(paths)
    }

    fn read_record_path(&self, path: &Path) -> Result<Option<InboxRecord>> {
        // A File Provider placeholder may materialize during coordinated access; only a clear
        // missing result before coordination is used as absence. Failed revalidation stays an
        // error so a cloud acknowledgement is never based on an unreadable or replaced record.
        match self.root.entry_identity(path) {
            Ok(Some(_)) => {}
            Ok(None) => return Ok(None),
            Err(error) if is_deadlock_error(&error) => {}
            Err(error) => return Err(error),
        }
        let mut record = None;
        let mut read = |coordinated_path: &Path| -> Result<()> {
            let coordinated = coordinated_path_under_root(&self.root, coordinated_path)?;
            if coordinated != path {
                bail!("File Provider changed a cloud inbox record path");
            }
            self.validate_directory()?;
            let file = self
                .root
                .open_bound_file(&coordinated)?
                .ok_or_else(|| anyhow!("cloud inbox record disappeared during coordinated read"))?;
            file.validate_private_single_link(0o600)
                .context("cloud inbox record is not owner-private")?;
            if file.byte_len()? > MAX_REQUEST_BYTES as u64 + 64 * 1024 {
                bail!("cloud inbox record exceeds the local persistence limit");
            }
            let bytes = file.read_limited(MAX_REQUEST_BYTES + 64 * 1024)?;
            let parsed: InboxRecord =
                serde_json::from_slice(&bytes).context("cloud inbox record is invalid")?;
            if parsed.schema_version != INBOX_RECORD_VERSION {
                bail!("unsupported cloud inbox record version");
            }
            parsed.request.validate()?;
            record = Some(parsed);
            Ok(())
        };
        self.file_provider
            .coordinate_read(path, &mut read)
            .map_err(|error| classify_deadlock_error(path, "read", error))?;
        Ok(record)
    }

    fn read_recovery_record(
        &self,
        path: &Path,
        expected: EntryIdentity,
        remaining_bytes: usize,
    ) -> Result<InboxRecordRead> {
        let mut result = None;
        let mut read = |coordinated_path: &Path| -> Result<()> {
            let coordinated = coordinated_path_under_root(&self.root, coordinated_path)?;
            if coordinated != path {
                bail!("File Provider changed a cloud inbox record path");
            }
            self.validate_directory()?;
            let entry = self.root.bind_entry(path, false)?;
            match self.root.bound_entry_identity(&entry)? {
                None => {
                    result = Some(InboxRecordRead::Missing);
                    return Ok(());
                }
                Some(identity) if identity != expected => {
                    bail!("cloud inbox request path was replaced during recovery")
                }
                Some(_) => {}
            }
            let file = self.root.open_bound_file_if_identity(&entry, expected)?;
            file.validate_private_single_link(0o600)
                .context("cloud inbox record is not owner-private")?;
            let byte_len = usize::try_from(file.byte_len()?)
                .context("cloud inbox record length is not representable")?;
            if byte_len > MAX_REQUEST_BYTES + 64 * 1024 {
                bail!("cloud inbox record exceeds the local persistence limit");
            }
            if byte_len.saturating_add(1) > remaining_bytes {
                result = Some(InboxRecordRead::Deferred);
                return Ok(());
            }
            let bytes = file.read_limited(byte_len)?;
            if bytes.len() != byte_len || usize::try_from(file.byte_len()?)? != byte_len {
                bail!("cloud inbox record changed length while being read");
            }
            file.validate_private_single_link(0o600)
                .context("cloud inbox record access policy changed while being read")?;
            if self.root.bound_entry_identity(&entry)? != Some(expected) {
                bail!("cloud inbox request path was replaced while being read");
            }
            let record: InboxRecord =
                serde_json::from_slice(&bytes).context("cloud inbox record is invalid")?;
            if record.schema_version != INBOX_RECORD_VERSION {
                bail!("unsupported cloud inbox record version");
            }
            record.request.validate()?;
            result = Some(InboxRecordRead::Present {
                record: Box::new(record),
                entry,
                identity: expected,
                file,
                bytes,
            });
            Ok(())
        };
        self.file_provider
            .coordinate_read(path, &mut read)
            .map_err(|error| classify_deadlock_error(path, "read", error))?;
        result.ok_or_else(|| anyhow!("File Provider did not provide cloud inbox record access"))
    }

    fn read_state_version(&self, path: &Path) -> Result<Option<u64>> {
        match self.root.entry_identity(path) {
            Ok(Some(_)) => {}
            Ok(None) => return Ok(None),
            Err(error) if is_deadlock_error(&error) => {}
            Err(error) => return Err(error),
        }
        let mut version = None;
        let mut read = |coordinated_path: &Path| -> Result<()> {
            let coordinated = coordinated_path_under_root(&self.root, coordinated_path)?;
            if coordinated != path {
                bail!("File Provider changed the cloud state-version path");
            }
            self.validate_directory()?;
            let file = self
                .root
                .open_bound_file(path)?
                .ok_or_else(|| anyhow!("cloud state-version file disappeared during read"))?;
            file.validate_private_single_link(0o600)?;
            if file.byte_len()? > 4096 {
                bail!("cloud state-version file exceeds its limit");
            }
            let bytes = file.read_limited(4096)?;
            let state: PersistedStateVersion =
                serde_json::from_slice(&bytes).context("cloud state-version file is invalid")?;
            if state.schema_version != INBOX_RECORD_VERSION {
                bail!("unsupported cloud state-version file version");
            }
            version = Some(state.state_version);
            Ok(())
        };
        self.file_provider
            .coordinate_read(path, &mut read)
            .map_err(|error| classify_deadlock_error(path, "read", error))?;
        Ok(version)
    }

    fn write_state_version(&self, path: &Path, state_version: u64) -> Result<()> {
        let bytes = serde_json::to_vec(&PersistedStateVersion {
            schema_version: INBOX_RECORD_VERSION,
            state_version,
        })?;
        let mut write = |coordinated_path: &Path| -> Result<()> {
            let coordinated = coordinated_path_under_root(&self.root, coordinated_path)?;
            if coordinated != path {
                bail!("File Provider changed the cloud state-version path");
            }
            self.validate_directory()?;
            if let Some(file) = self.root.open_bound_file(path)? {
                file.validate_private_single_link(0o600)?;
                let entry = self.root.bind_entry(path, false)?;
                self.root.replace_bound_file_atomically_if_identity(
                    &entry,
                    file.identity(),
                    &temporary_sibling(path),
                    &bytes,
                    0o600,
                )?;
            } else {
                self.root.create_new_bound_file(path, &bytes, 0o600)?;
            }
            Ok(())
        };
        self.file_provider
            .coordinate_write(path, &mut write)
            .map_err(|error| classify_deadlock_error(path, "write", error))
    }

    fn write_record_path(&self, path: &Path, record: &InboxRecord) -> Result<()> {
        let bytes = serde_json::to_vec(record).context("failed to encode cloud inbox record")?;
        if bytes.len() > MAX_REQUEST_BYTES + 64 * 1024 {
            bail!("cloud inbox record exceeds the local persistence limit");
        }
        let mut write = |coordinated_path: &Path| -> Result<()> {
            let coordinated = coordinated_path_under_root(&self.root, coordinated_path)?;
            if coordinated != path {
                bail!("File Provider changed a cloud inbox record path");
            }
            self.validate_directory()?;
            let temporary = temporary_sibling(path);
            if let Some(file) = self.root.open_bound_file(path)? {
                file.validate_private_single_link(0o600)
                    .context("cloud inbox record is not owner-private")?;
                let entry = self.root.bind_entry(path, false)?;
                self.root.replace_bound_file_atomically_if_identity(
                    &entry,
                    file.identity(),
                    &temporary,
                    &bytes,
                    0o600,
                )?;
            } else {
                let (source, identity) =
                    self.root.create_new_bound_file(&temporary, &bytes, 0o600)?;
                let destination = self.root.bind_entry(path, false)?;
                self.root.rename_via_bound_parents_noreplace_if_identity(
                    &source,
                    &destination,
                    identity,
                )?;
            }
            Ok(())
        };
        self.file_provider
            .coordinate_write(path, &mut write)
            .map_err(|error| classify_deadlock_error(path, "write", error))
    }

    fn validate_directory(&self) -> Result<()> {
        let base_entry = self.root.bind_entry(&self.base_directory, false)?;
        self.root
            .validate_private_bound_directory(&base_entry, self.base_directory_identity, 0o700)
            .context("cloud inbox base directory is not owner-private")?;
        if let Some((epochs_directory, epochs_identity)) = &self.epochs_directory {
            let epochs_entry = self.root.bind_entry(epochs_directory, false)?;
            self.root
                .validate_private_bound_directory(&epochs_entry, *epochs_identity, 0o700)
                .context("cloud inbox epoch directory is not owner-private")?;
        }
        let entry = self.root.bind_entry(&self.directory, false)?;
        self.root
            .validate_private_bound_directory(&entry, self.directory_identity, 0o700)
            .context("cloud inbox directory is not owner-private")
    }

    fn retired_namespace_may_exist(&self) -> Result<bool> {
        if self.database_epoch.is_empty() {
            return Ok(false);
        }
        let Some((epochs_directory, epochs_identity)) = &self.epochs_directory else {
            return Ok(true);
        };
        self.validate_directory()?;

        let Some(base_names) =
            self.bounded_directory_names(&self.base_directory, self.base_directory_identity)?
        else {
            return Ok(true);
        };
        for (name, identity) in base_names {
            let Some(name) = name.to_str() else {
                return Ok(true);
            };
            match name {
                CLOUD_INBOX_OWNER_LOCK | CLOUD_STATE_VERSION_FILE => {}
                CLOUD_INBOX_EPOCHS_DIRECTORY if identity.is_dir() => {}
                name if name.starts_with(CLOUD_REQUEST_PREFIX) => return Ok(true),
                _ => return Ok(true),
            }
        }

        let Some(epoch_names) = self.bounded_directory_names(epochs_directory, *epochs_identity)?
        else {
            return Ok(true);
        };
        for (name, identity) in epoch_names {
            if name.to_str() != Some(self.database_epoch.as_str()) || !identity.is_dir() {
                // Any sibling may contain old confirmations; keep all private previews until
                // there is a retirement-aware cleanup policy.
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn bounded_directory_names(
        &self,
        path: &Path,
        expected: EntryIdentity,
    ) -> Result<Option<Vec<(std::ffi::OsString, EntryIdentity)>>> {
        const MAX_RETIRED_DIRECTORY_NAMES: usize = 128;
        const PAGE_NAMES: usize = 16;

        let mut result = None;
        let mut read = |coordinated_path: &Path| -> Result<()> {
            let coordinated = coordinated_path_under_root(&self.root, coordinated_path)?;
            if coordinated != path {
                bail!("File Provider changed a retired cloud inbox directory path");
            }
            let entry = self.root.bind_entry(path, false)?;
            let mut cursor = self
                .root
                .start_bound_directory_cursor(&entry, expected, 0o700)?;
            let mut examined = 0usize;
            let mut names = Vec::new();
            loop {
                let page = self.root.peek_bound_directory_cursor(
                    &mut cursor,
                    PAGE_NAMES.min(MAX_RETIRED_DIRECTORY_NAMES.saturating_sub(examined)),
                    0o700,
                )?;
                examined = examined.saturating_add(page.names_read);
                match page.entry {
                    Some(BoundDirectoryCursorEntry::Unreadable { .. }) => {
                        result = Some(None);
                        return Ok(());
                    }
                    Some(BoundDirectoryCursorEntry::Present { name, identity }) => {
                        names.push((name, identity));
                        self.root.consume_bound_directory_cursor_entry(&mut cursor);
                    }
                    None if page.exhausted => {
                        result = Some(Some(names));
                        return Ok(());
                    }
                    None if examined >= MAX_RETIRED_DIRECTORY_NAMES => {
                        let extra = self
                            .root
                            .peek_bound_directory_cursor(&mut cursor, 1, 0o700)?;
                        if extra.entry.is_some() || !extra.exhausted {
                            result = Some(None);
                        } else {
                            result = Some(Some(names));
                        }
                        return Ok(());
                    }
                    None => {}
                }
            }
        };
        self.file_provider
            .coordinate_read(path, &mut read)
            .map_err(|error| classify_deadlock_error(path, "read", error))?;
        result
            .ok_or_else(|| anyhow!("File Provider did not provide retired inbox directory access"))
    }

    fn recover_interrupted_requests(&self) -> Result<bool> {
        let mut scan = self
            .recovery_scan
            .lock()
            .map_err(|_| anyhow!("cloud inbox recovery cursor lock was poisoned"))?;
        if scan.initial_complete {
            // Retention runs only after the startup index is complete. Deletions can perturb the
            // cursor, so later bounded calls resume with fresh passes until it reaches every row.
            self.prune_terminal_receipts_page(&mut scan)?;
            return Ok(true);
        }

        if !scan.indexing_complete {
            if !self.index_initial_recovery_page(&mut scan)? {
                return Ok(false);
            }
            if scan.processing_recovery.is_empty() {
                scan.initial_complete = true;
                return Ok(true);
            }
            // Processing rewrites use temporary siblings, so start them after this cursor closes.
            return Ok(false);
        }

        self.recover_processing_records_page(&mut scan)?;
        if scan.processing_recovery.is_empty() {
            // Rebuild the index after the bounded rewrites, using a clean read-only pass.
            self.reset_incomplete_recovery(&mut scan)?;
        }
        Ok(false)
    }

    fn index_initial_recovery_page(&self, scan: &mut InboxRecoveryScan) -> Result<bool> {
        let mut names_read = 0usize;
        let mut bytes_read = 0usize;

        loop {
            let names_left = CLOUD_INBOX_SCAN_MAX_NAMES.saturating_sub(names_read);
            let page = self.peek_recovery_cursor(scan, names_left)?;
            names_read = names_read.saturating_add(page.names_read);
            let Some(entry) = page.entry else {
                if page.exhausted {
                    if self.initial_enumeration_changed(scan)? {
                        self.reset_incomplete_recovery(scan)?;
                        return Ok(false);
                    }
                    scan.cursor = None;
                    scan.directory_entry_set_stamp = None;
                    scan.indexing_complete = true;
                    return Ok(true);
                }
                if names_read >= CLOUD_INBOX_SCAN_MAX_NAMES || page.names_read == 0 {
                    break;
                }
                continue;
            };

            match entry {
                BoundDirectoryCursorEntry::Unreadable { name, error } => {
                    bail!(
                        "cloud inbox directory entry is unreadable and was retained: {}: {error}",
                        name.to_string_lossy()
                    );
                }
                BoundDirectoryCursorEntry::Present { name, identity } => {
                    let Some(seq) = cloud_request_sequence_from_name(&name)? else {
                        self.root.consume_bound_directory_cursor_entry(
                            scan.cursor
                                .as_mut()
                                .ok_or_else(|| anyhow!("cloud inbox recovery cursor is missing"))?,
                        );
                        continue;
                    };
                    if !identity.is_file() {
                        bail!(
                            "cloud inbox request path is not a regular file: {}",
                            name.to_string_lossy()
                        );
                    }
                    let path = self.record_path(seq);
                    let remaining_bytes = CLOUD_INBOX_SCAN_MAX_BYTES.saturating_sub(bytes_read);
                    match self.read_recovery_record(&path, identity, remaining_bytes)? {
                        InboxRecordRead::Missing => {
                            bail!(
                                "cloud inbox request disappeared during recovery: {}",
                                path.display()
                            );
                        }
                        InboxRecordRead::Deferred => break,
                        InboxRecordRead::Present { record, bytes, .. } => {
                            bytes_read = bytes_read.saturating_add(bytes.len().saturating_add(1));
                            if record.request.seq != seq {
                                bail!(
                                    "cloud inbox request sequence does not match its filename: {}",
                                    path.display()
                                );
                            }
                            if record.status == InboxStatus::Processing {
                                scan.processing_recovery.insert(seq);
                            }
                            self.index_record(&record)?;
                            self.root.consume_bound_directory_cursor_entry(
                                scan.cursor.as_mut().ok_or_else(|| {
                                    anyhow!("cloud inbox recovery cursor is missing")
                                })?,
                            );
                        }
                    }
                }
            }
        }
        Ok(scan.indexing_complete)
    }

    fn recover_processing_records_page(&self, scan: &mut InboxRecoveryScan) -> Result<()> {
        let mut names_read = 0usize;
        let mut bytes_read = 0usize;
        while names_read < CLOUD_INBOX_SCAN_MAX_NAMES {
            let Some(seq) = scan.processing_recovery.iter().next().copied() else {
                break;
            };
            let path = self.record_path(seq);
            let entry = self.root.bind_entry(&path, false)?;
            let identity = self.root.bound_entry_identity(&entry)?.ok_or_else(|| {
                anyhow!(
                    "cloud inbox request disappeared during recovery: {}",
                    path.display()
                )
            })?;
            if !identity.is_file() {
                bail!(
                    "cloud inbox request path is not a regular file: {}",
                    path.display()
                );
            }
            let remaining_bytes = CLOUD_INBOX_SCAN_MAX_BYTES.saturating_sub(bytes_read);
            match self.read_recovery_record(&path, identity, remaining_bytes)? {
                InboxRecordRead::Missing => {
                    bail!(
                        "cloud inbox request disappeared during recovery: {}",
                        path.display()
                    );
                }
                InboxRecordRead::Deferred => break,
                InboxRecordRead::Present {
                    mut record, bytes, ..
                } => {
                    bytes_read = bytes_read.saturating_add(bytes.len().saturating_add(1));
                    if record.request.seq != seq {
                        bail!(
                            "cloud inbox request sequence does not match its filename: {}",
                            path.display()
                        );
                    }
                    if record.status == InboxStatus::Processing {
                        if record.request.kind == "telegram" {
                            record.status = InboxStatus::Uncertain;
                            record.error = Some(
                                "The bot stopped during this Telegram command. Check the task queue before resending."
                                    .to_string(),
                            );
                        } else {
                            record.status = InboxStatus::Pending;
                            record.attempts = record.attempts.saturating_add(1);
                            record.next_attempt_at = 0;
                            record.error = Some(
                                "Recovered an interrupted local operation; retrying from its durable request."
                                    .to_string(),
                            );
                        }
                        record.updated_at = now_seconds();
                        self.write_record_path(&path, &record)?;
                    }
                    self.index_record(&record)?;
                    scan.processing_recovery.remove(&seq);
                    names_read = names_read.saturating_add(1);
                }
            }
        }
        Ok(())
    }

    fn initial_enumeration_changed(&self, scan: &InboxRecoveryScan) -> Result<bool> {
        let expected = scan
            .directory_entry_set_stamp
            .ok_or_else(|| anyhow!("cloud inbox initial enumeration stamp is missing"))?;
        let mut current = None;
        let mut read = |coordinated_path: &Path| -> Result<()> {
            let coordinated = coordinated_path_under_root(&self.root, coordinated_path)?;
            if coordinated != self.directory {
                bail!("File Provider changed the cloud inbox directory path");
            }
            self.validate_directory()?;
            let entry = self.root.bind_entry(&self.directory, false)?;
            current = Some(self.root.bound_directory_entry_set_stamp(
                &entry,
                self.directory_identity,
                0o700,
            )?);
            Ok(())
        };
        self.file_provider
            .coordinate_read(&self.directory, &mut read)
            .map_err(|error| classify_deadlock_error(&self.directory, "read", error))?;
        let current = current.ok_or_else(|| {
            anyhow!("File Provider did not provide cloud inbox enumeration revalidation")
        })?;
        Ok(current != expected)
    }

    fn prune_terminal_receipts_page(&self, scan: &mut InboxRecoveryScan) -> Result<()> {
        let mut names_read = 0usize;
        let mut bytes_read = 0usize;

        loop {
            let names_left = CLOUD_INBOX_SCAN_MAX_NAMES.saturating_sub(names_read);
            let page = self.peek_recovery_cursor(scan, names_left)?;
            names_read = names_read.saturating_add(page.names_read);
            let Some(entry) = page.entry else {
                if page.exhausted {
                    scan.cursor = None;
                }
                if page.exhausted || names_read >= CLOUD_INBOX_SCAN_MAX_NAMES {
                    break;
                }
                if page.names_read == 0 {
                    break;
                }
                continue;
            };

            match entry {
                BoundDirectoryCursorEntry::Unreadable { name, error } => {
                    bail!(
                        "cloud inbox directory entry is unreadable and was retained: {}: {error}",
                        name.to_string_lossy()
                    );
                }
                BoundDirectoryCursorEntry::Present { name, identity } => {
                    let Some(seq) = cloud_request_sequence_from_name(&name)? else {
                        self.root.consume_bound_directory_cursor_entry(
                            scan.cursor
                                .as_mut()
                                .ok_or_else(|| anyhow!("cloud inbox recovery cursor is missing"))?,
                        );
                        continue;
                    };
                    if !identity.is_file() {
                        bail!(
                            "cloud inbox request path is not a regular file: {}",
                            name.to_string_lossy()
                        );
                    }
                    let path = self.record_path(seq);
                    let remaining_bytes = CLOUD_INBOX_SCAN_MAX_BYTES.saturating_sub(bytes_read);
                    match self.read_recovery_record(&path, identity, remaining_bytes)? {
                        InboxRecordRead::Missing => {
                            bail!(
                                "cloud inbox request disappeared during recovery: {}",
                                path.display()
                            );
                        }
                        InboxRecordRead::Deferred => break,
                        InboxRecordRead::Present {
                            record,
                            entry,
                            identity,
                            file,
                            bytes,
                        } => {
                            let content_read_budget = bytes.len().saturating_add(1);
                            bytes_read = bytes_read.saturating_add(content_read_budget);
                            if record.request.seq != seq {
                                bail!(
                                    "cloud inbox request sequence does not match its filename: {}",
                                    path.display()
                                );
                            }

                            let expired = self.inbox_record_retention_expired(&record)?;
                            if expired
                                && content_read_budget
                                    <= CLOUD_INBOX_SCAN_MAX_BYTES.saturating_sub(bytes_read)
                                && self.remove_expired_record(
                                    seq, &entry, identity, &file, &bytes, &record,
                                )?
                            {
                                // Removal revalidates the serialized envelope after quarantine.
                                bytes_read = bytes_read.saturating_add(content_read_budget);
                                self.forget_indexed_record(seq)?;
                                self.root.consume_bound_directory_cursor_entry(
                                    scan.cursor.as_mut().ok_or_else(|| {
                                        anyhow!("cloud inbox recovery cursor is missing")
                                    })?,
                                );
                                continue;
                            } else if expired
                                && content_read_budget
                                    <= CLOUD_INBOX_SCAN_MAX_BYTES.saturating_sub(bytes_read)
                            {
                                // Even a declined cleanup performs a second bounded content read.
                                bytes_read = bytes_read.saturating_add(content_read_budget);
                            }

                            self.index_record(&record)?;
                            self.root.consume_bound_directory_cursor_entry(
                                scan.cursor.as_mut().ok_or_else(|| {
                                    anyhow!("cloud inbox recovery cursor is missing")
                                })?,
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn peek_recovery_cursor(
        &self,
        scan: &mut InboxRecoveryScan,
        max_names: usize,
    ) -> Result<crate::safe_fs::BoundDirectoryCursorPage> {
        let mut page = None;
        let mut read = |coordinated_path: &Path| -> Result<()> {
            let coordinated = coordinated_path_under_root(&self.root, coordinated_path)?;
            if coordinated != self.directory {
                bail!("File Provider changed the cloud inbox directory path");
            }
            self.validate_directory()?;
            let directory = self.root.bind_entry(&self.directory, false)?;
            if self.root.bound_entry_identity(&directory)? != Some(self.directory_identity) {
                bail!("cloud inbox directory was replaced during recovery");
            }
            if scan.cursor.is_none() {
                if !scan.indexing_complete && !scan.initial_complete {
                    scan.directory_entry_set_stamp =
                        Some(self.root.bound_directory_entry_set_stamp(
                            &directory,
                            self.directory_identity,
                            0o700,
                        )?);
                }
                scan.cursor = Some(self.root.start_bound_directory_cursor(
                    &directory,
                    self.directory_identity,
                    0o700,
                )?);
            }
            let cursor = scan
                .cursor
                .as_mut()
                .ok_or_else(|| anyhow!("cloud inbox recovery cursor was not initialized"))?;
            page = Some(
                self.root
                    .peek_bound_directory_cursor(cursor, max_names, 0o700)?,
            );
            Ok(())
        };
        self.file_provider
            .coordinate_read(&self.directory, &mut read)
            .map_err(|error| classify_deadlock_error(&self.directory, "read", error))?;
        page.ok_or_else(|| anyhow!("File Provider did not provide cloud inbox recovery access"))
    }

    fn index_record(&self, record: &InboxRecord) -> Result<()> {
        let mut pending = self
            .pending_sequences
            .lock()
            .map_err(|_| anyhow!("cloud inbox pending index lock was poisoned"))?;
        let mut state_index = self
            .state_index
            .lock()
            .map_err(|_| anyhow!("cloud inbox state index lock was poisoned"))?;
        set_pending_sequence(&mut pending, record.request.seq, record.status);
        state_index.record(record);
        Ok(())
    }

    fn forget_indexed_record(&self, seq: u64) -> Result<()> {
        self.pending_sequences
            .lock()
            .map_err(|_| anyhow!("cloud inbox pending index lock was poisoned"))?
            .remove(&seq);
        self.state_index
            .lock()
            .map_err(|_| anyhow!("cloud inbox state index lock was poisoned"))?
            .forget(seq);
        Ok(())
    }

    fn inbox_record_retention_expired(&self, record: &InboxRecord) -> Result<bool> {
        if inbox_terminal_record_expired(record, now_seconds()) {
            return Ok(true);
        }
        if !inbox_abandoned_preview_expired(record, now_seconds()) {
            return Ok(false);
        }
        let Some(preview_id) = inbox_preview_id(record) else {
            return Ok(true);
        };
        let index = self
            .state_index
            .lock()
            .map_err(|_| anyhow!("cloud inbox state index lock was poisoned"))?;
        Ok(!index.has_active_confirmation_before_kind(
            preview_id,
            &record.request.kind,
            inbox_preview_expiry_seconds(record),
        ))
    }

    fn require_initial_recovery_complete_before_write(&self) -> Result<()> {
        let initial_complete = self
            .recovery_scan
            .lock()
            .map_err(|_| anyhow!("cloud inbox recovery cursor lock was poisoned"))?
            .initial_complete;
        if initial_complete {
            return Ok(());
        }
        if self.recover_interrupted_requests()? {
            return Ok(());
        }
        Err(CloudInboxRecoveryIncomplete.into())
    }

    fn reset_incomplete_recovery(&self, scan: &mut InboxRecoveryScan) -> Result<()> {
        scan.cursor = None;
        scan.initial_complete = false;
        scan.indexing_complete = false;
        scan.processing_recovery.clear();
        scan.directory_entry_set_stamp = None;
        self.pending_sequences
            .lock()
            .map_err(|_| anyhow!("cloud inbox pending index lock was poisoned"))?
            .clear();
        *self
            .state_index
            .lock()
            .map_err(|_| anyhow!("cloud inbox state index lock was poisoned"))? =
            InboxStateIndex::default();
        Ok(())
    }

    fn remove_expired_record(
        &self,
        seq: u64,
        entry: &BoundEntry,
        identity: EntryIdentity,
        file: &BoundFile,
        bytes: &[u8],
        record: &InboxRecord,
    ) -> Result<bool> {
        if !self.inbox_record_retention_expired(record)? {
            return Ok(false);
        }
        let path = self.record_path(seq);
        let mut removed_result = None;
        let mut remove = |coordinated_path: &Path| -> Result<()> {
            let coordinated = coordinated_path_under_root(&self.root, coordinated_path)?;
            if coordinated != path {
                bail!("File Provider changed a cloud inbox cleanup path");
            }
            if !self.inbox_record_retention_expired(record)? {
                removed_result = Some(false);
                return Ok(());
            }
            // Preserve object identity, exact envelope bytes, and private access policy through
            // quarantine. Modification times are intentionally not treated as content changes.
            self.validate_directory()?;
            file.validate_private_single_link(0o600)
                .context("cloud inbox record access policy changed before cleanup")?;
            let mut no_longer_eligible = false;
            let removed =
                self.root
                    .remove_bound_file_if_identity_with_validation(entry, identity, || {
                        self.validate_directory()?;
                        file.validate_private_single_link(0o600)
                            .context("cloud inbox record access policy changed during cleanup")?;
                        if usize::try_from(file.byte_len()?)? != bytes.len() {
                            bail!("cloud inbox record length changed before cleanup");
                        }
                        let current_bytes = file.read_limited(bytes.len())?;
                        if current_bytes != bytes
                            || usize::try_from(file.byte_len()?)? != bytes.len()
                        {
                            bail!("cloud inbox record content changed before cleanup");
                        }
                        let current: InboxRecord = serde_json::from_slice(&current_bytes)
                            .context("cloud inbox record became unreadable before cleanup")?;
                        if current.schema_version != INBOX_RECORD_VERSION
                            || current.request.seq != seq
                            || !self.inbox_record_retention_expired(&current)?
                        {
                            no_longer_eligible = true;
                            bail!("cloud inbox record is no longer eligible for cleanup");
                        }
                        current.request.validate()?;
                        file.validate_private_single_link(0o600)
                            .context("cloud inbox record access policy changed before unlink")?;
                        self.validate_directory()
                    });
            match removed {
                Ok(()) => removed_result = Some(true),
                Err(_) if no_longer_eligible => removed_result = Some(false),
                Err(error) => {
                    return Err(error.context("failed to safely prune expired cloud inbox record"));
                }
            }
            Ok(())
        };
        self.file_provider
            .coordinate_write(&path, &mut remove)
            .map_err(|error| classify_deadlock_error(&path, "write", error))?;
        removed_result
            .ok_or_else(|| anyhow!("File Provider did not provide cloud inbox cleanup access"))
    }
}

impl PreviewCleanupGate for CloudInbox {
    fn remove_preview_if_unconfirmed(
        &self,
        preview_id: &str,
        remove: &mut dyn FnMut() -> Result<bool>,
    ) -> Result<PreviewCleanupOutcome> {
        let _guard = self
            .operation_lock
            .lock()
            .map_err(|_| anyhow!("cloud inbox lock was poisoned"))?;
        self.validate_directory()?;
        if self.retired_namespace_may_exist().unwrap_or(true) {
            return Ok(PreviewCleanupOutcome::Deferred);
        }
        if !self.recover_interrupted_requests()? {
            return Ok(PreviewCleanupOutcome::Deferred);
        }
        let active = self
            .state_index
            .lock()
            .map_err(|_| anyhow!("cloud inbox state index lock was poisoned"))?
            .has_preexpiry_confirmation_for_preview(preview_id);
        match active {
            Some(true) => Ok(PreviewCleanupOutcome::Retained),
            None => Ok(PreviewCleanupOutcome::Deferred),
            Some(false) => Ok(if remove()? {
                PreviewCleanupOutcome::Removed
            } else {
                PreviewCleanupOutcome::NotRemoved
            }),
        }
    }
}

fn create_owner_lock(
    requested_directory: &Path,
    directory: &Path,
    root: &RootedFs,
    file_provider: &Arc<dyn QueueFileProvider>,
) -> Result<Arc<CloudInboxOwnerLock>> {
    let lock_path = directory.join(CLOUD_INBOX_OWNER_LOCK);
    let mut lock_file = None;
    let mut access = |coordinated_path: &Path| -> Result<()> {
        let coordinated = coordinated_path_under_root(root, coordinated_path)?;
        if coordinated != *directory && coordinated != lock_path {
            bail!("File Provider changed the cloud inbox lock path");
        }
        if root.open_bound_file(&lock_path)?.is_none() {
            match root.create_new_bound_file(&lock_path, &[], 0o600) {
                Ok(_) => {}
                Err(error) if root.entry_exists(&lock_path).unwrap_or(false) => {
                    let _ = error;
                }
                Err(error) => return Err(error),
            }
        }
        let file = root
            .open_bound_file(&lock_path)?
            .ok_or_else(|| anyhow!("cloud inbox owner lock disappeared"))?;
        file.validate_private_single_link(0o600)?;
        if !file.try_lock_exclusive()? {
            bail!("cloud inbox already has a live local owner");
        }
        lock_file = Some(Arc::new(CloudInboxOwnerLock {
            file,
            owner_process_id: std::process::id(),
        }));
        Ok(())
    };
    file_provider
        .coordinate_write(requested_directory, &mut access)
        .map_err(|error| classify_deadlock_error(requested_directory, "write", error))?;
    lock_file.ok_or_else(|| anyhow!("File Provider did not provide cloud inbox lock access"))
}

fn coordinate_download_root(
    file_provider: &dyn QueueFileProvider,
    path: &Path,
) -> Result<RootedFs> {
    let mut bound_root = None;
    let mut read = |coordinated_path: &Path| -> Result<()> {
        let root = RootedFs::new(path)?;
        let accessor = coordinated_path_under_root(&root, coordinated_path)?;
        if accessor != root.logical_root_path() {
            bail!("coordinated download directory is outside its configured root");
        }
        root.list_root_directory()?;
        bound_root = Some(root);
        Ok(())
    };
    file_provider
        .coordinate_read(path, &mut read)
        .map_err(|error| classify_deadlock_error(path, "read", error))?;
    bound_root.ok_or_else(|| anyhow!("File Provider did not provide a coordinated download root"))
}

fn coordinate_private_directory_creation(
    file_provider: &dyn QueueFileProvider,
    root: &RootedFs,
    path: &Path,
) -> Result<()> {
    let mut created = false;
    let mut create = |coordinated_path: &Path| -> Result<()> {
        let accessor = coordinated_path_under_root(root, coordinated_path)?;
        if accessor == root.logical_root_path() || accessor != path {
            bail!("coordinated cloud inbox path is outside its configured download root");
        }
        let _ = root.create_dir(&accessor, 0o700)?;
        let entry = root.bind_entry(&accessor, false)?;
        let identity = root
            .bound_entry_identity(&entry)?
            .ok_or_else(|| anyhow!("coordinated cloud inbox directory disappeared"))?;
        root.validate_private_bound_directory(&entry, identity, 0o700)?;
        created = true;
        Ok(())
    };
    file_provider
        .coordinate_write(path, &mut create)
        .map_err(|error| classify_deadlock_error(path, "write", error))?;
    if !created {
        bail!("File Provider did not provide cloud inbox directory access");
    }
    Ok(())
}

fn coordinate_bound_directory(
    file_provider: &dyn QueueFileProvider,
    root: &RootedFs,
    path: &Path,
) -> Result<(PathBuf, EntryIdentity)> {
    let mut result = None;
    let mut read = |coordinated_path: &Path| -> Result<()> {
        let accessor = coordinated_path_under_root(root, coordinated_path)?;
        if accessor != path {
            bail!("coordinated cloud inbox directory path changed");
        }
        let entry = root.bind_entry(&accessor, false)?;
        let identity = root
            .bound_entry_identity(&entry)?
            .ok_or_else(|| anyhow!("coordinated cloud inbox directory disappeared"))?;
        root.validate_private_bound_directory(&entry, identity, 0o700)?;
        result = Some((accessor, identity));
        Ok(())
    };
    file_provider
        .coordinate_read(path, &mut read)
        .map_err(|error| classify_deadlock_error(path, "read", error))?;
    result.ok_or_else(|| anyhow!("File Provider did not provide cloud inbox directory access"))
}

fn coordinated_path_under_root(root: &RootedFs, coordinated_path: &Path) -> Result<PathBuf> {
    // File Provider can return canonical /private/... URLs for roots configured through the
    // verified /var, /tmp, or /etc aliases. Normalize only those fixed system aliases; arbitrary
    // path components remain subject to the rooted-prefix and normal-component checks below.
    let coordinated_path = crate::media_libraries::normalize_trusted_macos_alias(coordinated_path)?;
    let logical_root =
        crate::media_libraries::normalize_trusted_macos_alias(root.logical_root_path())?;
    let canonical_root = crate::media_libraries::normalize_trusted_macos_alias(root.root_path())?;
    let relative = coordinated_path
        .strip_prefix(&logical_root)
        .or_else(|_| coordinated_path.strip_prefix(&canonical_root))
        .with_context(|| {
            format!(
                "coordinated File Provider URL is outside the configured download root: {}",
                coordinated_path.display()
            )
        })?;
    let mut normalized = PathBuf::new();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            bail!("coordinated File Provider URL contains an invalid path component");
        };
        normalized.push(name);
    }
    Ok(root.logical_root_path().join(normalized))
}

fn temporary_sibling(path: &Path) -> PathBuf {
    let count = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    path.with_file_name(format!(".cloud-tmp-{stamp:x}-{count:x}"))
}

pub fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

pub async fn next_websocket_request(socket: &mut CloudWebSocket) -> Result<Option<CloudRequest>> {
    loop {
        match socket.next().await {
            Some(Ok(WsMessage::Text(text))) => {
                let request: CloudRequest = serde_json::from_str(text.as_str())
                    .context("invalid JSON frame from local cloud WebSocket")?;
                request.validate()?;
                return Ok(Some(request));
            }
            Some(Ok(WsMessage::Binary(bytes))) => {
                let request: CloudRequest = serde_json::from_slice(&bytes)
                    .context("invalid binary JSON frame from local cloud WebSocket")?;
                request.validate()?;
                return Ok(Some(request));
            }
            Some(Ok(WsMessage::Ping(payload))) => {
                socket
                    .send(WsMessage::Pong(payload))
                    .await
                    .context("failed to answer local cloud WebSocket ping")?;
            }
            Some(Ok(WsMessage::Pong(_))) => {}
            Some(Ok(WsMessage::Close(_))) | None => return Ok(None),
            Some(Ok(WsMessage::Frame(_))) => {}
            Some(Err(error)) => return Err(anyhow!(error).context("local cloud WebSocket failed")),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::mpsc;
    use std::thread;
    use std::time::SystemTime;

    use serde_json::json;

    use super::*;

    fn temp_root(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be available")
            .as_nanos();
        std::env::temp_dir().join(format!("telegram-video-downloader-cloud-{label}-{nanos}"))
    }

    fn request(seq: u64, text: &str) -> CloudRequest {
        CloudRequest {
            seq,
            kind: "telegram".to_string(),
            payload: json!({"update_id": 12, "message": {"text": text}}),
            created_at: 100,
            extra: serde_json::Map::new(),
        }
    }

    fn legacy_import_request(seq: u64, text: &str) -> CloudRequest {
        CloudRequest {
            seq,
            kind: "legacy_import".to_string(),
            payload: json!({"text": text}),
            created_at: 100,
            extra: serde_json::Map::new(),
        }
    }

    fn legacy_import_record(seq: u64, status: InboxStatus, updated_at: u64) -> InboxRecord {
        InboxRecord {
            schema_version: INBOX_RECORD_VERSION,
            request: legacy_import_request(seq, "legacy mappings"),
            status,
            attempts: 1,
            received_at: None,
            updated_at,
            next_attempt_at: 0,
            error: None,
            result: Some(json!({"hints": [], "candidates": []})),
        }
    }

    fn finish_initial_recovery(inbox: &CloudInbox) {
        for _ in 0..16 {
            if inbox
                .recover_interrupted_requests()
                .expect("bounded startup recovery should progress")
            {
                return;
            }
        }
        panic!("bounded startup recovery should complete within the test fixture");
    }

    fn preview_inbox_record(seq: u64, preview_id: &str, updated_at: u64) -> InboxRecord {
        let mut cloud_request = request(seq, "preview");
        cloud_request.kind = "file_preview".to_string();
        cloud_request.payload = json!({});
        cloud_request.created_at = now_millis();
        InboxRecord {
            schema_version: INBOX_RECORD_VERSION,
            request: cloud_request,
            status: InboxStatus::AwaitingConfirmation,
            attempts: 1,
            received_at: Some(updated_at),
            updated_at,
            next_attempt_at: 0,
            error: None,
            result: Some(json!({
                "preview_id": preview_id,
                "revision": "revision-1",
                "preview": {"id": preview_id, "revision": "revision-1"}
            })),
        }
    }

    fn confirmation_inbox_record(
        seq: u64,
        preview_id: &str,
        status: InboxStatus,
        worker_created_at_millis: u64,
        received_at_seconds: Option<u64>,
    ) -> InboxRecord {
        let mut cloud_request = request(seq, "confirm");
        cloud_request.kind = "file_confirm".to_string();
        cloud_request.payload = json!({"preview_id": preview_id, "revision": "revision-1"});
        cloud_request.created_at = worker_created_at_millis;
        InboxRecord {
            schema_version: INBOX_RECORD_VERSION,
            request: cloud_request,
            status,
            attempts: 1,
            received_at: received_at_seconds,
            updated_at: now_seconds(),
            next_attempt_at: 0,
            error: None,
            result: None,
        }
    }

    fn migration_preview_inbox_record(seq: u64, preview_id: &str, updated_at: u64) -> InboxRecord {
        let mut cloud_request = request(seq, "migration preview");
        cloud_request.kind = "library_migration_preview".to_string();
        cloud_request.payload = json!({});
        InboxRecord {
            schema_version: INBOX_RECORD_VERSION,
            request: cloud_request,
            status: InboxStatus::AwaitingConfirmation,
            attempts: 1,
            received_at: Some(updated_at),
            updated_at,
            next_attempt_at: 0,
            error: None,
            result: Some(json!({
                "id": preview_id,
                "revision": "migration-revision-1",
                "items": [{"item_id": "video-1", "source_path": "old/video.mp4", "target_path": "new/video.mp4"}]
            })),
        }
    }

    fn persist_awaiting_preview(inbox: &CloudInbox, preview: InboxRecord) {
        let seq = preview.request.seq;
        inbox
            .persist(preview.request.clone())
            .expect("preview request should persist");
        inbox
            .update(seq, |stored| {
                stored.status = InboxStatus::AwaitingConfirmation;
                stored.result = preview.result.clone();
                stored.error = None;
            })
            .expect("preview should become awaiting confirmation");
    }

    fn migration_confirmation_inbox_record(
        seq: u64,
        preview_id: &str,
        status: InboxStatus,
        received_at_seconds: Option<u64>,
    ) -> InboxRecord {
        let mut cloud_request = request(seq, "migration confirm");
        cloud_request.kind = "library_migration_confirm".to_string();
        cloud_request.payload =
            json!({"preview_id": preview_id, "revision": "migration-revision-1"});
        InboxRecord {
            schema_version: INBOX_RECORD_VERSION,
            request: cloud_request,
            status,
            attempts: 1,
            received_at: received_at_seconds,
            updated_at: now_seconds(),
            next_attempt_at: 0,
            error: None,
            result: None,
        }
    }

    #[test]
    fn bounded_state_index_counts_omitted_previews_without_selecting_them() {
        let mut index = InboxStateIndex::default();
        for seq in 1..=120 {
            let mut preview = request(seq, "preview");
            preview.kind = "file_preview".to_string();
            index.record(&InboxRecord {
                schema_version: INBOX_RECORD_VERSION,
                request: preview,
                status: InboxStatus::AwaitingConfirmation,
                attempts: 0,
                received_at: Some(1),
                updated_at: 1,
                next_attempt_at: 0,
                error: None,
                result: None,
            });
        }
        for seq in 121..=220 {
            let mut operation = request(seq, "scan");
            operation.kind = "library_scan".to_string();
            index.record(&InboxRecord {
                schema_version: INBOX_RECORD_VERSION,
                request: operation,
                status: InboxStatus::Completed,
                attempts: 1,
                received_at: Some(2),
                updated_at: 2,
                next_attempt_at: 0,
                error: None,
                result: None,
            });
        }

        let (sequences, omitted) = index.sequences_bounded(10);
        assert_eq!(omitted, 110);
        assert!(!sequences.contains(&1));
        assert!(sequences.contains(&111));
        assert!(sequences.contains(&120));
        assert!(sequences.contains(&121));
        assert!(sequences.contains(&220));
    }

    #[test]
    fn recovery_only_accepts_canonical_request_filenames() {
        let path = PathBuf::from(format!(
            "{CLOUD_REQUEST_PREFIX}{:020}{CLOUD_REQUEST_SUFFIX}",
            42
        ));
        assert_eq!(
            cloud_request_sequence_from_name(path.file_name().unwrap())
                .expect("generated request path should parse"),
            Some(42)
        );
        assert_eq!(
            cloud_request_sequence_from_name(OsStr::new(CLOUD_STATE_VERSION_FILE))
                .expect("state version control file should be ignored"),
            None
        );
        assert_eq!(
            cloud_request_sequence_from_name(OsStr::new(CLOUD_INBOX_OWNER_LOCK))
                .expect("owner lock should be ignored"),
            None
        );
        for malformed in [
            "request-1.json",
            "request-00000000000000000000.json",
            "request-00000000000000000001.txt",
            "request-9007199254740992.json",
        ] {
            assert!(
                cloud_request_sequence_from_name(OsStr::new(malformed)).is_err(),
                "malformed generated-looking name should be retained and rejected: {malformed}"
            );
        }
    }

    #[test]
    fn recovery_retains_and_distinguishes_malformed_or_unknown_request_files() {
        let root = temp_root("inbox-unknown-record");
        fs::create_dir_all(&root).expect("root should create");
        let inbox = CloudInbox::open(&root).expect("inbox should open");
        drop(inbox);

        let directory = root.join(CLOUD_INBOX_DIRECTORY);
        let malformed = directory.join("request-1.json");
        let unknown = directory.join(format!(
            "{CLOUD_REQUEST_PREFIX}{:020}{CLOUD_REQUEST_SUFFIX}",
            900
        ));
        let rooted = RootedFs::new(&root).expect("root should bind");
        rooted
            .create_new_bound_file(&malformed, b"{}", 0o600)
            .expect("malformed fixture should be private");
        let unknown_record = InboxRecord {
            schema_version: INBOX_RECORD_VERSION + 1,
            request: request(900, "unknown schema"),
            status: InboxStatus::Pending,
            attempts: 0,
            received_at: None,
            updated_at: now_seconds(),
            next_attempt_at: 0,
            error: None,
            result: None,
        };
        rooted
            .create_new_bound_file(
                &unknown,
                &serde_json::to_vec(&unknown_record).expect("unknown record should encode"),
                0o600,
            )
            .expect("unknown schema fixture should be private");

        let malformed_error = CloudInbox::open(&root)
            .err()
            .expect("malformed request filename should stop recovery");
        assert!(format!("{malformed_error:#}").contains("invalid sequence"));
        assert!(malformed.exists(), "malformed file must remain untouched");
        assert!(
            unknown.exists(),
            "unreached unknown file must remain untouched"
        );

        fs::remove_file(&malformed).expect("test should remove malformed fixture");
        let schema_error = CloudInbox::open(&root)
            .err()
            .expect("unknown request schema should stop recovery");
        assert!(format!("{schema_error:#}").contains("unsupported cloud inbox record version"));
        assert!(
            unknown.exists(),
            "unknown schema file must remain untouched"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn inbox_persists_full_request_before_duplicate_ack_and_reloads_pending() {
        let root = temp_root("durable-inbox");
        fs::create_dir_all(&root).expect("root should create");
        let inbox = CloudInbox::open(&root).expect("inbox should open");
        let first = request(7, "/help");
        assert!(
            inbox
                .persist(first.clone())
                .expect("request should persist")
        );
        assert!(
            !inbox
                .persist(first.clone())
                .expect("exact duplicate should deduplicate")
        );
        assert_eq!(
            inbox.pending().expect("pending should load")[0].request,
            first
        );
        drop(inbox);
        let reloaded = CloudInbox::open(&root).expect("inbox should reopen");
        assert_eq!(reloaded.pending().expect("pending should reload").len(), 1);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn inbox_owner_lock_releases_on_last_clone_even_with_a_duplicate_descriptor() {
        let root = temp_root("owner-lock-last-clone");
        fs::create_dir_all(&root).expect("root should create");
        let inbox = CloudInbox::open(&root).expect("inbox should open");
        let inbox_clone = inbox.clone();
        let duplicate_fd = inbox
            ._owner_lock
            .file
            .duplicate_std_file()
            .expect("owner lock descriptor should duplicate");
        let inherited_guard_copy = CloudInboxOwnerLock {
            file: inbox._owner_lock.file.clone(),
            owner_process_id: std::process::id().wrapping_add(1),
        };
        drop(inherited_guard_copy);

        let error = CloudInbox::open(&root)
            .err()
            .expect("dropping a foreign-process guard copy must not unlock the owner");
        assert!(format!("{error:#}").contains("already has a live local owner"));

        drop(inbox);
        let error = CloudInbox::open(&root)
            .err()
            .expect("a live inbox clone must continue to hold ownership");
        assert!(format!("{error:#}").contains("already has a live local owner"));

        drop(inbox_clone);
        let reopened = CloudInbox::open(&root)
            .expect("last local owner should unlock even while a duplicate fd remains open");
        drop(reopened);
        drop(duplicate_fd);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn inbox_rejects_reused_sequence_with_changed_envelope_and_sorts_out_of_order() {
        let root = temp_root("sequence-order");
        fs::create_dir_all(&root).expect("root should create");
        let inbox = CloudInbox::open(&root).expect("inbox should open");
        inbox
            .persist(request(9, "/help"))
            .expect("seq 9 should persist");
        inbox
            .persist(request(3, "/help"))
            .expect("seq 3 should persist");
        assert_eq!(
            inbox
                .pending()
                .expect("pending should load")
                .iter()
                .map(|r| r.request.seq)
                .collect::<Vec<_>>(),
            vec![3, 9]
        );
        let err = inbox
            .persist(request(9, "/queue"))
            .expect_err("changed envelope must fail");
        assert!(format!("{err:#}").contains("different envelope"));
        drop(inbox);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn state_version_is_persisted_and_increases_after_restart() {
        let root = temp_root("state-version");
        fs::create_dir_all(&root).expect("root should create");
        let inbox = CloudInbox::open(&root).expect("inbox should open");
        assert_eq!(
            inbox
                .next_state_version()
                .expect("first version should persist"),
            1
        );
        assert_eq!(
            inbox
                .next_state_version()
                .expect("next version should persist"),
            2
        );
        drop(inbox);
        let reopened = CloudInbox::open(&root).expect("inbox should reopen");
        assert_eq!(
            reopened
                .next_state_version()
                .expect("version should persist across restart"),
            3
        );
        drop(reopened);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn state_version_advances_after_remote_or_local_version_and_survives_restart() {
        let root = temp_root("state-version-conflict");
        fs::create_dir_all(&root).expect("root should create");
        let inbox = CloudInbox::open(&root).expect("inbox should open");

        assert_eq!(
            inbox
                .next_state_version_after(40)
                .expect("missing local version should advance past the remote version"),
            41
        );
        assert_eq!(
            inbox
                .next_state_version_after(5)
                .expect("higher local version should win"),
            42
        );
        assert_eq!(
            inbox
                .next_state_version_after(80)
                .expect("higher remote version should win"),
            81
        );
        drop(inbox);

        let reopened = CloudInbox::open(&root).expect("inbox should reopen");
        assert_eq!(
            reopened
                .next_state_version_after(80)
                .expect("persisted local version should survive restart"),
            82
        );
        drop(reopened);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn state_version_exhaustion_is_typed_and_does_not_write_out_of_range_values() {
        let root = temp_root("state-version-exhausted");
        fs::create_dir_all(&root).expect("root should create");
        let inbox = CloudInbox::open(&root).expect("inbox should open");

        let remote_error = inbox
            .next_state_version_after(MAX_CLOUD_STATE_VERSION)
            .expect_err("the maximum JavaScript-safe value cannot be incremented");
        assert!(is_state_version_exhausted(&remote_error));
        assert_eq!(
            inbox
                .next_state_version()
                .expect("failed remote advancement should not write a version"),
            1
        );

        let state_path = inbox.directory.join(CLOUD_STATE_VERSION_FILE);
        inbox
            .write_state_version(&state_path, MAX_CLOUD_STATE_VERSION)
            .expect("maximum safe local version should persist");
        let local_error = inbox
            .next_state_version_after(1)
            .expect_err("a maximum local value cannot be incremented");
        assert!(is_state_version_exhausted(&local_error));
        assert_eq!(
            inbox
                .read_state_version(&state_path)
                .expect("maximum safe value should remain readable"),
            Some(MAX_CLOUD_STATE_VERSION)
        );

        let invalid_error = inbox
            .next_state_version_after(MAX_CLOUD_STATE_VERSION + 1)
            .expect_err("an unsafe observed value should be rejected");
        assert!(!is_state_version_exhausted(&invalid_error));
        drop(inbox);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn retryable_inbox_requests_respect_persisted_next_attempt_time() {
        let root = temp_root("retry-backoff");
        fs::create_dir_all(&root).expect("root should create");
        let inbox = CloudInbox::open(&root).expect("inbox should open");
        inbox
            .persist(request(4, "/queue"))
            .expect("request should persist");
        let next_attempt = now_seconds().saturating_add(60);
        inbox
            .update(4, |record| {
                record.status = InboxStatus::Retryable;
                record.next_attempt_at = next_attempt;
            })
            .expect("retry time should persist");
        assert!(inbox.pending().expect("pending should load").is_empty());
        inbox
            .update(4, |record| record.next_attempt_at = 0)
            .expect("retry should be made due");
        assert_eq!(inbox.pending().expect("due retry should load").len(), 1);
        drop(inbox);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn terminal_receipts_stay_deduplicated_without_entering_pending_index() {
        let root = temp_root("terminal-dedup");
        fs::create_dir_all(&root).expect("root should create");
        let inbox = CloudInbox::open(&root).expect("inbox should open");
        let lower = request(2, "/help");
        let higher = request(9, "/queue");
        assert!(
            inbox
                .persist(lower.clone())
                .expect("lower request should persist")
        );
        assert!(
            inbox
                .persist(higher.clone())
                .expect("higher request should persist")
        );
        inbox
            .persist_dispatch_result(9, InboxStatus::Completed, None, None)
            .expect("terminal receipt should persist");
        assert_eq!(
            inbox
                .pending()
                .expect("pending should retain lower unseen sequence")
                .iter()
                .map(|record| record.request.seq)
                .collect::<Vec<_>>(),
            vec![2]
        );
        assert!(
            !inbox
                .persist(higher)
                .expect("terminal duplicate should remain deduplicated")
        );
        assert_eq!(
            inbox
                .pending()
                .expect("pending should remain indexed")
                .len(),
            1
        );
        drop(inbox);

        let reopened = CloudInbox::open(&root).expect("inbox should reopen");
        assert_eq!(reopened.pending().expect("pending should recover").len(), 1);
        assert!(
            !reopened
                .persist(request(9, "/queue"))
                .expect("terminal duplicate should deduplicate after restart")
        );
        drop(reopened);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn expired_terminal_receipts_are_pruned_incrementally_and_active_records_recover() {
        let root = temp_root("inbox-terminal-retention");
        fs::create_dir_all(&root).expect("root should create");
        let inbox = CloudInbox::open(&root).expect("inbox should open");
        let old = now_seconds().saturating_sub(
            CLOUD_INBOX_TERMINAL_REPLAY_RETENTION
                .as_secs()
                .saturating_add(1),
        );

        let write_fixture = |seq: u64, status: InboxStatus, kind: &str, updated_at: u64| {
            let mut cloud_request = request(seq, "fixture");
            cloud_request.kind = kind.to_string();
            let record = InboxRecord {
                schema_version: INBOX_RECORD_VERSION,
                request: cloud_request,
                status,
                attempts: 0,
                received_at: Some(updated_at),
                updated_at,
                next_attempt_at: 0,
                error: None,
                result: None,
            };
            inbox.write_record_path(&inbox.record_path(seq), &record)
        };

        for seq in 1..=120 {
            let terminal_status = match seq % 5 {
                0 => InboxStatus::Failed,
                1 => InboxStatus::Completed,
                2 => InboxStatus::Uncertain,
                3 => InboxStatus::Stale,
                _ => InboxStatus::Cancelled,
            };
            write_fixture(seq, terminal_status, "completed", old)
                .expect("expired terminal fixture should persist with its original age");
            match seq {
                24 => write_fixture(121, InboxStatus::Pending, "library_scan", old)
                    .expect("pending fixture should persist"),
                48 => write_fixture(122, InboxStatus::Retryable, "library_scan", old)
                    .expect("retryable fixture should persist"),
                72 => write_fixture(
                    123,
                    InboxStatus::AwaitingConfirmation,
                    "file_preview",
                    now_seconds(),
                )
                .expect("awaiting preview fixture should persist"),
                96 => write_fixture(124, InboxStatus::Processing, "library_scan", old)
                    .expect("processing fixture should persist"),
                _ => {}
            }
        }
        let mut recent_terminal = request(125, "recent terminal");
        recent_terminal.kind = "library_scan".to_string();
        inbox
            .persist(recent_terminal)
            .expect("recent terminal fixture should persist");
        inbox
            .update(125, |record| record.status = InboxStatus::Completed)
            .expect("recent terminal fixture should be terminal");
        assert_eq!(
            inbox
                .next_state_version()
                .expect("control state should persist"),
            1
        );
        drop(inbox);

        let reopened = CloudInbox::open(&root).expect("inbox should reopen");
        assert!(
            !reopened
                .recovery_scan
                .lock()
                .expect("recovery scan lock should be available")
                .initial_complete,
            "large history must not be fully scanned in one open call"
        );
        assert!(
            reopened
                .pending()
                .expect("incomplete recovery should be safe")
                .is_empty(),
            "pending dispatch must wait for the complete initial scan"
        );
        assert!(
            reopened.state_records_bounded(10).is_err(),
            "partial indexes must not be published as complete history"
        );

        let incoming = request(126, "arrived during startup recovery");
        let incomplete = reopened
            .persist(incoming.clone())
            .expect_err("incomplete startup recovery must defer new durable writes");
        assert!(
            incomplete.is::<CloudInboxRecoveryIncomplete>(),
            "bounded recovery should be distinguishable from an access or format error"
        );
        assert!(
            reopened
                .record(126)
                .expect("deferred request lookup should succeed")
                .is_none(),
            "a deferred request must not be written or acknowledged before full recovery"
        );

        let mut externally_materialized = InboxRecord {
            schema_version: INBOX_RECORD_VERSION,
            request: request(127, "materialized during startup recovery"),
            status: InboxStatus::Pending,
            attempts: 0,
            received_at: Some(now_seconds()),
            updated_at: now_seconds(),
            next_attempt_at: 0,
            error: None,
            result: None,
        };
        externally_materialized.request.kind = "library_scan".to_string();
        reopened
            .write_record_path(&reopened.record_path(127), &externally_materialized)
            .expect("external child-entry churn should be representable in the fixture");
        assert!(
            reopened.state_records_bounded(10).is_err(),
            "directory churn during enumeration must not publish a partial index"
        );

        let mut deferred_attempts = 0;
        let mut persisted = false;
        for _ in 0..32 {
            match reopened.persist(incoming.clone()) {
                Ok(inserted) => {
                    assert!(inserted, "the deferred request should be written once");
                    persisted = true;
                    break;
                }
                Err(error) if error.is::<CloudInboxRecoveryIncomplete>() => {
                    deferred_attempts += 1;
                    assert!(
                        reopened
                            .record(126)
                            .expect("deferred request lookup should succeed")
                            .is_none(),
                        "partial startup recovery must not persist a new request"
                    );
                    assert!(
                        !reopened
                            .recovery_scan
                            .lock()
                            .expect("recovery scan lock should be available")
                            .initial_complete,
                        "a deferred write must not claim recovery completed"
                    );
                }
                Err(error) => panic!("recovery should not fail for the valid inbox: {error:#}"),
            }
        }
        assert!(
            persisted,
            "bounded recovery should eventually admit the request"
        );
        assert!(
            deferred_attempts >= 2,
            "repeated arrivals should advance a shared recovery cursor instead of restarting it"
        );
        assert!(
            reopened
                .recovery_scan
                .lock()
                .expect("recovery scan lock should be available")
                .initial_complete,
            "the first write must wait until the complete durable index is recovered"
        );

        for seq in [1, 120] {
            let record = reopened
                .record(seq)
                .expect("expired fixture should be readable before cleanup")
                .expect("expired fixture should exist before cleanup");
            assert_eq!(record.updated_at, old);
            assert!(
                inbox_terminal_record_expired(&record, now_seconds()),
                "terminal fixture {seq} should be expired before cleanup starts"
            );
        }

        let pending = reopened
            .pending()
            .expect("recovery should make bounded progress through history");
        assert_eq!(
            pending
                .iter()
                .map(|record| record.request.seq)
                .collect::<Vec<_>>(),
            vec![121, 122, 124, 126, 127],
            "pending, retryable, and recovered processing records must remain dispatchable"
        );
        assert!(
            reopened
                .recovery_scan
                .lock()
                .expect("recovery scan lock should be available")
                .initial_complete
        );

        for _ in 0..16 {
            let pending = reopened
                .pending()
                .expect("terminal cleanup should make bounded progress");
            assert_eq!(
                pending
                    .iter()
                    .map(|record| record.request.seq)
                    .collect::<Vec<_>>(),
                vec![121, 122, 124, 126, 127],
                "terminal cleanup must preserve active requests"
            );
        }
        for seq in 1..=120 {
            assert!(
                reopened
                    .record(seq)
                    .expect("record read should succeed")
                    .is_none(),
                "expired terminal record {seq} should be removed from disk"
            );
        }
        assert_eq!(
            reopened
                .record(123)
                .expect("awaiting preview should be readable")
                .expect("awaiting preview should be retained")
                .status,
            InboxStatus::AwaitingConfirmation
        );
        assert_eq!(
            reopened
                .record(124)
                .expect("recovered processing record should be readable")
                .expect("recovered processing record should be retained")
                .status,
            InboxStatus::Pending
        );
        assert_eq!(
            reopened
                .record(125)
                .expect("recent terminal record should be readable")
                .expect("recent terminal record should be retained")
                .status,
            InboxStatus::Completed
        );
        assert_eq!(
            reopened
                .record(127)
                .expect("materialized request should be readable")
                .expect("materialized request should be indexed")
                .request
                .seq,
            127
        );
        let (state_records, omitted) = reopened
            .state_records_bounded(10)
            .expect("complete state history should be published");
        assert_eq!(omitted, 0);
        assert!(state_records.iter().all(|record| record.request.seq > 120));
        assert!(state_records.iter().any(|record| record.request.seq == 123));
        assert!(state_records.iter().any(|record| record.request.seq == 125));
        assert_eq!(
            reopened
                .next_state_version()
                .expect("state version control file must remain available"),
            2
        );
        drop(reopened);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn expired_abandoned_previews_are_removed_but_live_confirms_protect_their_previews() {
        let root = temp_root("inbox-abandoned-preview-retention");
        fs::create_dir_all(&root).expect("root should create");
        let inbox = CloudInbox::open(&root).expect("inbox should open");
        let now = now_seconds();
        let age = CLOUD_INBOX_ABANDONED_PREVIEW_RETENTION
            .as_secs()
            .saturating_add(1);
        let old = now.saturating_sub(age);
        let expires_at_seconds =
            old.saturating_add(CLOUD_INBOX_ABANDONED_PREVIEW_RETENTION.as_secs());
        let worker_expiry_millis = expires_at_seconds.saturating_mul(1_000);

        for (seq, preview_id) in [
            (1, "pending-preview"),
            (3, "processing-preview"),
            (5, "retryable-preview"),
            (7, "abandoned-preview"),
        ] {
            let preview = preview_inbox_record(seq, preview_id, old);
            inbox
                .write_record_path(&inbox.record_path(seq), &preview)
                .expect("preview fixture should persist");
        }
        for (seq, preview_id, status) in [
            (2, "pending-preview", InboxStatus::Pending),
            (4, "processing-preview", InboxStatus::Processing),
            (6, "retryable-preview", InboxStatus::Retryable),
        ] {
            let confirmation = confirmation_inbox_record(
                seq,
                preview_id,
                status,
                worker_expiry_millis - 1,
                Some(expires_at_seconds - 1),
            );
            inbox
                .write_record_path(&inbox.record_path(seq), &confirmation)
                .expect("confirmation fixture should persist");
        }
        drop(inbox);

        let reopened = CloudInbox::open(&root).expect("inbox should reopen and index records");
        let interrupted_confirmation = reopened
            .record(4)
            .expect("processing confirmation should be readable before recovery")
            .expect("processing confirmation should still be on disk before recovery");
        assert_eq!(
            interrupted_confirmation.status,
            InboxStatus::Processing,
            "startup must see the persisted Processing state before recovering it"
        );
        let processing_preview = reopened
            .record(3)
            .expect("processing preview should be readable before recovery")
            .expect("processing preview should remain on disk before recovery");
        assert!(
            inbox_abandoned_preview_expired(&processing_preview, now_seconds()),
            "the Processing-linked preview fixture must be expired before recovery"
        );
        assert!(
            reopened
                .state_index
                .lock()
                .expect("cloud inbox state index lock should be available")
                .has_active_confirmation_before_kind(
                    "processing-preview",
                    "file_preview",
                    inbox_preview_expiry_seconds(&processing_preview),
                ),
            "the persisted Processing confirmation must be present in the active index"
        );
        assert!(
            !reopened
                .inbox_record_retention_expired(&processing_preview)
                .expect("Processing confirmation should protect its expired preview"),
            "a persisted Processing confirmation must protect the preview receipt"
        );
        let durable_processing_confirmation = reopened
            .record(4)
            .expect("Processing confirmation should be readable")
            .expect("Processing confirmation should remain durable");
        assert_eq!(
            durable_processing_confirmation.received_at,
            Some(expires_at_seconds - 1),
            "recovery must not replace the immutable local receive time"
        );
        for _ in 0..8 {
            let _ = reopened
                .pending()
                .expect("bounded startup recovery should progress");
            if reopened
                .recovery_scan
                .lock()
                .expect("recovery scan lock should be available")
                .initial_complete
            {
                break;
            }
        }
        assert!(
            reopened
                .recovery_scan
                .lock()
                .expect("recovery scan lock should be available")
                .initial_complete,
            "all preview and confirmation records must be indexed before lookup"
        );
        for (preview_id, confirmation_seq) in [
            ("pending-preview", 2),
            ("processing-preview", 4),
            ("retryable-preview", 6),
        ] {
            let record = reopened
                .awaiting_confirmation_record(preview_id, confirmation_seq)
                .expect("indexed preview lookup should succeed")
                .expect("pre-expiry confirmation should retain its preview");
            assert_eq!(record.status, InboxStatus::AwaitingConfirmation);
        }
        assert!(
            reopened
                .record(7)
                .expect("expired preview receipt should be readable")
                .is_none(),
            "an abandoned preview inbox file should be removed at the manifest retention boundary"
        );

        let (projected_records, _) = reopened
            .state_records_bounded(10)
            .expect("expired previews should be projected stale");
        for preview_id in ["pending-preview", "processing-preview", "retryable-preview"] {
            assert_eq!(
                projected_records
                    .iter()
                    .find(|record| {
                        record.request.kind == "file_preview"
                            && inbox_preview_id(record) == Some(preview_id)
                    })
                    .expect("protected preview should remain in operation history")
                    .status,
                InboxStatus::Stale,
                "an expired preview must not remain confirmable in snapshots"
            );
        }
        for (seq, expected_status) in [
            (2, InboxStatus::Pending),
            (4, InboxStatus::Pending),
            (6, InboxStatus::Retryable),
        ] {
            assert_eq!(
                reopened
                    .record(seq)
                    .expect("active confirmation should remain readable")
                    .expect("active confirmation should be retained")
                    .status,
                expected_status,
                "pending, processing, and retryable confirms must keep their durable receipts"
            );
        }

        reopened
            .update(6, |record| record.status = InboxStatus::Completed)
            .expect("completed confirmation should leave the active index");
        assert!(
            reopened
                .awaiting_confirmation_record("retryable-preview", 6)
                .expect("lookup should reject a terminal confirmation")
                .is_none(),
            "terminal confirmations must no longer resolve an active preview"
        );
        assert!(
            reopened
                .record(5)
                .expect("preview lookup should succeed")
                .is_none()
        );
        for seq in [1, 2, 3, 4] {
            assert!(
                reopened
                    .record(seq)
                    .expect("active request should read")
                    .is_some(),
                "pending or processing confirmation {seq} must keep its preview receipt"
            );
        }
        drop(reopened);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn confirmation_expiry_uses_local_receipt_time_under_worker_clock_skew() {
        let root = temp_root("inbox-confirmation-clock-domain");
        fs::create_dir_all(&root).expect("root should create");
        let inbox = CloudInbox::open(&root).expect("inbox should open");
        let now = now_seconds();
        let retention = CLOUD_INBOX_ABANDONED_PREVIEW_RETENTION.as_secs();
        let preview_updated_at = now.saturating_sub(retention.saturating_add(10));
        let preview_expiry = inbox_preview_expiry_seconds(&preview_inbox_record(
            1,
            "worker-ahead-preview",
            preview_updated_at,
        ));

        for (seq, preview_id) in [
            (1, "worker-ahead-preview"),
            (3, "worker-behind-preview"),
            (5, "legacy-unknown-preview"),
            (7, "boundary-preview"),
        ] {
            inbox
                .write_record_path(
                    &inbox.record_path(seq),
                    &preview_inbox_record(seq, preview_id, preview_updated_at),
                )
                .expect("expired preview fixture should persist");
        }

        let worker_ahead_created_at = preview_expiry
            .saturating_add(8 * 24 * 60 * 60)
            .saturating_mul(1_000);
        let worker_ahead = confirmation_inbox_record(
            2,
            "worker-ahead-preview",
            InboxStatus::Pending,
            worker_ahead_created_at,
            Some(preview_expiry - 1),
        );
        inbox
            .write_record_path(&inbox.record_path(2), &worker_ahead)
            .expect("pre-expiry confirmation fixture should persist");

        let worker_behind_received_at = preview_expiry.saturating_add(1);
        let worker_behind_created_at = worker_behind_received_at
            .saturating_sub(8 * 24 * 60 * 60)
            .saturating_mul(1_000);
        let worker_behind = confirmation_inbox_record(
            4,
            "worker-behind-preview",
            InboxStatus::Retryable,
            worker_behind_created_at,
            Some(worker_behind_received_at),
        );
        inbox
            .write_record_path(&inbox.record_path(4), &worker_behind)
            .expect("post-expiry confirmation fixture should persist");

        let at_expiry = confirmation_inbox_record(
            8,
            "boundary-preview",
            InboxStatus::Pending,
            preview_expiry
                .saturating_sub(8 * 24 * 60 * 60)
                .saturating_mul(1_000),
            Some(preview_expiry),
        );
        inbox
            .write_record_path(&inbox.record_path(8), &at_expiry)
            .expect("expiry-boundary confirmation fixture should persist");

        // A legacy active confirmation has no immutable local receive time. Its updated_at may
        // have been refreshed during recovery, so it must remain unknown rather than be inferred
        // from that mutable field or the Worker clock.
        let mut legacy_confirmation = confirmation_inbox_record(
            6,
            "legacy-unknown-preview",
            InboxStatus::Processing,
            now_millis(),
            None,
        );
        legacy_confirmation.updated_at = now;
        inbox
            .write_record_path(&inbox.record_path(6), &legacy_confirmation)
            .expect("legacy confirmation fixture should persist");
        let legacy_path = inbox.record_path(6);
        let mut legacy_json: Value = serde_json::from_slice(
            &fs::read(&legacy_path).expect("legacy confirmation bytes should be readable"),
        )
        .expect("legacy confirmation should be JSON");
        legacy_json
            .as_object_mut()
            .expect("legacy confirmation should be an object")
            .remove("received_at");
        fs::write(
            &legacy_path,
            serde_json::to_vec(&legacy_json).expect("legacy confirmation should encode"),
        )
        .expect("fixture should omit the newly introduced receipt-time field");
        drop(inbox);

        let reopened = CloudInbox::open(&root).expect("inbox should reopen");
        finish_initial_recovery(&reopened);
        let ahead_confirmation = reopened
            .record(2)
            .expect("confirmation should be readable")
            .expect("confirmation should remain persisted");
        assert_eq!(ahead_confirmation.received_at, Some(preview_expiry - 1));
        assert!(
            ahead_confirmation
                .request
                .created_at
                .saturating_sub(preview_expiry.saturating_mul(1_000))
                > 7_u64 * 24 * 60 * 60 * 1_000
        );
        let ahead_preview = reopened
            .record(1)
            .expect("preview should be readable")
            .expect("pre-expiry confirmation should retain its preview");
        assert!(
            !reopened
                .inbox_record_retention_expired(&ahead_preview)
                .expect("active confirmation retention should be checked"),
            "an active pre-expiry confirmation must protect its expired preview"
        );

        let behind_confirmation = reopened
            .record(4)
            .expect("confirmation should be readable")
            .expect("confirmation should remain persisted");
        assert!(
            behind_confirmation
                .request
                .created_at
                .saturating_add(7_u64 * 24 * 60 * 60 * 1_000)
                < worker_behind_received_at.saturating_mul(1_000)
        );
        let behind_preview = reopened
            .record(3)
            .expect("preview should be readable")
            .expect("preview receipt should remain for retention evaluation");
        assert!(
            reopened
                .inbox_record_retention_expired(&behind_preview)
                .expect("expired preview cleanup should be evaluated"),
            "a post-expiry confirmation must not protect the preview receipt"
        );

        let boundary_preview = reopened
            .record(7)
            .expect("boundary preview should be readable")
            .expect("boundary preview receipt should remain for retention evaluation");
        assert!(
            reopened
                .inbox_record_retention_expired(&boundary_preview)
                .expect("expiry-boundary cleanup should be evaluated"),
            "a confirmation received exactly at expiry must not protect the preview"
        );

        let legacy_preview = reopened
            .record(5)
            .expect("legacy preview should be readable")
            .expect("unknown-time active confirmation should protect the preview");
        assert!(
            !reopened
                .inbox_record_retention_expired(&legacy_preview)
                .expect("legacy retention should be checked conservatively"),
            "an active confirmation with unknown receipt time must conservatively protect its preview"
        );

        assert!(
            reopened
                .awaiting_confirmation_record("worker-ahead-preview", 2)
                .expect("lookup should use the local receipt clock")
                .is_some(),
            "a confirmation received locally before expiry must survive a Worker clock over seven days ahead"
        );
        assert!(
            reopened
                .awaiting_confirmation_record("worker-behind-preview", 4)
                .expect("lookup should use the local receipt clock")
                .is_none(),
            "a confirmation received locally after expiry must be rejected despite a Worker clock over seven days behind"
        );
        assert!(
            reopened
                .awaiting_confirmation_record("boundary-preview", 8)
                .expect("expiry-boundary confirmation should be checked locally")
                .is_none(),
            "a confirmation received exactly at local expiry must be rejected"
        );

        let recovered_legacy_confirmation = reopened
            .record(6)
            .expect("legacy confirmation should be readable")
            .expect("legacy confirmation should remain recoverable");
        assert_eq!(recovered_legacy_confirmation.received_at, None);
        assert_eq!(recovered_legacy_confirmation.status, InboxStatus::Pending);
        assert!(
            reopened
                .awaiting_confirmation_record("legacy-unknown-preview", 6)
                .expect("unknown-time legacy receipt should remain recoverable")
                .is_some(),
            "recovery must not expire a legacy confirmation using its mutable updated_at"
        );

        drop(reopened);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn preview_cleanup_allows_terminal_receipt_without_active_confirmations() {
        let root = temp_root("preview-cleanup-terminal-receipt");
        fs::create_dir_all(&root).expect("root should create");
        let inbox = CloudInbox::open(&root).expect("inbox should open");
        let mut preview_request = request(1, "preview");
        preview_request.kind = "file_preview".to_string();
        preview_request.payload = json!({});
        inbox
            .persist(preview_request)
            .expect("preview request should persist");
        inbox
            .update(1, |record| {
                record.status = InboxStatus::AwaitingConfirmation;
                record.result = Some(json!({
                    "preview_id": "terminal-receipt-preview",
                    "revision": "revision-1",
                    "preview": {"id": "terminal-receipt-preview", "revision": "revision-1"}
                }));
            })
            .expect("preview should become confirmable");
        inbox
            .update(1, |record| record.status = InboxStatus::Stale)
            .expect("terminal preview should leave the awaiting index");

        let mut unlink_called = false;
        let mut unlink = || {
            unlink_called = true;
            Ok(true)
        };
        assert!(matches!(
            inbox
                .remove_preview_if_unconfirmed("terminal-receipt-preview", &mut unlink)
                .expect("terminal receipt with no active confirmations should be known inactive"),
            PreviewCleanupOutcome::Removed
        ));
        assert!(
            unlink_called,
            "expired private preview cleanup should proceed"
        );
        drop(inbox);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn preview_cleanup_defers_when_active_confirmation_has_no_preview_expiry() {
        let root = temp_root("preview-cleanup-missing-expiry");
        fs::create_dir_all(&root).expect("root should create");
        let inbox = CloudInbox::open(&root).expect("inbox should open");
        let mut preview_request = request(1, "preview");
        preview_request.kind = "file_preview".to_string();
        preview_request.payload = json!({});
        inbox
            .persist(preview_request)
            .expect("preview request should persist");
        inbox
            .update(1, |record| {
                record.status = InboxStatus::AwaitingConfirmation;
                record.result = Some(json!({
                    "preview_id": "missing-expiry-preview",
                    "revision": "revision-1",
                    "preview": {"id": "missing-expiry-preview", "revision": "revision-1"}
                }));
            })
            .expect("preview should become confirmable");

        let mut confirmation = request(2, "confirm");
        confirmation.kind = "file_confirm".to_string();
        confirmation.payload = json!({
            "preview_id": "missing-expiry-preview",
            "revision": "revision-1"
        });
        confirmation.created_at = now_millis();
        inbox
            .persist(confirmation)
            .expect("active pre-expiry confirmation should persist");
        inbox
            .update(1, |record| record.status = InboxStatus::Stale)
            .expect("terminal preview receipt should leave the expiry index");

        let mut unlink_called = false;
        let mut unlink = || {
            unlink_called = true;
            Ok(true)
        };
        assert!(matches!(
            inbox
                .remove_preview_if_unconfirmed("missing-expiry-preview", &mut unlink)
                .expect("missing expiry for an active confirmation should defer cleanup"),
            PreviewCleanupOutcome::Deferred
        ));
        assert!(
            !unlink_called,
            "an active confirmation with unknown expiry must retain its private preview"
        );
        drop(inbox);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn preview_cleanup_serializes_confirmation_persistence_through_unlink() {
        let root = temp_root("preview-cleanup-gate-race");
        fs::create_dir_all(&root).expect("root should create");
        let inbox = CloudInbox::open(&root).expect("inbox should open");
        let mut preview_request = request(1, "preview");
        preview_request.kind = "file_preview".to_string();
        preview_request.payload = json!({});
        inbox
            .persist(preview_request)
            .expect("preview request should persist");
        inbox
            .update(1, |record| {
                record.status = InboxStatus::AwaitingConfirmation;
                record.result = Some(json!({
                    "preview_id": "cleanup-race-preview",
                    "revision": "revision-1",
                    "preview": {"id": "cleanup-race-preview", "revision": "revision-1"}
                }));
            })
            .expect("preview should become confirmable");

        let (unlink_started_tx, unlink_started_rx) = mpsc::channel();
        let (release_unlink_tx, release_unlink_rx) = mpsc::channel();
        let cleanup_inbox = inbox.clone();
        let cleanup = thread::spawn(move || {
            let mut unlink = || {
                assert!(
                    cleanup_inbox.operation_lock.try_lock().is_err(),
                    "the inbox operation lock must remain held through unlink"
                );
                unlink_started_tx
                    .send(())
                    .expect("cleanup test should signal its final unlink window");
                release_unlink_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("cleanup test should release the final unlink window");
                assert!(
                    cleanup_inbox.operation_lock.try_lock().is_err(),
                    "confirmation persistence must remain excluded until unlink completes"
                );
                Ok(true)
            };
            cleanup_inbox.remove_preview_if_unconfirmed("cleanup-race-preview", &mut unlink)
        });
        unlink_started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("cleanup should reach its conditional unlink callback");

        let (persist_started_tx, persist_started_rx) = mpsc::channel();
        let (persist_done_tx, persist_done_rx) = mpsc::channel();
        let persist_inbox = inbox.clone();
        let persistence = thread::spawn(move || {
            let mut confirmation = request(2, "confirm");
            confirmation.kind = "file_confirm".to_string();
            confirmation.payload = json!({
                "preview_id": "cleanup-race-preview",
                "revision": "revision-1"
            });
            persist_started_tx
                .send(())
                .expect("persistence test should signal before writing");
            let result = persist_inbox.persist(confirmation);
            persist_done_tx
                .send(result.is_ok())
                .expect("persistence test should report completion");
        });
        persist_started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("confirmation persistence should start");
        assert!(
            persist_done_rx
                .recv_timeout(Duration::from_millis(50))
                .is_err(),
            "a new active confirmation must not persist between the absence check and unlink"
        );
        release_unlink_tx
            .send(())
            .expect("cleanup test should release unlink");
        assert!(matches!(
            cleanup.join().expect("cleanup worker should finish"),
            Ok(PreviewCleanupOutcome::Removed)
        ));
        assert!(
            persist_done_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("confirmation should persist after unlink")
        );
        persistence
            .join()
            .expect("persistence worker should finish");

        let mut retained = false;
        let mut should_not_unlink = || {
            retained = true;
            Ok(true)
        };
        assert!(matches!(
            inbox
                .remove_preview_if_unconfirmed("cleanup-race-preview", &mut should_not_unlink)
                .expect("active confirmation lookup should succeed"),
            PreviewCleanupOutcome::Retained
        ));
        assert!(
            !retained,
            "a persisted active confirmation must retain its preview"
        );
        drop(inbox);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn persisted_confirmation_uses_immutable_local_receipt_time() {
        let root = temp_root("inbox-local-confirmation-receipt");
        fs::create_dir_all(&root).expect("root should create");
        let inbox = CloudInbox::open(&root).expect("inbox should open");
        let mut preview_request = request(1, "preview");
        preview_request.kind = "file_preview".to_string();
        preview_request.payload = json!({});
        inbox
            .persist(preview_request)
            .expect("fresh preview fixture should persist");
        inbox
            .update(1, |record| {
                record.status = InboxStatus::AwaitingConfirmation;
                record.result = Some(json!({
                    "preview_id": "fresh-preview",
                    "revision": "revision-1",
                    "preview": {"id": "fresh-preview", "revision": "revision-1"}
                }));
            })
            .expect("preview should become confirmable");
        let mut confirmation_request = request(2, "confirm");
        confirmation_request.kind = "file_confirm".to_string();
        confirmation_request.payload =
            json!({"preview_id": "fresh-preview", "revision": "revision-1"});
        confirmation_request.created_at = now_millis();
        inbox
            .persist(confirmation_request)
            .expect("confirmation should persist");
        let original_received_at = inbox
            .record(2)
            .expect("confirmation receipt should be readable")
            .expect("confirmation receipt should exist")
            .received_at
            .expect("new receipts should record a local receive time");
        inbox
            .update(2, |record| record.status = InboxStatus::Processing)
            .expect("confirmation should enter processing");
        assert_eq!(
            inbox
                .record(2)
                .expect("processing receipt should be readable")
                .expect("processing receipt should exist")
                .received_at,
            Some(original_received_at),
            "status transitions must preserve the original local receive time"
        );
        drop(inbox);

        let reopened = CloudInbox::open(&root).expect("inbox should reopen");
        for _ in 0..4 {
            let _ = reopened
                .pending()
                .expect("bounded startup recovery should progress");
            if reopened
                .recovery_scan
                .lock()
                .expect("recovery scan lock should be available")
                .initial_complete
            {
                break;
            }
        }
        assert!(
            reopened
                .recovery_scan
                .lock()
                .expect("recovery scan lock should be available")
                .initial_complete,
            "the preview index must be complete before resolving confirmation"
        );
        let record = reopened
            .awaiting_confirmation_record("fresh-preview", 2)
            .expect("confirmation receipt should resolve the preview")
            .expect("a locally fresh confirmation must resolve the preview");
        assert_eq!(record.status, InboxStatus::AwaitingConfirmation);
        assert_eq!(
            reopened
                .record(2)
                .expect("recovered confirmation should be readable")
                .expect("recovered confirmation should exist")
                .received_at,
            Some(original_received_at),
            "startup recovery must preserve the original local receive time"
        );
        drop(reopened);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn migration_confirmation_resolves_kind_qualified_preview_identity() {
        let root = temp_root("inbox-migration-confirmation-lookup");
        fs::create_dir_all(&root).expect("root should create");
        let inbox = CloudInbox::open(&root).expect("inbox should open");
        let same_id = "shared-preview-id";

        let mut file_preview = request(1, "file preview");
        file_preview.kind = "file_preview".to_string();
        inbox
            .persist(file_preview)
            .expect("file preview should persist");
        inbox
            .update(1, |record| {
                record.status = InboxStatus::AwaitingConfirmation;
                record.result = Some(json!({
                    "preview_id": same_id,
                    "revision": "revision-1",
                    "preview": {"id": same_id, "revision": "revision-1"}
                }));
            })
            .expect("file preview should become confirmable");

        let migration_preview = migration_preview_inbox_record(2, same_id, now_seconds());
        inbox
            .persist(migration_preview.request.clone())
            .expect("migration preview should persist");
        inbox
            .update(2, |record| {
                record.status = InboxStatus::AwaitingConfirmation;
                record.result = migration_preview.result.clone();
            })
            .expect("migration preview should become confirmable");

        inbox
            .persist(
                migration_confirmation_inbox_record(
                    3,
                    same_id,
                    InboxStatus::Pending,
                    Some(now_seconds()),
                )
                .request,
            )
            .expect("migration confirmation should persist");
        inbox
            .persist(
                confirmation_inbox_record(
                    4,
                    same_id,
                    InboxStatus::Pending,
                    now_millis(),
                    Some(now_seconds()),
                )
                .request,
            )
            .expect("file confirmation should persist");
        finish_initial_recovery(&inbox);

        let migration = inbox
            .awaiting_confirmation_record(same_id, 3)
            .expect("migration lookup should succeed")
            .expect("migration confirmation should resolve its persisted preview");
        assert_eq!(migration.request.seq, 2);
        assert_eq!(migration.request.kind, "library_migration_preview");
        assert_eq!(inbox_preview_id(&migration), Some(same_id));

        let file = inbox
            .awaiting_confirmation_record(same_id, 4)
            .expect("file lookup should succeed")
            .expect("file confirmation should resolve its persisted preview");
        assert_eq!(file.request.seq, 1);
        assert_eq!(file.request.kind, "file_preview");

        drop(inbox);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn preview_cancel_exactly_cancels_without_changing_result_and_is_idempotent() {
        let root = temp_root("preview-cancel-idempotent");
        fs::create_dir_all(&root).expect("root should create");
        let inbox = CloudInbox::open(&root).expect("inbox should open");
        finish_initial_recovery(&inbox);
        let preview = preview_inbox_record(1, "preview-cancel-exact", now_seconds());
        let original_result = preview.result.clone().expect("preview result should exist");
        persist_awaiting_preview(&inbox, preview);

        assert!(
            inbox
                .cancel_awaiting_preview(
                    "file_preview",
                    1,
                    "preview-cancel-exact",
                    "wrong-revision"
                )
                .expect("wrong revision should be a safe rejection")
                .is_none()
        );
        assert!(
            inbox
                .cancel_awaiting_preview(
                    "library_migration_preview",
                    1,
                    "preview-cancel-exact",
                    "revision-1"
                )
                .expect("wrong kind should be a safe rejection")
                .is_none()
        );
        assert!(
            inbox
                .cancel_awaiting_preview("file_preview", 2, "preview-cancel-exact", "revision-1")
                .expect("wrong sequence should be a safe rejection")
                .is_none()
        );
        assert!(
            inbox
                .cancel_awaiting_preview("file_preview", 0, "preview-cancel-exact", "revision-1")
                .expect("zero sequence should be a safe rejection")
                .is_none()
        );
        assert!(
            inbox
                .cancel_awaiting_preview(
                    "file_preview",
                    MAX_JS_SAFE_INTEGER + 1,
                    "preview-cancel-exact",
                    "revision-1"
                )
                .expect("unsafe sequence should be a safe rejection")
                .is_none()
        );
        assert!(
            inbox
                .cancel_awaiting_preview(
                    "file_preview",
                    1,
                    &"x".repeat(MAX_PREVIEW_CANCEL_ID_BYTES + 1),
                    "revision-1"
                )
                .expect("oversized preview id should be a safe rejection")
                .is_none()
        );
        assert!(
            inbox
                .cancel_awaiting_preview(
                    "file_preview",
                    1,
                    "preview-cancel-exact",
                    &"x".repeat(MAX_PREVIEW_CANCEL_REVISION_BYTES + 1)
                )
                .expect("oversized revision should be a safe rejection")
                .is_none()
        );
        assert!(
            inbox
                .cancel_awaiting_preview("file_preview", 1, " preview-cancel-exact", "revision-1")
                .expect("untrimmed preview id should be a safe rejection")
                .is_none()
        );
        assert!(
            inbox
                .cancel_awaiting_preview("file_preview", 1, "preview-cancel-exact", "revision\n1")
                .expect("control characters should be a safe rejection")
                .is_none()
        );
        assert_eq!(
            inbox
                .record(1)
                .expect("preview should remain readable")
                .expect("preview should remain stored")
                .status,
            InboxStatus::AwaitingConfirmation
        );

        let cancelled = inbox
            .cancel_awaiting_preview("file_preview", 1, "preview-cancel-exact", "revision-1")
            .expect("exact preview cancellation should succeed")
            .expect("exact awaiting preview should be cancellable");
        assert_eq!(cancelled.status, InboxStatus::Cancelled);
        assert_eq!(cancelled.result, Some(original_result.clone()));
        assert_eq!(
            cancelled.error.as_deref(),
            Some("This preview was cancelled before confirmation.")
        );

        let repeated = inbox
            .cancel_awaiting_preview("file_preview", 1, "preview-cancel-exact", "revision-1")
            .expect("repeated cancellation should be safe")
            .expect("the exact already-cancelled target should remain idempotent");
        assert_eq!(repeated.status, InboxStatus::Cancelled);
        assert_eq!(repeated.result, Some(original_result.clone()));
        assert_eq!(
            inbox
                .state_index
                .lock()
                .expect("state index should be available")
                .awaiting_preview_sequence_for_kind("preview-cancel-exact", "file_preview"),
            None
        );

        drop(inbox);
        let reopened = CloudInbox::open(&root).expect("inbox should reopen");
        finish_initial_recovery(&reopened);
        let replayed = reopened
            .record(1)
            .expect("cancelled preview should remain readable after replay")
            .expect("cancelled preview should remain in history");
        assert_eq!(replayed.status, InboxStatus::Cancelled);
        assert_eq!(replayed.result, Some(original_result));
        drop(reopened);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn preview_cancel_blocks_on_accepted_confirmation_and_prevents_later_confirmation() {
        let root = temp_root("preview-cancel-confirm-race");
        fs::create_dir_all(&root).expect("root should create");
        let inbox = CloudInbox::open(&root).expect("inbox should open");
        finish_initial_recovery(&inbox);

        for (preview_seq, confirmation_seq, status) in [
            (1, 2, InboxStatus::Pending),
            (3, 4, InboxStatus::Processing),
            (5, 6, InboxStatus::Retryable),
        ] {
            let preview_id = format!("preview-confirm-active-{confirmation_seq}");
            persist_awaiting_preview(
                &inbox,
                preview_inbox_record(preview_seq, &preview_id, now_seconds()),
            );
            inbox
                .persist(
                    confirmation_inbox_record(
                        confirmation_seq,
                        &preview_id,
                        InboxStatus::Pending,
                        now_millis(),
                        Some(now_seconds()),
                    )
                    .request,
                )
                .expect("accepted confirmation should persist");
            if status != InboxStatus::Pending {
                inbox
                    .update(confirmation_seq, |record| record.status = status)
                    .expect("confirmation lifecycle status should persist");
            }

            assert!(
                inbox
                    .cancel_awaiting_preview("file_preview", preview_seq, &preview_id, "revision-1")
                    .expect("active confirmation should safely block cancellation")
                    .is_none()
            );
            assert_eq!(
                inbox
                    .record(preview_seq)
                    .expect("preview should remain readable")
                    .expect("preview should remain stored")
                    .status,
                InboxStatus::AwaitingConfirmation
            );
        }

        let preview_id = "preview-cancel-before-confirm";
        persist_awaiting_preview(&inbox, preview_inbox_record(7, preview_id, now_seconds()));
        assert!(
            inbox
                .cancel_awaiting_preview("file_preview", 7, preview_id, "revision-1")
                .expect("cancel-before-confirm should succeed")
                .is_some()
        );
        inbox
            .persist(
                confirmation_inbox_record(
                    8,
                    preview_id,
                    InboxStatus::Pending,
                    now_millis(),
                    Some(now_seconds()),
                )
                .request,
            )
            .expect("later confirmation should persist as a rejected action");
        assert!(
            inbox
                .awaiting_confirmation_record(preview_id, 8)
                .expect("later confirmation should be checked safely")
                .is_none()
        );
        assert_eq!(
            inbox
                .record(7)
                .expect("cancelled preview should remain readable")
                .expect("cancelled preview should remain stored")
                .status,
            InboxStatus::Cancelled
        );

        drop(inbox);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn preview_cancel_supports_migration_without_library_state_and_rejects_stale_targets() {
        let root = temp_root("preview-cancel-migration");
        fs::create_dir_all(&root).expect("root should create");
        let inbox = CloudInbox::open(&root).expect("inbox should open");
        finish_initial_recovery(&inbox);
        let migration = migration_preview_inbox_record(1, "migration-cancel-target", now_seconds());
        let original_result = migration
            .result
            .clone()
            .expect("migration result should exist");
        persist_awaiting_preview(&inbox, migration);

        assert!(
            inbox
                .cancel_awaiting_preview(
                    "file_preview",
                    1,
                    "migration-cancel-target",
                    "migration-revision-1"
                )
                .expect("wrong preview kind should be rejected")
                .is_none()
        );
        let cancelled = inbox
            .cancel_awaiting_preview(
                "library_migration_preview",
                1,
                "migration-cancel-target",
                "migration-revision-1",
            )
            .expect("migration cancellation should not require library access")
            .expect("exact migration preview should be cancellable");
        assert_eq!(cancelled.status, InboxStatus::Cancelled);
        assert_eq!(cancelled.result, Some(original_result));

        for (seq, status, preview_id) in [
            (2, InboxStatus::Stale, "stale-cancel-target"),
            (3, InboxStatus::Processing, "running-cancel-target"),
            (4, InboxStatus::Completed, "confirmed-cancel-target"),
        ] {
            let mut preview = preview_inbox_record(seq, preview_id, now_seconds());
            preview.status = status;
            inbox
                .persist(preview.request.clone())
                .expect("terminal or running preview request should persist");
            inbox
                .update(seq, |record| {
                    record.status = status;
                    record.result = preview.result.clone();
                })
                .expect("terminal or running preview status should persist");
            assert!(
                inbox
                    .cancel_awaiting_preview("file_preview", seq, preview_id, "revision-1")
                    .expect("terminal or running target should be safely rejected")
                    .is_none()
            );
            assert_eq!(
                inbox
                    .record(seq)
                    .expect("terminal or running target should remain readable")
                    .expect("terminal or running target should remain stored")
                    .status,
                status
            );
        }

        drop(inbox);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn expired_preview_is_projected_stale_before_bounded_cleanup_reaches_its_file() {
        let root = temp_root("inbox-preview-expiry-projection");
        fs::create_dir_all(&root).expect("root should create");
        let inbox = CloudInbox::open(&root).expect("inbox should open");
        let old = now_seconds().saturating_sub(
            CLOUD_INBOX_ABANDONED_PREVIEW_RETENTION
                .as_secs()
                .saturating_add(1),
        );
        for seq in 1..=100 {
            let preview_id = format!("expired-preview-{seq}");
            let record = preview_inbox_record(seq, &preview_id, old);
            inbox
                .write_record_path(&inbox.record_path(seq), &record)
                .expect("expired preview should persist");
        }
        drop(inbox);

        let reopened = CloudInbox::open(&root).expect("inbox should reopen");
        for _ in 0..8 {
            let _ = reopened
                .pending()
                .expect("bounded startup recovery should progress");
            if reopened
                .recovery_scan
                .lock()
                .expect("recovery scan lock should be available")
                .initial_complete
            {
                break;
            }
        }
        assert!(
            reopened
                .recovery_scan
                .lock()
                .expect("recovery scan lock should be available")
                .initial_complete,
            "the complete inbox index is required before publishing state"
        );
        let (records, _) = reopened
            .state_records_bounded(10)
            .expect("complete state should project expired previews safely");
        let projected = records
            .iter()
            .find(|record| record.request.kind == "file_preview")
            .expect("a preview beyond the bounded cleanup page should remain in history");
        assert_eq!(projected.status, InboxStatus::Stale);
        assert!(
            projected
                .error
                .as_deref()
                .is_some_and(|error| error.contains("expired"))
        );
        let projected_seq = projected.request.seq;
        assert_eq!(
            reopened
                .record(projected_seq)
                .expect("disk receipt should still be readable before its cleanup page")
                .expect("a preview beyond the current cleanup page should remain on disk")
                .status,
            InboxStatus::AwaitingConfirmation
        );
        drop(reopened);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn management_state_index_is_bounded_and_keeps_awaiting_previews() {
        let mut index = InboxStateIndex::default();
        let mut preview = InboxRecord {
            schema_version: INBOX_RECORD_VERSION,
            request: CloudRequest {
                seq: 1,
                kind: "file_preview".to_string(),
                payload: json!({}),
                created_at: 1,
                extra: serde_json::Map::new(),
            },
            status: InboxStatus::AwaitingConfirmation,
            attempts: 1,
            received_at: Some(1),
            updated_at: 1,
            next_attempt_at: 0,
            error: None,
            result: Some(json!({"preview": {"id": "preview-a"}})),
        };
        index.record(&preview);
        for seq in 2..=105 {
            let mut management = request(seq, "/queue");
            management.kind = "library_scan".to_string();
            let record = InboxRecord {
                schema_version: INBOX_RECORD_VERSION,
                request: management,
                status: InboxStatus::Completed,
                attempts: 1,
                received_at: Some(seq),
                updated_at: seq,
                next_attempt_at: 0,
                error: None,
                result: None,
            };
            index.record(&record);
        }
        let (sequences, omitted_previews) = index.sequences_bounded(1);
        assert_eq!(
            omitted_previews, 0,
            "the selected preview should not be omitted"
        );
        assert_eq!(sequences.len(), MAX_MANAGEMENT_STATE_RECORDS + 1);
        assert!(
            sequences.contains(&1),
            "awaiting preview should remain visible"
        );
        assert_eq!(
            sequences.iter().nth(1),
            Some(&6),
            "only the latest 100 management items remain"
        );
        assert_eq!(sequences.iter().next_back(), Some(&105));

        let (without_previews, omitted_previews) = index.sequences_bounded(0);
        assert_eq!(
            omitted_previews, 1,
            "the active preview omission must be counted"
        );
        assert_eq!(without_previews.len(), MAX_MANAGEMENT_STATE_RECORDS);
        assert!(!without_previews.contains(&1));

        preview.status = InboxStatus::Stale;
        index.record(&preview);
        let (sequences, omitted_previews) = index.sequences_bounded(1);
        assert_eq!(omitted_previews, 0);
        assert_eq!(sequences.len(), MAX_MANAGEMENT_STATE_RECORDS);
        assert!(!sequences.contains(&1));
    }

    #[test]
    fn completed_legacy_import_index_is_bounded_and_tracks_status() {
        let mut index = InboxStateIndex::default();
        let newest_seq = MAX_COMPLETED_LEGACY_IMPORT_RECORDS as u64 + 1;
        for seq in 1..=newest_seq {
            index.record(&legacy_import_record(
                seq,
                InboxStatus::Completed,
                now_seconds(),
            ));
        }
        assert_eq!(
            index.completed_legacy_imports.len(),
            MAX_COMPLETED_LEGACY_IMPORT_RECORDS
        );
        assert!(!index.completed_legacy_imports.contains(&1));
        assert!(index.completed_legacy_imports.contains(&newest_seq));

        index.record(&legacy_import_record(
            newest_seq,
            InboxStatus::Failed,
            now_seconds(),
        ));
        assert!(!index.completed_legacy_imports.contains(&newest_seq));
        assert_eq!(index.completed_legacy_imports.len(), 99);

        let pending_seq = newest_seq + 1;
        index.record(&legacy_import_record(
            pending_seq,
            InboxStatus::Pending,
            now_seconds(),
        ));
        assert!(!index.completed_legacy_imports.contains(&pending_seq));
        index.forget(2);
        assert!(!index.completed_legacy_imports.contains(&2));
    }

    #[test]
    fn completed_legacy_import_survives_management_eviction_and_reopen() {
        let root = temp_root("legacy-import-index-management-eviction");
        fs::create_dir_all(&root).expect("root should create");
        let inbox = CloudInbox::open(&root).expect("inbox should open");
        let result = json!({
            "hints": [{"title": "Episode 1", "requires_confirmation": true}],
            "candidates": [{"hint_index": 0, "item_id": "media-1"}]
        });
        inbox
            .persist(legacy_import_request(
                1,
                "Episode 1 https://youtu.be/PHH1wTDF-1M",
            ))
            .expect("legacy import should persist as pending");
        assert!(
            inbox
                .completed_legacy_import_records_bounded()
                .expect("pending imports should be ignored")
                .is_empty(),
            "only completed imports may authorize visible mappings"
        );
        inbox
            .persist_dispatch_result(1, InboxStatus::Completed, Some(result.clone()), None)
            .expect("legacy import result should persist");

        for seq in 2..=100 {
            let mut management = request(seq, "scan");
            management.kind = "library_scan".to_string();
            management.payload = json!({});
            inbox
                .persist(management)
                .expect("management receipt should persist");
        }
        let (before_preview, _) = inbox
            .state_records_bounded(0)
            .expect("the current management window should be readable");
        assert!(
            before_preview.iter().any(|record| record.request.seq == 1),
            "the source import should be visible as the oldest of 100 management receipts"
        );

        let mut preview = request(101, "preview");
        preview.kind = "file_preview".to_string();
        preview.payload = json!({"preview_id": "preview-101"});
        inbox.persist(preview).expect("new preview should persist");
        let (after_preview, _) = inbox
            .state_records_bounded(0)
            .expect("the updated management window should be readable");
        assert_eq!(after_preview.len(), MAX_MANAGEMENT_STATE_RECORDS);
        assert!(
            !after_preview.iter().any(|record| record.request.seq == 1),
            "the UI management history should remain capped at its newest 100 receipts"
        );
        let retained = inbox
            .completed_legacy_import_records_bounded()
            .expect("the separate import index should retain the visible mapping source");
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].request.seq, 1);
        assert_eq!(retained[0].result, Some(result.clone()));

        drop(inbox);
        let reopened = CloudInbox::open(&root).expect("inbox should reopen");
        finish_initial_recovery(&reopened);
        let retained_after_reopen = reopened
            .completed_legacy_import_records_bounded()
            .expect("recovery should rebuild the completed-import index");
        assert_eq!(retained_after_reopen.len(), 1);
        assert_eq!(retained_after_reopen[0].request.seq, 1);
        assert_eq!(retained_after_reopen[0].result, Some(result));
        reopened
            .persist_dispatch_result(
                1,
                InboxStatus::Failed,
                Some(json!({"error": "fixture failure"})),
                Some("fixture failure".to_string()),
            )
            .expect("failed import status should persist");
        assert!(
            reopened
                .completed_legacy_import_records_bounded()
                .expect("updated import index should be readable")
                .is_empty(),
            "an import that becomes failed must leave the completed-source index"
        );
        let (reopened_management, _) = reopened
            .state_records_bounded(0)
            .expect("reopened management history should remain bounded");
        assert_eq!(reopened_management.len(), MAX_MANAGEMENT_STATE_RECORDS);
        assert!(
            !reopened_management
                .iter()
                .any(|record| record.request.seq == 1),
            "the private source index must not expand the UI management history"
        );
        drop(reopened);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn expired_completed_legacy_import_is_not_returned_and_is_pruned() {
        let root = temp_root("legacy-import-index-expiry");
        fs::create_dir_all(&root).expect("root should create");
        let inbox = CloudInbox::open(&root).expect("inbox should open");
        inbox
            .persist(legacy_import_request(1, "legacy mappings"))
            .expect("legacy import should persist");
        inbox
            .persist_dispatch_result(
                1,
                InboxStatus::Completed,
                Some(json!({"hints": [], "candidates": []})),
                None,
            )
            .expect("legacy import should complete");

        let mut expired = inbox
            .record(1)
            .expect("legacy receipt should be readable")
            .expect("legacy receipt should exist");
        expired.updated_at =
            now_seconds().saturating_sub(CLOUD_INBOX_TERMINAL_REPLAY_RETENTION.as_secs());
        inbox
            .write_record_path(&inbox.record_path(1), &expired)
            .expect("expired receipt fixture should retain its original age");
        drop(inbox);

        let reopened = CloudInbox::open(&root).expect("inbox should reopen");
        finish_initial_recovery(&reopened);
        assert!(
            reopened
                .completed_legacy_import_records_bounded()
                .expect("expiry checks should succeed")
                .is_empty(),
            "completed import sources must use the existing seven-day retention"
        );
        assert!(
            reopened
                .record(1)
                .expect("expired record lookup should succeed")
                .is_none(),
            "the ordinary bounded terminal cleanup should prune the expired source"
        );
        drop(reopened);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn oversized_generated_result_becomes_a_compact_terminal_receipt() {
        let root = temp_root("oversized-result");
        fs::create_dir_all(&root).expect("root should create");
        let inbox = CloudInbox::open(&root).expect("inbox should open");
        inbox
            .persist(request(5, "/queue"))
            .expect("request should persist");
        let receipt = inbox
            .persist_dispatch_result(
                5,
                InboxStatus::Completed,
                Some(json!({"large": "x".repeat(MAX_REQUEST_BYTES + 128 * 1024)})),
                None,
            )
            .expect("oversized result should become a compact receipt");
        assert_eq!(receipt.status, InboxStatus::Failed);
        assert!(
            receipt
                .error
                .as_deref()
                .is_some_and(|error| error.contains("too large"))
        );
        assert_eq!(
            receipt
                .result
                .as_ref()
                .and_then(|value| value["error"].as_str()),
            Some("The local operation result exceeded the receipt size limit.")
        );
        assert!(
            serde_json::to_vec(&receipt)
                .expect("compact receipt should serialize")
                .len()
                <= MAX_REQUEST_BYTES + 64 * 1024
        );
        assert!(
            inbox
                .pending()
                .expect("terminal receipt is not pending")
                .is_empty()
        );
        drop(inbox);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn chunked_cloud_state_preserves_partitions_and_accounts_for_wire_bytes() {
        let library = json!({
            "revision": "library-1", "scanned_at": 42,
            "items": [{"title": "家庭教师\"\\\n🎬", "bytes": 1_234_567_890_u64}],
            "categories": ["Anime"], "warnings": [],
        });
        let operations = vec![json!({"result": {"title": "完成"}})];
        let settings = json!({"libraries": []});
        let state = CloudState {
            state_version: 123,
            revision: Some("state-1"),
            reported_at: 456,
            library: &library,
            previews: &[],
            operations: &operations,
            tasks: &[],
            settings: CloudSettings {
                download_dir: "/media",
                applied: Some(&settings),
            },
        };
        let prepared = state
            .chunked_snapshot(true)
            .expect("chunked state should encode");
        assert_eq!(state.encoded_wire_size().unwrap(), prepared.wire_size);
        let envelope: Value = serde_json::from_slice(&prepared.manifest).unwrap();
        assert_eq!(envelope["format"], "chunked-v1");
        assert_eq!(envelope["state_version"], 123);
        assert_eq!(envelope["reported_at"], 456);
        assert_eq!(envelope["library_revision"], "library-1");
        let chunk_map = prepared
            .chunks
            .iter()
            .map(|chunk| (chunk.sha256.as_str(), chunk.bytes.as_slice()))
            .collect::<BTreeMap<_, _>>();
        let decode_partition = |name: &str| {
            let mut bytes = Vec::new();
            for reference in envelope[name]["chunks"].as_array().unwrap() {
                let hash = reference["sha256"].as_str().unwrap();
                let chunk = chunk_map.get(hash).unwrap();
                assert_eq!(reference["bytes"], chunk.len());
                bytes.extend_from_slice(chunk);
            }
            assert_eq!(envelope[name]["bytes"], bytes.len());
            serde_json::from_slice::<Value>(&bytes).unwrap()
        };
        assert_eq!(decode_partition("library"), library);
        let contents = decode_partition("state");
        assert_eq!(contents["revision"], "state-1");
        assert_eq!(contents["operations"], json!(operations));
        assert_eq!(contents["settings"]["applied"], settings);
        for reserved in ["library", "state_version", "reported_at"] {
            assert!(contents.get(reserved).is_none());
        }
        let mut refreshed = state.clone();
        refreshed.reported_at += 1;
        let retry = refreshed.chunked_snapshot(true).unwrap();
        let retry_envelope: Value = serde_json::from_slice(&retry.manifest).unwrap();
        assert_eq!(retry_envelope["library"], envelope["library"]);
        assert_eq!(retry_envelope["state"], envelope["state"]);
        assert_eq!(retry.chunks.len(), prepared.chunks.len());
        assert_ne!(retry_envelope["reported_at"], envelope["reported_at"]);
    }

    #[test]
    fn chunked_cloud_state_omits_optional_revision() {
        let library = json!({"revision": "test", "scanned_at": 0, "items": [], "categories": [], "warnings": []});
        let state = CloudState {
            state_version: 1,
            revision: None,
            reported_at: 1,
            library: &library,
            previews: &[],
            operations: &[],
            tasks: &[],
            settings: CloudSettings {
                download_dir: "/media",
                applied: None,
            },
        };
        let prepared = state.chunked_snapshot(true).unwrap();
        let envelope: Value = serde_json::from_slice(&prepared.manifest).unwrap();
        let chunks = prepared
            .chunks
            .iter()
            .map(|chunk| (chunk.sha256.as_str(), chunk.bytes.as_slice()))
            .collect::<BTreeMap<_, _>>();
        let mut contents = Vec::new();
        for reference in envelope["state"]["chunks"].as_array().unwrap() {
            contents.extend_from_slice(chunks[reference["sha256"].as_str().unwrap()]);
        }
        let contents: Value = serde_json::from_slice(&contents).unwrap();
        assert!(contents.get("revision").is_none());
        assert_eq!(contents["settings"], json!({"download_dir": "/media"}));
    }

    #[test]
    fn cloud_state_chunks_respect_utf8_boundaries_and_fixed_byte_limit() {
        let source = format!(
            "{}🎬{}",
            "x".repeat(CLOUD_STATE_CHUNK_BYTES - 1),
            "y".repeat(17)
        );
        let mut unique = BTreeMap::new();
        let refs = chunk_cloud_partition(source.as_bytes(), &mut unique).unwrap();
        let mut rebuilt = Vec::new();
        for reference in refs {
            let sha256 = reference["sha256"].as_str().unwrap();
            let bytes = &unique[sha256];
            assert!(bytes.len() <= CLOUD_STATE_CHUNK_BYTES);
            std::str::from_utf8(bytes).expect("each chunk must remain valid UTF-8");
            assert_eq!(reference["bytes"], bytes.len());
            rebuilt.extend_from_slice(bytes);
        }
        assert_eq!(rebuilt, source.as_bytes());
    }

    #[test]
    fn cloud_state_complexity_admission_does_not_block_compaction_sizing() {
        let mut nested = Value::Null;
        for _ in 0..=MAX_CLOUD_STATE_JSON_DEPTH {
            nested = json!({"nested": nested});
        }
        let library = json!({
            "revision": "deep-library",
            "scanned_at": 1,
            "items": [],
            "categories": [],
            "warnings": [],
            "extra": nested,
        });
        let state = CloudState {
            state_version: 1,
            revision: None,
            reported_at: 1,
            library: &library,
            previews: &[],
            operations: &[],
            tasks: &[],
            settings: CloudSettings {
                download_dir: "/media",
                applied: None,
            },
        };

        let error = state
            .chunked_snapshot(true)
            .expect_err("deep state should fail upload admission");
        assert!(
            error
                .downcast_ref::<CloudStateComplexityExceeded>()
                .is_some()
        );
        assert!(
            state.encoded_wire_size().is_ok(),
            "the reporter still needs a byte estimate so compaction can run"
        );
    }

    #[test]
    fn cloud_state_post_errors_classify_transient_and_payload_rejections() {
        for status in [
            reqwest::StatusCode::UNAUTHORIZED,
            reqwest::StatusCode::FORBIDDEN,
            reqwest::StatusCode::CONFLICT,
            reqwest::StatusCode::REQUEST_TIMEOUT,
            reqwest::StatusCode::PRECONDITION_REQUIRED,
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        ] {
            assert!(CloudStatePostError::HttpStatus(status).is_retryable());
        }
        assert!(!CloudStatePostError::HttpStatus(reqwest::StatusCode::BAD_REQUEST).is_retryable());
        assert!(
            CloudStatePostError::HttpStatus(reqwest::StatusCode::PAYLOAD_TOO_LARGE)
                .is_payload_too_large()
        );
        assert!(CloudStatePostError::Transport(anyhow!("connection reset")).is_retryable());
        let conflict = CloudStatePostError::Conflict { stored_version: 42 };
        assert!(conflict.is_retryable());
        assert_eq!(conflict.stored_version_hint(), Some(42));
        assert_eq!(
            CloudStatePostError::HttpStatus(reqwest::StatusCode::CONFLICT).stored_version_hint(),
            None
        );
    }

    #[test]
    fn cloud_state_version_hint_requires_positive_canonical_safe_decimal() {
        let cases: &[(&[u8], Option<u64>)] = &[
            (b"1", Some(1)),
            (b"9007199254740991", Some(MAX_CLOUD_STATE_VERSION)),
            (b"", None),
            (b"0", None),
            (b"01", None),
            (b"+1", None),
            (b"-1", None),
            (b" 1", None),
            (b"1 ", None),
            (b"9007199254740992", None),
            (b"18446744073709551616", None),
            (b"1.0", None),
        ];
        for (header, expected) in cases {
            assert_eq!(parse_cloud_state_version_hint(header), *expected);
        }
    }

    #[test]
    fn cloud_requests_validate_safe_sequences_and_import_size() {
        let mut invalid = request(MAX_JS_SAFE_INTEGER + 1, "/help");
        assert!(invalid.validate().is_err());
        invalid.seq = 4;
        invalid.kind = "legacy_import".to_string();
        invalid.payload = json!({"text": "x".repeat(MAX_LEGACY_IMPORT_BYTES + 1)});
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn client_uses_header_auth_and_maps_secure_websocket_path() {
        let client = CloudClient::with_test_base_url("https://worker.example.test/")
            .expect("client should construct");
        assert_eq!(
            client.api_url("api/local/requests").unwrap().as_str(),
            "https://worker.example.test/api/local/requests"
        );
        let mut ws_url = client.api_url("api/local/ws").unwrap();
        ws_url.set_scheme("wss").unwrap();
        assert_eq!(ws_url.scheme(), "wss");
        let expected_authorization = format!("Bearer {TEST_CLOUD_SECRET}");
        assert_eq!(
            client.authorization.to_str().unwrap(),
            expected_authorization.as_str()
        );
    }
}
