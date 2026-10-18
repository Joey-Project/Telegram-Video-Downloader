use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::path::{Component, Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::config::CloudConfig;
use crate::file_provider::{
    QueueFileProvider, classify_deadlock_error, is_deadlock_error, platform_queue_file_provider,
};
use crate::safe_fs::{
    BoundDirectoryCursor, BoundDirectoryCursorEntry, BoundDirectoryEntrySetStamp, BoundEntry,
    BoundFile, EntryIdentity, RootedFs,
};
use anyhow::{Context, Result, anyhow, bail};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use reqwest::header::{AUTHORIZATION, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION as WS_AUTHORIZATION;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

const CLOUD_INBOX_DIRECTORY: &str = ".telegram-video-downloader-cloud";
const CLOUD_INBOX_OWNER_LOCK: &str = ".owner.lock";
const CLOUD_STATE_VERSION_FILE: &str = ".state-version.json";
const CLOUD_REQUEST_PREFIX: &str = "request-";
const CLOUD_REQUEST_SUFFIX: &str = ".json";
const INBOX_RECORD_VERSION: u32 = 1;
const MAX_JS_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
pub(crate) const MAX_CLOUD_STATE_VERSION: u64 = MAX_JS_SAFE_INTEGER;
const MAX_REQUEST_BYTES: usize = 1024 * 1024;
const MAX_LEGACY_IMPORT_BYTES: usize = 256 * 1024;
const MAX_MANAGEMENT_STATE_RECORDS: usize = 100;
pub(crate) const CLOUD_REQUEST_PAGE_SIZE: usize = 100;
// Keep terminal local receipts through the same replay window as D1 ACK tombstones.
const CLOUD_INBOX_TERMINAL_REPLAY_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);
// Each inbox API call makes bounded recovery/cleanup progress instead of draining history at once.
const CLOUD_INBOX_SCAN_MAX_NAMES: usize = 32;
const CLOUD_INBOX_SCAN_MAX_BYTES: usize = 8 * 1024 * 1024;
static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

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

pub(crate) fn is_state_version_exhausted(error: &anyhow::Error) -> bool {
    error.downcast_ref::<CloudStateVersionExhausted>().is_some()
}

#[cfg(test)]
// Catalog fixture: pool joey-private-v3, token ID bearer-a, role bearer, state active.
pub(crate) const TEST_CLOUD_SECRET: &str = "codex_synth_v1_bearer_a";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CloudRequest {
    pub seq: u64,
    pub kind: String,
    pub payload: Value,
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
    requests: Vec<CloudRequest>,
}

#[derive(Debug, Serialize)]
struct AckBody<'a> {
    seqs: &'a [u64],
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

#[derive(Debug, Clone, Serialize)]
pub struct CloudSettings<'a> {
    pub download_dir: &'a str,
}

#[derive(Clone)]
pub struct CloudClient {
    base_url: url::Url,
    client: Client,
    authorization: HeaderValue,
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
}

impl CloudStatePostError {
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Transport(_) | Self::Conflict { .. } => true,
            Self::HttpStatus(status) => {
                *status == reqwest::StatusCode::UNAUTHORIZED
                    || *status == reqwest::StatusCode::FORBIDDEN
                    || *status == reqwest::StatusCode::CONFLICT
                    || *status == reqwest::StatusCode::REQUEST_TIMEOUT
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
            Self::Transport(_) | Self::HttpStatus(_) => None,
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
        })
    }

    #[cfg(test)]
    pub(crate) fn with_test_base_url(base_url: &str) -> Result<Self> {
        Self::with_base_url(url::Url::parse(base_url)?, TEST_CLOUD_SECRET.to_string())
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

    pub async fn fetch_requests(&self) -> Result<Vec<CloudRequest>> {
        let page_size = CLOUD_REQUEST_PAGE_SIZE.to_string();
        let response = self
            .client
            .get(self.api_url("api/local/requests")?)
            .query(&[("limit", page_size.as_str())])
            .header(AUTHORIZATION, self.authorization.clone())
            .send()
            .await
            .context("failed to fetch local cloud requests")?
            .error_for_status()
            .context("local cloud request fetch returned HTTP error")?;
        let request_list = response
            .json::<RequestList>()
            .await
            .context("failed to decode local cloud request list")?;
        for request in &request_list.requests {
            request.validate()?;
        }
        Ok(request_list.requests)
    }

    pub async fn acknowledge(&self, seqs: &[u64]) -> Result<()> {
        if seqs.is_empty() {
            return Ok(());
        }
        if seqs
            .iter()
            .any(|seq| *seq == 0 || *seq > MAX_JS_SAFE_INTEGER)
        {
            bail!("cannot acknowledge a sequence outside JavaScript's safe integer range");
        }
        self.client
            .post(self.api_url("api/local/ack")?)
            .header(AUTHORIZATION, self.authorization.clone())
            .json(&AckBody { seqs })
            .send()
            .await
            .context("failed to acknowledge local cloud requests")?
            .error_for_status()
            .context("local cloud acknowledgement returned HTTP error")?;
        Ok(())
    }

    pub async fn post_state(
        &self,
        state: &CloudState<'_>,
    ) -> std::result::Result<(), CloudStatePostError> {
        let url = self
            .api_url("api/local/state")
            .map_err(CloudStatePostError::Transport)?;
        let response = self
            .client
            .post(url)
            .header(AUTHORIZATION, self.authorization.clone())
            .json(state)
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
        } else if status == reqwest::StatusCode::CONFLICT {
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
        let (socket, _) = timeout(Duration::from_secs(15), connect_async(request))
            .await
            .context("local cloud WebSocket handshake timed out")?
            .context("failed to connect to local cloud WebSocket")?;
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InboxRecord {
    pub schema_version: u32,
    pub request: CloudRequest,
    pub status: InboxStatus,
    pub attempts: u32,
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
    awaiting_previews: BTreeSet<u64>,
}

impl InboxStateIndex {
    fn record(&mut self, record: &InboxRecord) {
        if record.request.kind != "telegram" {
            self.recent_management.insert(record.request.seq);
            while self.recent_management.len() > MAX_MANAGEMENT_STATE_RECORDS {
                if let Some(oldest) = self.recent_management.first().copied() {
                    self.recent_management.remove(&oldest);
                }
            }
        }
        if record.request.kind == "file_preview" {
            if record.status == InboxStatus::AwaitingConfirmation {
                self.awaiting_previews.insert(record.request.seq);
            } else {
                self.awaiting_previews.remove(&record.request.seq);
            }
        }
    }

    fn forget(&mut self, seq: u64) {
        self.recent_management.remove(&seq);
        self.awaiting_previews.remove(&seq);
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
        InboxStatus::Completed | InboxStatus::Failed | InboxStatus::Uncertain | InboxStatus::Stale
    );
    terminal
        && record.updated_at <= now
        && now.saturating_sub(record.updated_at) >= CLOUD_INBOX_TERMINAL_REPLAY_RETENTION.as_secs()
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

#[derive(Clone)]
pub struct CloudInbox {
    root: RootedFs,
    file_provider: Arc<dyn QueueFileProvider>,
    directory: PathBuf,
    directory_identity: EntryIdentity,
    _owner_lock: BoundFile,
    operation_lock: Arc<Mutex<()>>,
    pending_sequences: Arc<Mutex<BTreeSet<u64>>>,
    state_index: Arc<Mutex<InboxStateIndex>>,
    recovery_scan: Arc<Mutex<InboxRecoveryScan>>,
}

impl CloudInbox {
    pub fn open(root_path: &Path) -> Result<Self> {
        Self::open_with_file_provider(root_path, platform_queue_file_provider())
    }

    pub(crate) fn open_with_file_provider(
        root_path: &Path,
        file_provider: Arc<dyn QueueFileProvider>,
    ) -> Result<Self> {
        let root = coordinate_download_root(file_provider.as_ref(), root_path)?;
        let directory = root.logical_root_path().join(CLOUD_INBOX_DIRECTORY);
        coordinate_private_directory_creation(file_provider.as_ref(), &root, &directory)?;
        let (directory, directory_identity) =
            coordinate_bound_directory(file_provider.as_ref(), &root, &directory)?;
        let owner_lock = create_owner_lock(&directory, &directory, &root, &file_provider)?;
        let inbox = Self {
            root,
            file_provider,
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
            bail!(
                "cloud request sequence {} was reused with a different envelope",
                request.seq
            );
        }
        let record = InboxRecord {
            schema_version: INBOX_RECORD_VERSION,
            request,
            status: InboxStatus::Pending,
            attempts: 0,
            updated_at: now_seconds(),
            next_attempt_at: 0,
            error: None,
            result: None,
        };
        self.write_record_path(&path, &record)?;
        if !self.restart_incomplete_recovery_after_directory_write()? {
            self.pending_sequences
                .lock()
                .map_err(|_| anyhow!("cloud inbox pending index lock was poisoned"))?
                .insert(record.request.seq);
            self.state_index
                .lock()
                .map_err(|_| anyhow!("cloud inbox state index lock was poisoned"))?
                .record(&record);
        }
        Ok(true)
    }

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

    /// Return the recent management history and every preview still awaiting confirmation.
    /// The index stores sequence numbers only; each selected durable record is revalidated/read.
    pub fn state_records(&self) -> Result<Vec<InboxRecord>> {
        self.state_records_bounded(usize::MAX)
            .map(|(records, _)| records)
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
        let (sequences, omitted_previews) = self
            .state_index
            .lock()
            .map_err(|_| anyhow!("cloud inbox state index lock was poisoned"))?
            .sequences_bounded(max_awaiting_previews);
        let records = sequences
            .into_iter()
            .map(|seq| {
                self.read_record_path(&self.record_path(seq))?
                    .ok_or_else(|| anyhow!("indexed cloud state request {seq} is missing"))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((records, omitted_previews))
    }

    pub fn record(&self, seq: u64) -> Result<Option<InboxRecord>> {
        let _guard = self
            .operation_lock
            .lock()
            .map_err(|_| anyhow!("cloud inbox lock was poisoned"))?;
        self.read_record_path(&self.record_path(seq))
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
        let path = self.record_path(seq);
        let mut record = self
            .read_record_path(&path)?
            .ok_or_else(|| anyhow!("cloud inbox request {seq} is missing"))?;
        update(&mut record)?;
        record.updated_at = now_seconds();
        self.write_record_path(&path, &record)?;
        if !self.restart_incomplete_recovery_after_directory_write()? {
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
        }
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
        self.restart_incomplete_recovery_after_directory_write()?;
        Ok(next)
    }

    fn record_path(&self, seq: u64) -> PathBuf {
        self.directory.join(format!(
            "{CLOUD_REQUEST_PREFIX}{seq:020}{CLOUD_REQUEST_SUFFIX}"
        ))
    }

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
        let entry = self.root.bind_entry(&self.directory, false)?;
        self.root
            .validate_private_bound_directory(&entry, self.directory_identity, 0o700)
            .context("cloud inbox directory is not owner-private")
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

                            if inbox_terminal_record_expired(&record, now_seconds())
                                && content_read_budget
                                    <= CLOUD_INBOX_SCAN_MAX_BYTES.saturating_sub(bytes_read)
                                && self.remove_expired_terminal_record(
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
                            } else if inbox_terminal_record_expired(&record, now_seconds())
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

    fn restart_incomplete_recovery_after_directory_write(&self) -> Result<bool> {
        let mut scan = self
            .recovery_scan
            .lock()
            .map_err(|_| anyhow!("cloud inbox recovery cursor lock was poisoned"))?;
        if scan.initial_complete {
            return Ok(false);
        }
        self.reset_incomplete_recovery(&mut scan)?;
        Ok(true)
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

    fn remove_expired_terminal_record(
        &self,
        seq: u64,
        entry: &BoundEntry,
        identity: EntryIdentity,
        file: &BoundFile,
        bytes: &[u8],
        record: &InboxRecord,
    ) -> Result<bool> {
        if !inbox_terminal_record_expired(record, now_seconds()) {
            return Ok(false);
        }
        let path = self.record_path(seq);
        let mut removed_result = None;
        let mut remove = |coordinated_path: &Path| -> Result<()> {
            let coordinated = coordinated_path_under_root(&self.root, coordinated_path)?;
            if coordinated != path {
                bail!("File Provider changed a cloud inbox cleanup path");
            }
            if !inbox_terminal_record_expired(record, now_seconds()) {
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
                            || !inbox_terminal_record_expired(&current, now_seconds())
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

fn create_owner_lock(
    requested_directory: &Path,
    directory: &Path,
    root: &RootedFs,
    file_provider: &Arc<dyn QueueFileProvider>,
) -> Result<BoundFile> {
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
        lock_file = Some(file);
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
    let relative = coordinated_path
        .strip_prefix(root.logical_root_path())
        .or_else(|_| coordinated_path.strip_prefix(root.root_path()))
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
                updated_at,
                next_attempt_at: 0,
                error: None,
                result: None,
            };
            inbox.write_record_path(&inbox.record_path(seq), &record)
        };

        for seq in 1..=120 {
            let terminal_status = match seq % 4 {
                0 => InboxStatus::Failed,
                1 => InboxStatus::Completed,
                2 => InboxStatus::Uncertain,
                _ => InboxStatus::Stale,
            };
            write_fixture(seq, terminal_status, "completed", old)
                .expect("expired terminal fixture should persist with its original age");
            match seq {
                24 => write_fixture(121, InboxStatus::Pending, "library_scan", old)
                    .expect("pending fixture should persist"),
                48 => write_fixture(122, InboxStatus::Retryable, "library_scan", old)
                    .expect("retryable fixture should persist"),
                72 => write_fixture(123, InboxStatus::AwaitingConfirmation, "file_preview", old)
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

        assert!(
            reopened
                .persist(request(126, "arrived during startup recovery"))
                .expect("new durable request should persist during bounded recovery"),
            "a new sequence should be durably added"
        );
        assert!(
            reopened
                .pending()
                .expect("recovery should restart after a new directory entry")
                .is_empty(),
            "dispatch must remain blocked while the restarted index is partial"
        );

        let mut externally_materialized = InboxRecord {
            schema_version: INBOX_RECORD_VERSION,
            request: request(127, "materialized during startup recovery"),
            status: InboxStatus::Pending,
            attempts: 0,
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

        let pending = (0..32)
            .find_map(|_| {
                let pending = reopened
                    .pending()
                    .expect("bounded recovery pass should succeed");
                let initial_complete = reopened
                    .recovery_scan
                    .lock()
                    .expect("recovery scan lock should be available")
                    .initial_complete;
                if initial_complete {
                    (!pending.is_empty()).then_some(pending)
                } else {
                    assert!(
                        pending.is_empty(),
                        "partial startup indexes must not dispatch active records"
                    );
                    None
                }
            })
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
    fn cloud_state_post_errors_classify_transient_and_payload_rejections() {
        for status in [
            reqwest::StatusCode::UNAUTHORIZED,
            reqwest::StatusCode::FORBIDDEN,
            reqwest::StatusCode::CONFLICT,
            reqwest::StatusCode::REQUEST_TIMEOUT,
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
