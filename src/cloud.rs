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
use crate::safe_fs::{BoundFile, EntryIdentity, RootedFs};
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
const MAX_REQUEST_BYTES: usize = 1024 * 1024;
const MAX_LEGACY_IMPORT_BYTES: usize = 256 * 1024;
static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

pub type CloudWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

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
        let response = self
            .client
            .get(self.api_url("api/local/requests")?)
            .query(&[("limit", "100")])
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

    pub async fn post_state(&self, state: &CloudState<'_>) -> Result<()> {
        self.client
            .post(self.api_url("api/local/state")?)
            .header(AUTHORIZATION, self.authorization.clone())
            .json(state)
            .send()
            .await
            .context("failed to publish local cloud state")?
            .error_for_status()
            .context("local cloud state update returned HTTP error")?;
        Ok(())
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedStateVersion {
    schema_version: u32,
    state_version: u64,
}

#[derive(Clone)]
pub struct CloudInbox {
    root: RootedFs,
    file_provider: Arc<dyn QueueFileProvider>,
    directory: PathBuf,
    directory_identity: EntryIdentity,
    _owner_lock: BoundFile,
    operation_lock: Arc<Mutex<()>>,
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
            coordinate_bound_directory_listing(file_provider.as_ref(), &root, &directory)?;
        let owner_lock = create_owner_lock(&directory, &directory, &root, &file_provider)?;
        let mut inbox = Self {
            root,
            file_provider,
            directory,
            directory_identity,
            _owner_lock: owner_lock,
            operation_lock: Arc::new(Mutex::new(())),
        };
        // A single local process owns the inbox. Recover unfinished management operations, which
        // are idempotent by cloud sequence and (for confirmed moves) by the file manager's
        // persistent preview manifest. A Telegram update interrupted mid-command is surfaced as
        // uncertain because some commands have external side effects without a durable receipt.
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
        Ok(true)
    }

    pub fn records(&self) -> Result<Vec<InboxRecord>> {
        let _guard = self
            .operation_lock
            .lock()
            .map_err(|_| anyhow!("cloud inbox lock was poisoned"))?;
        let paths = self.list_record_paths()?;
        paths
            .into_iter()
            .map(|path| {
                self.read_record_path(&path)?
                    .ok_or_else(|| anyhow!("cloud inbox record disappeared: {}", path.display()))
            })
            .collect()
    }

    pub fn pending(&self) -> Result<Vec<InboxRecord>> {
        let mut pending = self
            .records()?
            .into_iter()
            .filter(|record| {
                matches!(record.status, InboxStatus::Pending | InboxStatus::Retryable)
                    && record.next_attempt_at <= now_seconds()
            })
            .collect::<Vec<_>>();
        pending.sort_by_key(|record| record.request.seq);
        Ok(pending)
    }

    pub fn update(&self, seq: u64, update: impl FnOnce(&mut InboxRecord)) -> Result<InboxRecord> {
        let _guard = self
            .operation_lock
            .lock()
            .map_err(|_| anyhow!("cloud inbox lock was poisoned"))?;
        let path = self.record_path(seq);
        let mut record = self
            .read_record_path(&path)?
            .ok_or_else(|| anyhow!("cloud inbox request {seq} is missing"))?;
        update(&mut record);
        record.updated_at = now_seconds();
        self.write_record_path(&path, &record)?;
        Ok(record)
    }

    pub fn next_state_version(&self) -> Result<u64> {
        let _guard = self
            .operation_lock
            .lock()
            .map_err(|_| anyhow!("cloud inbox lock was poisoned"))?;
        let path = self.directory.join(CLOUD_STATE_VERSION_FILE);
        let current = self.read_state_version(&path)?.unwrap_or(0);
        let next = current
            .checked_add(1)
            .ok_or_else(|| anyhow!("cloud state version counter is exhausted"))?;
        if next > MAX_JS_SAFE_INTEGER {
            bail!("cloud state version is outside JavaScript's safe integer range");
        }
        self.write_state_version(&path, next)?;
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
        let mut paths = entries
            .into_iter()
            .filter_map(|(name, identity)| {
                if !identity.is_file() {
                    return None;
                }
                let name = name.to_string_lossy();
                if name.starts_with(CLOUD_REQUEST_PREFIX) && name.ends_with(CLOUD_REQUEST_SUFFIX) {
                    Some(self.directory.join(name.as_ref()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
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

    fn recover_interrupted_requests(&mut self) -> Result<()> {
        for mut record in self.records()? {
            if record.status != InboxStatus::Processing {
                continue;
            }
            if record.request.kind == "telegram" {
                record.status = InboxStatus::Uncertain;
                record.error = Some(
                    "The bot stopped during this Telegram command. Check the task queue before resending.".to_string(),
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
            self.write_record_path(&self.record_path(record.request.seq), &record)?;
        }
        Ok(())
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

fn coordinate_bound_directory_listing(
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
        let _ = root.list_bound_directory(&entry, identity)?;
        root.validate_private_bound_directory(&entry, identity, 0o700)?;
        result = Some((accessor, identity));
        Ok(())
    };
    file_provider
        .coordinate_read(path, &mut read)
        .map_err(|error| classify_deadlock_error(path, "read", error))?;
    result.ok_or_else(|| anyhow!("File Provider did not provide cloud inbox listing"))
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
