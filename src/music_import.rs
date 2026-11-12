//! Fixed test-runtime Apple Music import with durable, per-content receipts.

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
use anyhow::Context;
use anyhow::bail;
#[cfg(all(feature = "test-runtime", target_os = "macos"))]
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(all(feature = "test-runtime", target_os = "macos"))]
use serde_json::json;
#[cfg(all(feature = "test-runtime", target_os = "macos"))]
use sha2::{Digest, Sha256};
use std::path::Path;
#[cfg(all(feature = "test-runtime", target_os = "macos"))]
use std::path::PathBuf;

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
const MAX_SOURCE_SIZE: u64 = 512 * 1024 * 1024;
#[cfg(all(feature = "test-runtime", target_os = "macos"))]
const MAX_REQUEST_SIZE: u64 = 16 * 1024;
#[cfg(all(feature = "test-runtime", target_os = "macos"))]
const MAX_RECEIPT_SIZE: u64 = 64 * 1024;
#[cfg(all(feature = "test-runtime", target_os = "macos"))]
const APPLE_SCRIPT_TIMEOUT_SECONDS: u64 = 30;
#[cfg(all(feature = "test-runtime", target_os = "macos"))]
const MUSIC_SCRIPT_TIMEOUT_SECONDS: u64 = 20;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Import,
    Status,
}

impl Mode {
    fn parse(value: &str) -> anyhow::Result<Self> {
        match value {
            "import" => Ok(Self::Import),
            "status" => Ok(Self::Status),
            _ => bail!("music import mode must be 'import' or 'status'"),
        }
    }
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ImportRequest {
    schema: u8,
    source_path: String,
    expected_sha256: String,
    expected_size: u64,
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    schema: u8,
    state: String,
    request_sequence: String,
    source_path: String,
    sha256: String,
    size: u64,
    track_persistent_id: Option<String>,
    cloud_status: Option<String>,
    location: Option<String>,
    duration: Option<String>,
    last_status_error: Option<String>,
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
#[derive(Clone, Debug, Eq, PartialEq)]
struct TrackStatus {
    lookup_scope: String,
    track_class: String,
    track_kind: String,
    cloud_status: String,
    location: String,
    location_error_number: Option<String>,
    location_error_message: Option<String>,
    duration: String,
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
struct PrivatePaths {
    home: PathBuf,
    root: PathBuf,
    requests: PathBuf,
    receipts: PathBuf,
    sources: PathBuf,
    uid: u32,
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceKind {
    CloudMusic,
    PrivateStaging,
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
trait MusicExecutor {
    fn add_track(&self, source: &Path) -> anyhow::Result<String>;
    fn query_track(&self, persistent_id: &str) -> anyhow::Result<Option<TrackStatus>>;
}

/// Import or inspect one request held in the fixed private test-runtime inbox.
pub fn run_cli(mode: &str, request_path: &Path) -> anyhow::Result<Value> {
    let mode = Mode::parse(mode)?;

    #[cfg(not(feature = "test-runtime"))]
    {
        let _ = (mode, request_path);
        bail!("Music imports are available only in the test-runtime binary");
    }

    #[cfg(all(feature = "test-runtime", not(target_os = "macos")))]
    {
        let _ = (mode, request_path);
        bail!("Music imports are supported only on macOS");
    }

    #[cfg(all(feature = "test-runtime", target_os = "macos"))]
    {
        let paths = private_paths()?;
        verify_private_directory_tree(&paths, false)?;
        let (sequence, request) = load_request(&paths, request_path)?;
        validate_request(&request, &paths.home)?;

        let preflight = crate::music_automation::run_cli("check")?;
        require_music_ready(&preflight)?;

        let executor = AppleMusicExecutor;
        match mode {
            Mode::Import => {
                let receipt_path = receipt_path(&paths.receipts, &request.expected_sha256);
                if path_exists_no_follow(&receipt_path)? {
                    import_or_recover(&paths, &sequence, &request, None, &executor)
                } else {
                    let verified = verify_source(&request, &paths)?;
                    ensure_private_directory(&paths.receipts, paths.uid)?;
                    import_or_recover(&paths, &sequence, &request, Some(verified), &executor)
                }
            }
            Mode::Status => status_saved_receipt(&paths, &sequence, &request, &executor),
        }
    }
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn validate_request(request: &ImportRequest, paths_home: &Path) -> anyhow::Result<()> {
    if request.schema != 1 {
        bail!("music import request schema must be 1");
    }
    if request.expected_size == 0 || request.expected_size > MAX_SOURCE_SIZE {
        bail!("music import expected_size must be between 1 and 512 MiB");
    }
    if !is_sha256(&request.expected_sha256) {
        bail!("music import expected_sha256 must contain exactly 64 hexadecimal characters");
    }
    let source = Path::new(&request.source_path);
    validate_source_path_lexical(source, paths_home, &request.expected_sha256).map(|_| ())
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn validate_source_path_lexical(
    source: &Path,
    home: &Path,
    expected_sha256: &str,
) -> anyhow::Result<SourceKind> {
    if !source.is_absolute() {
        bail!("music source_path must be absolute");
    }
    if source.components().any(|component| {
        matches!(
            component,
            std::path::Component::CurDir | std::path::Component::ParentDir
        )
    }) {
        bail!("music source_path must not contain dot or parent components");
    }
    let kind = if source.parent() == Some(cloud_music_directory(home).as_path()) {
        SourceKind::CloudMusic
    } else if source.parent() == Some(private_sources_directory(home).as_path()) {
        let expected_name = format!("{}.m4a", normalized_sha256(expected_sha256));
        if source.file_name().and_then(std::ffi::OsStr::to_str) != Some(expected_name.as_str()) {
            bail!("private Music source filename must match the lowercase request SHA-256");
        }
        SourceKind::PrivateStaging
    } else {
        bail!(
            "music source_path must be a direct child of the iCloud Collection/Music folder or fixed private staging folder"
        );
    };
    if !source
        .extension()
        .and_then(std::ffi::OsStr::to_str)
        .is_some_and(|extension| extension.eq_ignore_ascii_case("m4a"))
    {
        bail!("music source_path must name an .m4a file");
    }
    apple_script_string_literal(
        source
            .to_str()
            .context("Music source path must be valid UTF-8")?,
    )?;
    Ok(kind)
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn cloud_music_directory(home: &Path) -> PathBuf {
    home.join("Library/Mobile Documents/com~apple~CloudDocs/Collection/Music")
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn private_sources_directory(home: &Path) -> PathBuf {
    home.join(".local/lib/telegram-video-downloader-test/music-import/sources")
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn normalized_sha256(value: &str) -> String {
    value.to_ascii_lowercase()
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn is_persistent_id(value: &str) -> bool {
    value.len() == 16 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn require_music_ready(preflight: &Value) -> anyhow::Result<()> {
    if preflight.get("ready").and_then(Value::as_bool) == Some(true)
        && preflight.get("musicPermission").and_then(Value::as_str) == Some("granted")
        && preflight.get("systemOutputMuted").and_then(Value::as_bool) == Some(true)
    {
        return Ok(());
    }
    bail!("Music preflight is not ready: {preflight}");
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn private_paths() -> anyhow::Result<PrivatePaths> {
    use std::ffi::CStr;
    use std::os::unix::ffi::OsStrExt;

    let uid = unsafe { libc::getuid() };
    let mut passwd: libc::passwd = unsafe { std::mem::zeroed() };
    let mut buffer = vec![0_u8; 16 * 1024];
    let mut result = std::ptr::null_mut();
    let status = unsafe {
        libc::getpwuid_r(
            uid,
            &mut passwd,
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            &mut result,
        )
    };
    if status != 0 {
        bail!(
            "resolve current account home directory failed: {}",
            std::io::Error::from_raw_os_error(status)
        );
    }
    if result.is_null() || passwd.pw_dir.is_null() {
        bail!("current account has no home directory in the system account database");
    }
    let home = PathBuf::from(std::ffi::OsStr::from_bytes(
        unsafe { CStr::from_ptr(passwd.pw_dir) }.to_bytes(),
    ));
    let root = home.join(".local/lib/telegram-video-downloader-test/music-import");
    Ok(PrivatePaths {
        requests: root.join("requests"),
        receipts: root.join("receipts"),
        sources: root.join("sources"),
        home,
        root,
        uid,
    })
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn verify_private_directory_tree(
    paths: &PrivatePaths,
    require_receipts: bool,
) -> anyhow::Result<()> {
    let install_root = paths.home.join(".local/lib/telegram-video-downloader-test");
    verify_owned_directory_chain(&paths.home, &install_root, paths.uid)?;
    verify_private_directory(&paths.root, paths.uid)?;
    verify_private_directory(&paths.requests, paths.uid)?;
    if require_receipts || path_exists_no_follow(&paths.receipts)? {
        verify_private_directory(&paths.receipts, paths.uid)?;
    }
    Ok(())
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn verify_owned_directory_chain(home: &Path, end: &Path, uid: u32) -> anyhow::Result<()> {
    use std::os::unix::fs::MetadataExt;

    let home_metadata = std::fs::symlink_metadata(home)
        .with_context(|| format!("inspect current account home: {}", home.display()))?;
    if home_metadata.file_type().is_symlink()
        || !home_metadata.is_dir()
        || home_metadata.uid() != uid
    {
        bail!("current account home must be an owned real directory");
    }
    let relative = end
        .strip_prefix(home)
        .context("test runtime path escaped current account home")?;
    let mut current = home.to_path_buf();
    for component in relative.components() {
        current.push(component);
        let metadata = std::fs::symlink_metadata(&current)
            .with_context(|| format!("Music import directory is missing: {}", current.display()))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() || metadata.uid() != uid {
            bail!(
                "Music import path component is not an owned real directory: {}",
                current.display()
            );
        }
    }
    Ok(())
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn verify_private_directory(path: &Path, uid: u32) -> anyhow::Result<()> {
    use std::os::unix::fs::MetadataExt;

    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("private Music directory is missing: {}", path.display()))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != uid
        || metadata.mode() & 0o777 != 0o700
    {
        bail!(
            "private Music directory must be an owned 0700 directory: {}",
            path.display()
        );
    }
    Ok(())
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn ensure_private_directory(path: &Path, uid: u32) -> anyhow::Result<()> {
    use std::os::unix::fs::DirBuilderExt;

    let mut builder = std::fs::DirBuilder::new();
    builder.mode(0o700);
    match builder.create(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error).with_context(|| format!("create {}", path.display())),
    }
    verify_private_directory(path, uid)
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn load_request(
    paths: &PrivatePaths,
    request_path: &Path,
) -> anyhow::Result<(String, ImportRequest)> {
    use std::io::Read;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

    if !request_path.is_absolute() || request_path.parent() != Some(paths.requests.as_path()) {
        bail!("request path must be a direct child of the fixed private requests directory");
    }
    let file_name = request_path
        .file_name()
        .and_then(|name| name.to_str())
        .context("request filename must be UTF-8")?;
    let sequence = file_name
        .strip_suffix(".json")
        .filter(|stem| !stem.is_empty() && stem.bytes().all(|byte| byte.is_ascii_digit()))
        .context("request filename must be <sequence>.json")?
        .to_owned();

    let link_metadata = std::fs::symlink_metadata(request_path)
        .with_context(|| format!("read request metadata: {}", request_path.display()))?;
    if link_metadata.file_type().is_symlink()
        || !link_metadata.is_file()
        || link_metadata.uid() != paths.uid
        || link_metadata.mode() & 0o777 != 0o600
        || link_metadata.len() > MAX_REQUEST_SIZE
    {
        bail!("request file must be an owned regular 0600 file no larger than 16 KiB");
    }
    let mut options = std::fs::OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options.open(request_path)?;
    let file_metadata = file.metadata()?;
    if file_metadata.dev() != link_metadata.dev() || file_metadata.ino() != link_metadata.ino() {
        bail!("request file changed while opening");
    }
    let mut bytes = Vec::with_capacity(file_metadata.len() as usize);
    file.take(MAX_REQUEST_SIZE + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_REQUEST_SIZE {
        bail!("request file exceeds 16 KiB");
    }
    let request = serde_json::from_slice(&bytes).context("parse music import request JSON")?;
    Ok((sequence, request))
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn verify_source(request: &ImportRequest, paths: &PrivatePaths) -> anyhow::Result<PathBuf> {
    use std::io::Read;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

    let source = PathBuf::from(&request.source_path);
    let source_kind = validate_source_path_lexical(&source, &paths.home, &request.expected_sha256)?;
    match source_kind {
        SourceKind::CloudMusic => verify_cloud_music_directory(&paths.home, paths.uid)?,
        SourceKind::PrivateStaging => verify_private_staging_directory(paths)?,
    }

    let link_metadata = std::fs::symlink_metadata(&source)
        .with_context(|| format!("read M4A metadata: {}", source.display()))?;
    if link_metadata.file_type().is_symlink()
        || !link_metadata.is_file()
        || link_metadata.uid() != paths.uid
        || link_metadata.len() != request.expected_size
        || (source_kind == SourceKind::PrivateStaging && link_metadata.mode() & 0o777 != 0o600)
    {
        bail!("source must be an owned regular file with the requested size");
    }

    let mut options = std::fs::OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let mut file = options.open(&source)?;
    let before = file.metadata()?;
    if before.dev() != link_metadata.dev()
        || before.ino() != link_metadata.ino()
        || (source_kind == SourceKind::PrivateStaging
            && (before.uid() != paths.uid || before.mode() & 0o777 != 0o600))
    {
        bail!("source file changed while opening");
    }

    let mut digest = Sha256::new();
    let mut first_bytes = [0_u8; 12];
    file.read_exact(&mut first_bytes)?;
    digest.update(first_bytes);
    if &first_bytes[4..8] != b"ftyp" {
        bail!("source is not an ISO BMFF M4A file with an ftyp box");
    }

    let mut total_size = first_bytes.len() as u64;
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        total_size = total_size
            .checked_add(count as u64)
            .context("source size overflow")?;
        if total_size > MAX_SOURCE_SIZE {
            bail!("source exceeds the 512 MiB import limit");
        }
        digest.update(&buffer[..count]);
    }
    let after = file.metadata()?;
    let path_after = std::fs::symlink_metadata(&source)?;
    if total_size != request.expected_size
        || after.len() != before.len()
        || after.dev() != before.dev()
        || after.ino() != before.ino()
        || path_after.file_type().is_symlink()
        || path_after.dev() != before.dev()
        || path_after.ino() != before.ino()
        || (source_kind == SourceKind::PrivateStaging
            && (after.uid() != paths.uid
                || after.mode() & 0o777 != 0o600
                || path_after.uid() != paths.uid
                || path_after.mode() & 0o777 != 0o600))
    {
        bail!("source file identity or size changed during validation");
    }

    let actual_hash = format!("{:x}", digest.finalize());
    if actual_hash != normalized_sha256(&request.expected_sha256) {
        bail!("source SHA-256 does not match the request");
    }
    Ok(source)
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn verify_private_staging_directory(paths: &PrivatePaths) -> anyhow::Result<()> {
    let install_root = paths.home.join(".local/lib/telegram-video-downloader-test");
    verify_owned_directory_chain(&paths.home, &install_root, paths.uid)?;
    verify_private_directory(&paths.root, paths.uid)?;
    verify_private_directory(&paths.sources, paths.uid)
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn verify_cloud_music_directory(home: &Path, uid: u32) -> anyhow::Result<()> {
    use std::os::unix::fs::MetadataExt;

    let home_metadata = std::fs::symlink_metadata(home)
        .with_context(|| format!("inspect current account home: {}", home.display()))?;
    if home_metadata.file_type().is_symlink()
        || !home_metadata.is_dir()
        || home_metadata.uid() != uid
    {
        bail!("current account home must be an owned real directory");
    }
    let directory = cloud_music_directory(home);
    let mut current = home.to_path_buf();
    for component in directory
        .strip_prefix(home)
        .context("cloud music directory escaped home")?
        .components()
    {
        current.push(component);
        let metadata = std::fs::symlink_metadata(&current).with_context(|| {
            format!(
                "Music source directory is unavailable: {}",
                current.display()
            )
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() || metadata.uid() != uid {
            bail!(
                "Music source path contains a non-owned directory or symlink: {}",
                current.display()
            );
        }
    }
    Ok(())
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn receipt_path(receipt_dir: &Path, sha256: &str) -> PathBuf {
    receipt_dir.join(format!("{}.json", normalized_sha256(sha256)))
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn path_exists_no_follow(path: &Path) -> anyhow::Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn read_receipt(path: &Path, uid: u32) -> anyhow::Result<Receipt> {
    use std::io::Read;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

    let link_metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("read receipt metadata: {}", path.display()))?;
    if link_metadata.file_type().is_symlink()
        || !link_metadata.is_file()
        || link_metadata.uid() != uid
        || link_metadata.mode() & 0o777 != 0o600
        || link_metadata.len() > MAX_RECEIPT_SIZE
    {
        bail!("receipt must be an owned regular 0600 file no larger than 64 KiB");
    }
    let mut options = std::fs::OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options.open(path)?;
    let metadata = file.metadata()?;
    if metadata.dev() != link_metadata.dev() || metadata.ino() != link_metadata.ino() {
        bail!("receipt changed while opening");
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_RECEIPT_SIZE + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_RECEIPT_SIZE {
        bail!("receipt exceeds 64 KiB");
    }
    let receipt: Receipt = serde_json::from_slice(&bytes).context("parse saved Music receipt")?;
    if receipt.schema != 1
        || !matches!(
            receipt.state.as_str(),
            "intent" | "unknown" | "imported" | "complete"
        )
        || !is_sha256(&receipt.sha256)
        || !is_persistent_id_or_absent(&receipt)
    {
        bail!("saved Music receipt has invalid fields");
    }
    Ok(receipt)
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn is_persistent_id_or_absent(receipt: &Receipt) -> bool {
    receipt
        .track_persistent_id
        .as_deref()
        .is_none_or(is_persistent_id)
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn create_intent(path: &Path, receipt: &Receipt) -> anyhow::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    let parent = path.parent().context("receipt path has no parent")?;
    let bytes = serde_json::to_vec(receipt)?;
    let mut options = std::fs::OpenOptions::new();
    options
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let mut file = options
        .open(path)
        .with_context(|| format!("create durable Music intent: {}", path.display()))?;
    file.write_all(&bytes)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    sync_directory(parent)
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn update_receipt(path: &Path, receipt: &Receipt) -> anyhow::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);
    let parent = path.parent().context("receipt path has no parent")?;
    let mut temp_path = None;
    let mut temp_file = None;
    for _ in 0..8 {
        let suffix = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let candidate = parent.join(format!(".receipt-{}-{suffix}.tmp", std::process::id()));
        let mut options = std::fs::OpenOptions::new();
        options
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        match options.open(&candidate) {
            Ok(file) => {
                temp_path = Some(candidate);
                temp_file = Some(file);
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
    let temp_path = temp_path.context("could not allocate private receipt update file")?;
    let mut file = temp_file.expect("temporary path and file are created together");
    let bytes = serde_json::to_vec(receipt)?;
    let result = (|| -> anyhow::Result<()> {
        file.write_all(&bytes)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temp_path, path)?;
        sync_directory(parent)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp_path);
    }
    result
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn sync_directory(path: &Path) -> anyhow::Result<()> {
    std::fs::File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn make_intent(sequence: &str, request: &ImportRequest) -> Receipt {
    Receipt {
        schema: 1,
        state: "intent".into(),
        request_sequence: sequence.into(),
        source_path: request.source_path.clone(),
        sha256: normalized_sha256(&request.expected_sha256),
        size: request.expected_size,
        track_persistent_id: None,
        cloud_status: None,
        location: None,
        duration: None,
        last_status_error: None,
    }
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn validate_receipt_for_request(receipt: &Receipt, request: &ImportRequest) -> anyhow::Result<()> {
    if receipt.sha256 != normalized_sha256(&request.expected_sha256)
        || receipt.size != request.expected_size
    {
        bail!("saved receipt does not match the request content identity");
    }
    Ok(())
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn import_or_recover<E: MusicExecutor>(
    paths: &PrivatePaths,
    sequence: &str,
    request: &ImportRequest,
    verified_source: Option<PathBuf>,
    executor: &E,
) -> anyhow::Result<Value> {
    let target = receipt_path(&paths.receipts, &request.expected_sha256);
    if path_exists_no_follow(&target)? {
        let receipt = read_receipt(&target, paths.uid)?;
        validate_receipt_for_request(&receipt, request)?;
        if !matches!(receipt.state.as_str(), "imported" | "complete")
            || !receipt
                .track_persistent_id
                .as_deref()
                .is_some_and(is_persistent_id)
        {
            bail!("saved Music intent is unresolved; refusing to repeat the add");
        }
        return refresh_saved_receipt(&target, receipt, executor, "import", false);
    }

    let source = verified_source.context("new Music import requires verified source bytes")?;
    let mut receipt = make_intent(sequence, request);
    create_intent(&target, &receipt)?;

    let add_result = executor.add_track(&source);
    let persistent_id = match add_result {
        Ok(value) if is_persistent_id(value.trim()) => value.trim().to_ascii_uppercase(),
        Ok(_) => {
            receipt.state = "unknown".into();
            let _ = update_receipt(&target, &receipt);
            bail!(
                "Music add returned an invalid persistent ID; outcome is unknown and will not be retried"
            );
        }
        Err(error) => {
            receipt.state = "unknown".into();
            let update_error = update_receipt(&target, &receipt).err();
            if let Some(update_error) = update_error {
                bail!(
                    "Music add outcome is unknown ({error:#}); failed to persist unknown state ({update_error:#})"
                );
            }
            bail!("Music add outcome is unknown and will not be retried: {error:#}");
        }
    };

    receipt.state = "imported".into();
    receipt.track_persistent_id = Some(persistent_id.clone());
    receipt.last_status_error = None;
    update_receipt(&target, &receipt)
        .context("persist Music persistent ID before status lookup")?;
    refresh_saved_receipt(&target, receipt, executor, "import", true)
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn status_saved_receipt<E: MusicExecutor>(
    paths: &PrivatePaths,
    _sequence: &str,
    request: &ImportRequest,
    executor: &E,
) -> anyhow::Result<Value> {
    verify_private_directory(&paths.receipts, paths.uid)?;
    let target = receipt_path(&paths.receipts, &request.expected_sha256);
    let receipt = read_receipt(&target, paths.uid)?;
    validate_receipt_for_request(&receipt, request)?;
    if !matches!(receipt.state.as_str(), "imported" | "complete")
        || !receipt
            .track_persistent_id
            .as_deref()
            .is_some_and(is_persistent_id)
    {
        bail!("saved Music intent has no known persistent ID; status cannot query a track");
    }
    refresh_saved_receipt(&target, receipt, executor, "status", false)
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn refresh_saved_receipt<E: MusicExecutor>(
    target: &Path,
    mut receipt: Receipt,
    executor: &E,
    mode: &str,
    added: bool,
) -> anyhow::Result<Value> {
    let persistent_id = receipt
        .track_persistent_id
        .clone()
        .context("saved receipt has no persistent ID")?;
    match executor.query_track(&persistent_id) {
        Ok(Some(status)) => {
            receipt.state = "complete".into();
            receipt.cloud_status = Some(status.cloud_status.clone());
            receipt.location = Some(status.location.clone());
            receipt.duration = Some(status.duration.clone());
            receipt.last_status_error = None;
            update_receipt(target, &receipt).context("save Music track status")?;
            Ok(json!({
                "mode": mode,
                "state": "found",
                "added": added,
                "request_sequence": receipt.request_sequence,
                "sha256": receipt.sha256,
                "size": receipt.size,
                "track_persistent_id": persistent_id,
                "lookup_scope": status.lookup_scope,
                "track_class": status.track_class,
                "track_kind": status.track_kind,
                "cloud_status": status.cloud_status,
                "location": status.location,
                "location_error_number": status.location_error_number,
                "location_error_message": status.location_error_message,
                "duration": status.duration,
            }))
        }
        Ok(None) => {
            receipt.last_status_error = None;
            let _ = update_receipt(target, &receipt);
            Ok(json!({
                "mode": mode,
                "state": "missing",
                "added": added,
                "request_sequence": receipt.request_sequence,
                "sha256": receipt.sha256,
                "size": receipt.size,
                "track_persistent_id": persistent_id,
                "cloud_status": receipt.cloud_status,
                "location": receipt.location,
                "duration": receipt.duration,
            }))
        }
        Err(error) => {
            receipt.last_status_error = Some(format!("{error:#}"));
            let persistence_error = update_receipt(target, &receipt).err();
            let mut result = json!({
                "mode": mode,
                "state": "status_error",
                "added": added,
                "request_sequence": receipt.request_sequence,
                "sha256": receipt.sha256,
                "size": receipt.size,
                "track_persistent_id": persistent_id,
                "status_error": format!("{error:#}"),
            });
            if let Some(persistence_error) = persistence_error {
                result["receipt_update_error"] = json!(format!("{persistence_error:#}"));
            }
            Ok(result)
        }
    }
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn apple_script_string_literal(value: &str) -> anyhow::Result<String> {
    let mut literal = String::with_capacity(value.len() + 2);
    literal.push('"');
    for character in value.chars() {
        match character {
            '"' => literal.push_str("\\\""),
            '\\' => literal.push_str("\\\\"),
            character if character.is_control() => {
                bail!("AppleScript path argument contains a control character")
            }
            character => literal.push(character),
        }
    }
    literal.push('"');
    Ok(literal)
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn build_add_script(source: &Path) -> anyhow::Result<String> {
    let source = source
        .to_str()
        .context("Music source path must be valid UTF-8")?;
    let source_literal = apple_script_string_literal(source)?;
    Ok(format!(
        r#"on run
with timeout of {MUSIC_SCRIPT_TIMEOUT_SECONDS} seconds
  tell application id "com.apple.Music"
    set addedItem to add (POSIX file {source_literal})
    if class of addedItem is list then
      if (count of addedItem) is not 1 then error "unexpected added track count"
      set addedTrack to item 1 of addedItem
    else if class of addedItem is track or class of addedItem is file track then
      set addedTrack to addedItem
    else
      error "unexpected add result class"
    end if
    set trackPid to persistent ID of addedTrack as text
  end tell
end timeout
return trackPid
end run"#
    ))
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn build_status_script(persistent_id: &str) -> anyhow::Result<String> {
    if !is_persistent_id(persistent_id) {
        bail!("invalid Music persistent ID");
    }
    let persistent_id = apple_script_string_literal(&persistent_id.to_ascii_uppercase())?;
    Ok(format!(
        r#"on run
set trackLocation to ""
set locationErrorNumber to ""
set locationErrorMessage to ""
with timeout of {MUSIC_SCRIPT_TIMEOUT_SECONDS} seconds
  tell application id "com.apple.Music"
    set fileMatches to (file tracks of library playlist 1 whose persistent ID is {persistent_id})
    if (count of fileMatches) > 1 then error "unexpected file track match count"
    if (count of fileMatches) is 1 then
      set trackItem to item 1 of fileMatches
      set lookupScope to "file_track"
    else
      set genericMatches to (tracks of library playlist 1 whose persistent ID is {persistent_id})
      if (count of genericMatches) is 0 then return "missing"
      if (count of genericMatches) is not 1 then error "unexpected generic track match count"
      set trackItem to item 1 of genericMatches
      set lookupScope to "generic_track_no_file_track"
    end if
    set trackClass to (class of trackItem) as text
    set trackKind to kind of trackItem as text
    set trackCloudStatus to cloud status of trackItem as text
    try
      set trackFile to (get location of trackItem)
      set trackLocation to POSIX path of (trackFile as alias)
    on error caughtMessage number caughtNumber
      set trackLocation to ""
      set locationErrorNumber to caughtNumber as text
      set locationErrorMessage to caughtMessage as text
    end try
    set trackDuration to duration of trackItem as text
  end tell
end timeout
return "found" & tab & my safeField(lookupScope) & tab & my safeField(trackClass) & tab & my safeField(trackKind) & tab & my safeField(trackCloudStatus) & tab & my safeField(trackLocation) & tab & my safeField(locationErrorNumber) & tab & my safeField(locationErrorMessage) & tab & my safeField(trackDuration)
end run

on safeField(theValue)
  set sourceText to theValue as text
  set cleanText to ""
  repeat with charIndex from 1 to (count characters of sourceText)
    set theCharacter to character charIndex of sourceText
    if theCharacter is tab or theCharacter is return or theCharacter is linefeed then
      set cleanText to cleanText & " "
    else
      set cleanText to cleanText & theCharacter
    end if
  end repeat
  return cleanText
end safeField"#
    ))
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn execute_apple_script(source: String, stage: &'static str) -> anyhow::Result<String> {
    use std::sync::mpsc;
    use std::time::Duration;

    let (sender, receiver) = mpsc::channel();
    std::thread::Builder::new()
        .name("music-import-appleevent".into())
        .spawn(move || {
            let _ = sender.send(crate::music_automation::execute_owned_music_script(
                source,
                stage,
                APPLE_SCRIPT_TIMEOUT_SECONDS,
            ));
        })
        .context("start Music AppleEvent worker")?;

    match receiver.recv_timeout(Duration::from_secs(APPLE_SCRIPT_TIMEOUT_SECONDS + 2)) {
        Ok(result) => result,
        Err(mpsc::RecvTimeoutError::Timeout) => {
            bail!(
                "{stage} worker exceeded {} seconds",
                APPLE_SCRIPT_TIMEOUT_SECONDS + 2
            )
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            bail!("{stage} AppleEvent worker stopped without a result")
        }
    }
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
struct AppleMusicExecutor;

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
impl MusicExecutor for AppleMusicExecutor {
    fn add_track(&self, source: &Path) -> anyhow::Result<String> {
        execute_apple_script(build_add_script(source)?, "Music.add")
    }

    fn query_track(&self, persistent_id: &str) -> anyhow::Result<Option<TrackStatus>> {
        let text = execute_apple_script(build_status_script(persistent_id)?, "Music.status")?;
        if text.trim() == "missing" {
            return Ok(None);
        }
        let mut fields = text.splitn(9, '\t');
        if fields.next() != Some("found") {
            bail!("Music.status returned an unknown response");
        }
        let lookup_scope = fields.next().context("Music.status omitted lookup scope")?;
        let track_class = fields.next().context("Music.status omitted track class")?;
        let track_kind = fields.next().context("Music.status omitted track kind")?;
        let cloud_status = fields.next().context("Music.status omitted cloud status")?;
        let location = fields
            .next()
            .context("Music.status omitted track location")?;
        let location_error_number = fields
            .next()
            .context("Music.status omitted location error number")?;
        let location_error_message = fields
            .next()
            .context("Music.status omitted location error message")?;
        let duration = fields.next().context("Music.status omitted duration")?;
        Ok(Some(TrackStatus {
            lookup_scope: lookup_scope.to_owned(),
            track_class: track_class.to_owned(),
            track_kind: track_kind.to_owned(),
            cloud_status: cloud_status.to_owned(),
            location: location.to_owned(),
            location_error_number: (!location_error_number.is_empty())
                .then(|| location_error_number.to_owned()),
            location_error_message: (!location_error_message.is_empty())
                .then(|| location_error_message.to_owned()),
            duration: duration.to_owned(),
        }))
    }
}

#[cfg(all(test, feature = "test-runtime", target_os = "macos"))]
mod tests {
    use super::*;

    #[test]
    fn only_fixed_modes_are_accepted() {
        assert_eq!(Mode::parse("import").unwrap(), Mode::Import);
        assert_eq!(Mode::parse("status").unwrap(), Mode::Status);
        assert!(Mode::parse("retry").is_err());
    }

    #[test]
    fn apple_script_path_literal_escapes_quotes_and_backslashes_and_rejects_controls() {
        let path = r#"/tmp/artist\"; error \"injected\""#;
        let literal = apple_script_string_literal(path).unwrap();
        assert_eq!(literal, r#""/tmp/artist\\\"; error \\\"injected\\\"""#);
        assert!(apple_script_string_literal("/tmp/line\nbreak.m4a").is_err());
        assert!(apple_script_string_literal("/tmp/injected\nend tell").is_err());
    }

    #[test]
    fn status_script_uses_only_the_persistent_id() {
        let script = build_status_script("0123456789ABCDEF").unwrap();
        assert!(script.contains("whose persistent ID is \"0123456789ABCDEF\""));
        assert!(script.contains("file tracks of library playlist 1"));
        assert!(script.contains("generic_track_no_file_track"));
        assert!(script.contains("set trackLocation to \"\"\nset locationErrorNumber to \"\"\nset locationErrorMessage to \"\""));
        assert!(script.contains("on error caughtMessage number caughtNumber"));
        let location_get = script
            .find("set trackFile to (get location of trackItem)")
            .unwrap();
        let alias_conversion = script
            .find("set trackLocation to POSIX path of (trackFile as alias)")
            .unwrap();
        assert!(location_get < alias_conversion);
        assert!(!script.contains("POSIX path of (location of trackItem)"));
        assert!(script.contains("set locationErrorNumber to caughtNumber as text"));
        assert!(script.contains("set locationErrorMessage to caughtMessage as text"));
        assert!(
            script.contains(
                "theCharacter is tab or theCharacter is return or theCharacter is linefeed"
            )
        );
        assert!(!script.contains("source_path"));
        assert!(!script.contains("read "));
        assert!(build_status_script("bad\"; add something").is_err());
    }

    #[test]
    fn request_path_must_be_direct_child_of_expected_cloud_music_folder() {
        let home = Path::new("/Users/example");
        let allowed = cloud_music_directory(home);
        let hash = "a".repeat(64);
        assert!(validate_source_path_lexical(&allowed.join("track.m4a"), home, &hash).is_ok());
        assert!(
            validate_source_path_lexical(&allowed.join("nested/track.m4a"), home, &hash).is_err()
        );
        assert!(validate_source_path_lexical(&allowed.join("../track.m4a"), home, &hash).is_err());
    }

    #[test]
    fn private_staging_source_is_direct_and_bound_to_lowercase_request_hash() {
        let home = Path::new("/Users/example");
        let hash = "a".repeat(64);
        let directory = private_sources_directory(home);
        let valid = directory.join(format!("{hash}.m4a"));
        assert_eq!(
            validate_source_path_lexical(&valid, home, &hash).unwrap(),
            SourceKind::PrivateStaging
        );
        assert!(validate_source_path_lexical(&valid, home, &"b".repeat(64)).is_err());
        assert!(validate_source_path_lexical(&directory.join("other.m4a"), home, &hash).is_err());
        assert!(
            validate_source_path_lexical(&directory.join("../sources/track.m4a"), home, &hash)
                .is_err()
        );
        let other_root = home.join(".local/lib/other/music-import/sources");
        assert!(
            validate_source_path_lexical(&other_root.join(format!("{hash}.m4a")), home, &hash)
                .is_err()
        );
    }

    #[cfg(all(feature = "test-runtime", target_os = "macos"))]
    mod import_state_tests {
        use super::*;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct TestDirectory(PathBuf);

        impl TestDirectory {
            fn new() -> Self {
                use std::os::unix::fs::PermissionsExt;
                static NEXT: AtomicUsize = AtomicUsize::new(0);
                let path = std::env::temp_dir().join(format!(
                    "music-import-unit-{}-{}",
                    std::process::id(),
                    NEXT.fetch_add(1, Ordering::Relaxed)
                ));
                std::fs::create_dir(&path).unwrap();
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
                let receipts = path.join("receipts");
                std::fs::create_dir(&receipts).unwrap();
                std::fs::set_permissions(&receipts, std::fs::Permissions::from_mode(0o700))
                    .unwrap();
                Self(path)
            }
        }

        impl Drop for TestDirectory {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }

        struct MockExecutor {
            adds: AtomicUsize,
            queries: AtomicUsize,
            add_error: bool,
            query_error: bool,
            receipt_to_inspect: Option<PathBuf>,
            uid: u32,
        }

        impl MockExecutor {
            fn new(uid: u32) -> Self {
                Self {
                    adds: AtomicUsize::new(0),
                    queries: AtomicUsize::new(0),
                    add_error: false,
                    query_error: false,
                    receipt_to_inspect: None,
                    uid,
                }
            }
        }

        impl MusicExecutor for MockExecutor {
            fn add_track(&self, _source: &Path) -> anyhow::Result<String> {
                self.adds.fetch_add(1, Ordering::SeqCst);
                if self.add_error {
                    bail!("mock add timeout");
                }
                Ok("0123456789ABCDEF".into())
            }

            fn query_track(&self, _persistent_id: &str) -> anyhow::Result<Option<TrackStatus>> {
                self.queries.fetch_add(1, Ordering::SeqCst);
                if let Some(path) = &self.receipt_to_inspect {
                    let receipt = read_receipt(path, self.uid)?;
                    if receipt.state != "imported"
                        || receipt.track_persistent_id.as_deref() != Some("0123456789ABCDEF")
                    {
                        bail!("PID was not persisted before status query");
                    }
                }
                if self.query_error {
                    bail!("mock status timeout");
                }
                Ok(Some(TrackStatus {
                    lookup_scope: "file_track".into(),
                    track_class: "file track".into(),
                    track_kind: "Apple Lossless audio file".into(),
                    cloud_status: "matched".into(),
                    location: "/tmp/music-owned-copy.m4a".into(),
                    location_error_number: None,
                    location_error_message: None,
                    duration: "123.4".into(),
                }))
            }
        }

        fn test_paths(root: &Path) -> PrivatePaths {
            PrivatePaths {
                home: root.to_path_buf(),
                root: root.to_path_buf(),
                requests: root.join("requests"),
                receipts: root.join("receipts"),
                sources: root.join("sources"),
                uid: unsafe { libc::getuid() },
            }
        }

        fn test_request() -> ImportRequest {
            ImportRequest {
                schema: 1,
                source_path: "/missing/source.m4a".into(),
                expected_sha256: "a".repeat(64),
                expected_size: 100,
            }
        }

        #[test]
        fn unknown_add_intent_is_never_retried() {
            let directory = TestDirectory::new();
            let paths = test_paths(&directory.0);
            let request = test_request();
            let executor = MockExecutor {
                add_error: true,
                ..MockExecutor::new(paths.uid)
            };
            let source = Some(PathBuf::from("/mock/validated-source.m4a"));

            let error =
                import_or_recover(&paths, "001", &request, source.clone(), &executor).unwrap_err();
            assert!(error.to_string().contains("outcome is unknown"));

            let receipt_path = receipt_path(&paths.receipts, &request.expected_sha256);
            assert_eq!(
                read_receipt(&receipt_path, paths.uid).unwrap().state,
                "unknown"
            );
            assert!(import_or_recover(&paths, "001", &request, source, &executor).is_err());
            assert_eq!(executor.adds.load(Ordering::SeqCst), 1);
            assert_eq!(executor.queries.load(Ordering::SeqCst), 0);
        }

        #[test]
        fn persistent_id_is_saved_before_status_and_status_failure_keeps_it() {
            let directory = TestDirectory::new();
            let paths = test_paths(&directory.0);
            let request = test_request();
            let target = receipt_path(&paths.receipts, &request.expected_sha256);
            let executor = MockExecutor {
                query_error: true,
                receipt_to_inspect: Some(target.clone()),
                ..MockExecutor::new(paths.uid)
            };

            let result = import_or_recover(
                &paths,
                "001",
                &request,
                Some(PathBuf::from("/mock/validated-source.m4a")),
                &executor,
            )
            .unwrap();
            assert_eq!(result["state"], "status_error");
            assert!(result["added"].as_bool().unwrap());
            assert_eq!(result["track_persistent_id"], "0123456789ABCDEF");

            let receipt = read_receipt(&target, paths.uid).unwrap();
            assert_eq!(receipt.state, "imported");
            assert_eq!(
                receipt.track_persistent_id.as_deref(),
                Some("0123456789ABCDEF")
            );
            assert_eq!(executor.adds.load(Ordering::SeqCst), 1);

            let _ = import_or_recover(&paths, "002", &request, None, &executor).unwrap();
            assert_eq!(executor.adds.load(Ordering::SeqCst), 1);
        }

        #[test]
        fn status_recovery_never_reads_request_source_bytes() {
            let directory = TestDirectory::new();
            let paths = test_paths(&directory.0);
            let mut request = test_request();
            request.source_path = directory.0.join("evicted-source.m4a").display().to_string();
            let mut receipt = make_intent("001", &request);
            receipt.state = "complete".into();
            receipt.track_persistent_id = Some("0123456789ABCDEF".into());
            let target = receipt_path(&paths.receipts, &request.expected_sha256);
            create_intent(&target, &receipt).unwrap();
            let executor = MockExecutor::new(paths.uid);

            let result = status_saved_receipt(&paths, "001", &request, &executor).unwrap();
            assert_eq!(result["state"], "found");
            assert_eq!(executor.queries.load(Ordering::SeqCst), 1);
            assert!(!Path::new(&request.source_path).exists());
        }
    }
}
