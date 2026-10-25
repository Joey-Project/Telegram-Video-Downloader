use super::*;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug, Clone)]
struct CapturedHttpRequest {
    method: String,
    path: String,
    authorization: Option<String>,
    database_epoch: Option<String>,
}

#[derive(Debug)]
struct MockHttpResponse {
    status: u16,
    headers: Vec<(String, String)>,
}

impl MockHttpResponse {
    fn status(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
        }
    }

    fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }
}

async fn start_mock_worker<F>(
    request_count: usize,
    respond: F,
) -> (
    String,
    Arc<StdMutex<Vec<CapturedHttpRequest>>>,
    tokio::task::JoinHandle<()>,
)
where
    F: Fn(&CapturedHttpRequest) -> MockHttpResponse + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("mock Worker should bind localhost");
    let address = listener
        .local_addr()
        .expect("mock Worker should expose its local address");
    let requests = Arc::new(StdMutex::new(Vec::new()));
    let captured = Arc::clone(&requests);
    let server = tokio::spawn(async move {
        for _ in 0..request_count {
            let (mut stream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
                .await
                .expect("mock Worker should receive each expected request")
                .expect("mock Worker accept should succeed");
            let request = read_http_request(&mut stream).await;
            let response = respond(&request);
            captured
                .lock()
                .expect("mock Worker request log should be available")
                .push(request);
            write_http_response(&mut stream, response).await;
        }
    });
    (format!("http://{address}/"), requests, server)
}

async fn read_http_request(stream: &mut TcpStream) -> CapturedHttpRequest {
    let mut bytes = Vec::new();
    let mut header_end = None;
    let mut content_length = 0_usize;
    loop {
        if header_end.is_none()
            && let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n")
        {
            let end = index + 4;
            let headers = std::str::from_utf8(&bytes[..index])
                .expect("mock Worker request headers should be UTF-8");
            for line in headers.lines().skip(1) {
                if let Some((name, value)) = line.split_once(':')
                    && name.eq_ignore_ascii_case("content-length")
                {
                    content_length = value
                        .trim()
                        .parse()
                        .expect("content length should be numeric");
                }
            }
            header_end = Some(end);
        }
        if header_end.is_some_and(|end| bytes.len() >= end.saturating_add(content_length)) {
            break;
        }
        let mut buffer = [0_u8; 4096];
        let read = stream
            .read(&mut buffer)
            .await
            .expect("mock Worker request should be readable");
        assert_ne!(
            read, 0,
            "mock Worker request must contain its complete body"
        );
        bytes.extend_from_slice(&buffer[..read]);
    }

    let end = header_end.expect("mock Worker request should contain HTTP headers");
    let headers =
        std::str::from_utf8(&bytes[..end - 4]).expect("mock Worker headers should be UTF-8");
    let mut lines = headers.lines();
    let request_line = lines
        .next()
        .expect("mock Worker request should have a request line");
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .expect("request method should exist")
        .to_string();
    let path = request_parts
        .next()
        .expect("request path should exist")
        .to_string();
    let mut authorization = None;
    let mut database_epoch = None;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("authorization") {
                authorization = Some(value.trim().to_string());
            } else if name.eq_ignore_ascii_case("x-cloud-database-epoch") {
                database_epoch = Some(value.trim().to_string());
            }
        }
    }
    CapturedHttpRequest {
        method,
        path,
        authorization,
        database_epoch,
    }
}

async fn write_http_response(stream: &mut TcpStream, response: MockHttpResponse) {
    let reason = match response.status {
        200 => "OK",
        409 => "Conflict",
        428 => "Precondition Required",
        500 => "Internal Server Error",
        _ => "Test Response",
    };
    let mut wire = format!(
        "HTTP/1.1 {} {}\r\ncontent-length: 0\r\nconnection: close\r\n",
        response.status, reason
    );
    for (name, value) in response.headers {
        wire.push_str(&format!("{name}: {value}\r\n"));
    }
    wire.push_str("\r\n");
    stream
        .write_all(wire.as_bytes())
        .await
        .expect("mock Worker response should be writable");
}

fn state<'a>(state_version: u64, library: &'a Value) -> CloudState<'a> {
    CloudState {
        state_version,
        revision: None,
        reported_at: now_millis(),
        library,
        previews: &[],
        operations: &[],
        tasks: &[],
        settings: CloudSettings {
            download_dir: "/Videos",
            applied: None,
        },
    }
}

async fn post_state(
    client: &CloudClient,
    state: &CloudState<'_>,
) -> std::result::Result<(), CloudStatePostError> {
    tokio::time::timeout(Duration::from_secs(3), client.post_state(state))
        .await
        .expect("mock Worker request should finish promptly")
}

fn count_prefix(requests: &[CapturedHttpRequest], prefix: &str) -> usize {
    requests
        .iter()
        .filter(|request| request.path.starts_with(prefix))
        .count()
}

fn count_exact_path(requests: &[CapturedHttpRequest], path: &str) -> usize {
    requests
        .iter()
        .filter(|request| request.path == path)
        .count()
}

#[test]
fn staged_hashes_expire_but_committed_hashes_require_a_newer_epoch_version() {
    let mut cache = AcknowledgedCloudStateChunks::default();
    let now = Instant::now();
    let after_expiry = now
        .checked_add(CLOUD_STATE_CHUNK_ACK_CACHE_TTL + Duration::from_secs(1))
        .expect("test clock should represent the staged-cache expiry point");
    cache.set_epoch(TEST_DATABASE_EPOCH);
    cache.remember_staged_at(TEST_DATABASE_EPOCH, "staged-hash", now);
    assert!(cache.contains_at(TEST_DATABASE_EPOCH, "staged-hash", now));
    assert!(!cache.contains_at(TEST_DATABASE_EPOCH, "staged-hash", after_expiry));

    cache.pin_committed(
        TEST_DATABASE_EPOCH,
        10,
        BTreeSet::from(["current-hash".to_string()]),
    );
    cache.pin_committed(
        TEST_DATABASE_EPOCH,
        9,
        BTreeSet::from(["delayed-old-hash".to_string()]),
    );
    assert!(cache.contains_at(TEST_DATABASE_EPOCH, "current-hash", after_expiry));
    assert!(!cache.contains_at(TEST_DATABASE_EPOCH, "delayed-old-hash", after_expiry));

    let next_epoch = "ffffffffffffffffffffffffffffffff";
    cache.set_epoch(next_epoch);
    cache.pin_committed(
        TEST_DATABASE_EPOCH,
        11,
        BTreeSet::from(["old-epoch-hash".to_string()]),
    );
    assert!(!cache.contains_at(TEST_DATABASE_EPOCH, "current-hash", after_expiry));
    assert!(!cache.contains_at(next_epoch, "old-epoch-hash", after_expiry));
}

#[tokio::test]
async fn committed_chunks_stay_pinned_and_changed_content_uploads_only_its_delta() {
    let (base_url, requests, server) =
        start_mock_worker(6, |_| MockHttpResponse::status(200)).await;
    let client =
        CloudClient::with_test_base_url(&base_url).expect("mock cloud client should build");
    let library_v1 = serde_json::json!({"revision":"cache-library-v1","items":[],"free_bytes":100});
    let first = state(1, &library_v1);
    post_state(&client, &first)
        .await
        .expect("initial snapshot should publish");

    let library_v2 = serde_json::json!({"revision":"cache-library-v1","items":[],"free_bytes":101});
    let changed = state(2, &library_v2);
    post_state(&client, &changed)
        .await
        .expect("changed snapshot should publish");
    let unchanged = state(3, &library_v2);
    post_state(&client, &unchanged)
        .await
        .expect("newer unchanged snapshot should publish");

    let requests = requests
        .lock()
        .expect("mock Worker request log should be available")
        .clone();
    assert_eq!(count_prefix(&requests, "/api/local/state/chunks/"), 3);
    assert_eq!(count_exact_path(&requests, "/api/local/state"), 3);
    assert!(requests.iter().all(|request| {
        request.method == "POST"
            && request.authorization.as_deref()
                == Some(format!("Bearer {TEST_CLOUD_SECRET}").as_str())
            && request.database_epoch.as_deref() == Some(TEST_DATABASE_EPOCH)
    }));
    let committed = client
        .acknowledged_state_chunks
        .lock()
        .expect("chunk cache should be available");
    assert_eq!(
        committed.committed.as_ref().map(|set| set.state_version),
        Some(3)
    );
    server.abort();
}

#[tokio::test]
async fn failed_manifest_stays_staged_until_expiry_and_428_clears_the_cache() {
    let manifest_count = Arc::new(AtomicUsize::new(0));
    let manifest_script = Arc::clone(&manifest_count);
    let (base_url, requests, server) = start_mock_worker(10, move |request| {
        if request.path == "/api/local/state" {
            let status = match manifest_script.fetch_add(1, Ordering::SeqCst) {
                0 => 500,
                1 => 200,
                2 => 428,
                _ => 200,
            };
            MockHttpResponse::status(status)
        } else {
            MockHttpResponse::status(200)
        }
    })
    .await;
    let client =
        CloudClient::with_test_base_url(&base_url).expect("mock cloud client should build");
    let library = serde_json::json!({"revision":"cache-failure-library","items":[]});
    let first = state(1, &library);
    assert!(matches!(
        post_state(&client, &first).await,
        Err(CloudStatePostError::HttpStatus(status)) if status == reqwest::StatusCode::INTERNAL_SERVER_ERROR
    ));
    {
        let cache = client
            .acknowledged_state_chunks
            .lock()
            .expect("chunk cache should be available");
        assert!(
            cache.committed.is_none(),
            "chunks must not be pinned before manifest ACK"
        );
        assert_eq!(cache.staged.len(), 2);
    }

    let future = Instant::now()
        .checked_add(CLOUD_STATE_CHUNK_ACK_CACHE_TTL + Duration::from_secs(1))
        .expect("test clock should represent the staged-cache expiry point");
    client
        .acknowledged_state_chunks
        .lock()
        .expect("chunk cache should be available")
        .expire_staged_at(future);
    post_state(&client, &first)
        .await
        .expect("expired staged chunks should upload again before a successful commit");

    let second = state(2, &library);
    assert!(matches!(
        post_state(&client, &second).await,
        Err(CloudStatePostError::HttpStatus(status)) if status == reqwest::StatusCode::PRECONDITION_REQUIRED
    ));
    {
        let cache = client
            .acknowledged_state_chunks
            .lock()
            .expect("chunk cache should be available");
        assert!(cache.committed.is_none(), "428 must clear committed hashes");
        assert!(cache.staged.is_empty(), "428 must clear staged hashes");
    }
    let third = state(3, &library);
    post_state(&client, &third)
        .await
        .expect("a post-428 retry should upload chunks and commit successfully");

    let requests = requests
        .lock()
        .expect("mock Worker request log should be available")
        .clone();
    assert_eq!(count_prefix(&requests, "/api/local/state/chunks/"), 6);
    assert_eq!(count_exact_path(&requests, "/api/local/state"), 4);
    server.abort();
}

#[tokio::test]
async fn failed_chunk_upload_is_not_cached_and_successful_sibling_remains_staged() {
    let library = serde_json::json!({"revision":"cache-chunk-failure-library","items":[]});
    let snapshot = state(1, &library);
    let failed_hash = snapshot
        .chunked_snapshot(true)
        .expect("fixture snapshot should chunk")
        .chunks[0]
        .sha256
        .clone();
    let failed_once = Arc::new(AtomicBool::new(false));
    let fail_hash = failed_hash.clone();
    let failed_state = Arc::clone(&failed_once);
    let (base_url, requests, server) = start_mock_worker(4, move |request| {
        if request.path.ends_with(&fail_hash) && !failed_state.swap(true, Ordering::SeqCst) {
            MockHttpResponse::status(500)
        } else {
            MockHttpResponse::status(200)
        }
    })
    .await;
    let client =
        CloudClient::with_test_base_url(&base_url).expect("mock cloud client should build");
    assert!(matches!(
        post_state(&client, &snapshot).await,
        Err(CloudStatePostError::HttpStatus(status)) if status == reqwest::StatusCode::INTERNAL_SERVER_ERROR
    ));
    post_state(&client, &snapshot)
        .await
        .expect("failed chunk should retry while its successful sibling stays staged");

    let requests = requests
        .lock()
        .expect("mock Worker request log should be available")
        .clone();
    assert_eq!(count_prefix(&requests, "/api/local/state/chunks/"), 3);
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.path.ends_with(&failed_hash))
            .count(),
        2
    );
    assert_eq!(count_exact_path(&requests, "/api/local/state"), 1);
    server.abort();
}

#[tokio::test]
async fn epoch_conflict_precedes_a_concurrent_chunk_error_and_clears_hints() {
    let chunk_count = Arc::new(AtomicUsize::new(0));
    let first_two = Arc::clone(&chunk_count);
    let (base_url, requests, server) = start_mock_worker(5, move |request| {
        if request.path.starts_with("/api/local/state/chunks/") {
            match first_two.fetch_add(1, Ordering::SeqCst) {
                0 => MockHttpResponse::status(500),
                1 => MockHttpResponse::status(409)
                    .with_header("X-Cloud-Database-Epoch", "ffffffffffffffffffffffffffffffff"),
                _ => MockHttpResponse::status(200),
            }
        } else {
            MockHttpResponse::status(200)
        }
    })
    .await;
    let client =
        CloudClient::with_test_base_url(&base_url).expect("mock cloud client should build");
    let library = serde_json::json!({"revision":"cache-epoch-library","items":[]});
    let first = state(1, &library);
    assert!(matches!(
        post_state(&client, &first).await,
        Err(CloudStatePostError::DatabaseEpochChanged { .. })
    ));
    {
        let cache = client
            .acknowledged_state_chunks
            .lock()
            .expect("chunk cache should be available");
        assert!(cache.committed.is_none());
        assert!(cache.staged.is_empty());
    }
    let retry = state(2, &library);
    post_state(&client, &retry)
        .await
        .expect("retry fixture should receive successful chunk and manifest responses");

    let requests = requests
        .lock()
        .expect("mock Worker request log should be available")
        .clone();
    assert_eq!(count_prefix(&requests, "/api/local/state/chunks/"), 4);
    assert_eq!(count_exact_path(&requests, "/api/local/state"), 1);
    server.abort();
}
