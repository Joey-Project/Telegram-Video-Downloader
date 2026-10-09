use std::fmt;
use std::time::Duration;

use anyhow::{Result, anyhow, bail, ensure};
use bbdown_core::{Credentials, QrLoginState};
use reqwest::{Client, Response, redirect::Policy};
use serde::{Deserialize, de::DeserializeOwned};
use url::Url;

const AUTH_BASE: &str = "https://www.biliplus.com";
const RESPONSE_LIMIT: usize = 64 * 1024;

pub struct AccessKeyQrClient {
    client: Client,
    login_url: Url,
}

pub struct AccessKeyQrTicket {
    url: String,
    auth_code: String,
}

impl AccessKeyQrTicket {
    pub fn url(&self) -> &str {
        &self.url
    }
}

impl fmt::Debug for AccessKeyQrTicket {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AccessKeyQrTicket")
            .field("url", &"<redacted>")
            .field("auth_code", &"<redacted>")
            .finish()
    }
}

#[derive(Deserialize)]
struct ApiResponse {
    code: i64,
    data: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct TicketData {
    url: String,
    auth_code: String,
}

#[derive(Deserialize)]
struct PollData {
    token_info: TokenInfo,
}

#[derive(Deserialize)]
struct TokenInfo {
    access_token: String,
}

impl AccessKeyQrClient {
    pub fn new(timeout: Duration) -> Result<Self> {
        Self::with_base(AUTH_BASE, timeout)
    }

    fn with_base(base: &str, timeout: Duration) -> Result<Self> {
        Ok(Self {
            client: Client::builder()
                .redirect(Policy::none())
                .timeout(timeout)
                .user_agent("Mozilla/5.0")
                .build()
                .map_err(|_| anyhow!("failed to create BiliPlus authorization client"))?,
            login_url: Url::parse(base)?.join("/login")?,
        })
    }

    pub async fn create_ticket(&self) -> Result<AccessKeyQrTicket> {
        let response = self
            .client
            .get(self.login_url.clone())
            .query(&[("act", "getauth")])
            .send()
            .await
            .map_err(|_| anyhow!("BiliPlus access-key QR request failed"))?;
        let result: ApiResponse = decode_response(response).await?;
        ensure!(
            result.code == 0,
            "BiliPlus access-key QR request returned code {}",
            result.code
        );
        let data: TicketData = serde_json::from_value(
            result
                .data
                .ok_or_else(|| anyhow!("BiliPlus access-key QR response omitted ticket data"))?,
        )
        .map_err(|_| anyhow!("BiliPlus access-key QR response contained invalid ticket data"))?;
        ensure!(
            !data.auth_code.trim().is_empty(),
            "BiliPlus access-key QR response omitted the authorization code"
        );
        let url = Url::parse(&data.url)
            .map_err(|_| anyhow!("BiliPlus access-key QR response contained an invalid URL"))?;
        ensure!(
            url.scheme() == "https"
                && url.host_str() == Some("passport.bilibili.com")
                && url.port_or_known_default() == Some(443)
                && url.username().is_empty()
                && url.password().is_none(),
            "BiliPlus access-key QR response used an unexpected authorization origin"
        );
        Ok(AccessKeyQrTicket {
            url: data.url,
            auth_code: data.auth_code,
        })
    }

    pub async fn poll(&self, ticket: &AccessKeyQrTicket) -> Result<QrLoginState> {
        let response = self
            .client
            .post(self.login_url.clone())
            .query(&[("act", "authpoll")])
            .form(&[("auth_code", ticket.auth_code.as_str())])
            .send()
            .await
            .map_err(|_| anyhow!("BiliPlus access-key QR poll request failed"))?;
        let result: ApiResponse = decode_response(response).await?;
        match result.code {
            86039 => Ok(QrLoginState::WaitingForScan),
            86090 => Ok(QrLoginState::WaitingForConfirm),
            86038 => Ok(QrLoginState::Expired),
            0 => {
                let data: PollData = serde_json::from_value(result.data.ok_or_else(|| {
                    anyhow!("BiliPlus access-key QR response omitted token data")
                })?)
                .map_err(|_| {
                    anyhow!("BiliPlus access-key QR response contained invalid token data")
                })?;
                let token = data.token_info.access_token;
                ensure!(
                    !token.trim().is_empty(),
                    "BiliPlus access-key QR response omitted the access key"
                );
                Ok(QrLoginState::Succeeded {
                    credentials: Credentials::default().with_access_key(token),
                })
            }
            code => bail!("BiliPlus access-key QR poll returned code {code}"),
        }
    }
}

async fn decode_response<T: DeserializeOwned>(mut response: Response) -> Result<T> {
    ensure!(
        response.status().is_success(),
        "BiliPlus authorization returned HTTP {}",
        response.status().as_u16()
    );
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| anyhow!("failed to read BiliPlus authorization response"))?
    {
        ensure!(
            chunk.len() <= RESPONSE_LIMIT.saturating_sub(body.len()),
            "BiliPlus authorization response exceeded the size limit"
        );
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body)
        .map_err(|_| anyhow!("BiliPlus authorization returned invalid response data"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    // synthetic-token-fixtures: joey-private-v3 / access-a.
    const ACCESS_TOKEN: &str = "codex_synth_v1_access_a";

    async fn server(
        responses: Vec<(u16, String)>,
    ) -> (AccessKeyQrClient, tokio::task::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let mut requests = Vec::new();
            for (status, body) in responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut buffer = [0; 1024];
                loop {
                    let size = stream.read(&mut buffer).await.unwrap();
                    assert!(size > 0);
                    request.extend_from_slice(&buffer[..size]);
                    let Some(header_end) = request.windows(4).position(|part| part == b"\r\n\r\n")
                    else {
                        continue;
                    };
                    let headers = String::from_utf8_lossy(&request[..header_end]).to_lowercase();
                    let body_size = headers
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length:"))
                        .map(|value| value.trim().parse::<usize>().unwrap())
                        .unwrap_or(0);
                    if request.len() >= header_end + 4 + body_size {
                        break;
                    }
                }
                requests.push(String::from_utf8(request).unwrap());
                let location = if status == 302 {
                    "Location: /unexpected\r\n"
                } else {
                    ""
                };
                let response = format!(
                    "HTTP/1.1 {status} Test\r\n{location}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
            requests
        });
        (
            AccessKeyQrClient::with_base(&base, Duration::from_secs(2)).unwrap(),
            task,
        )
    }

    fn ticket_response(url: &str) -> String {
        serde_json::json!({"code":0,"data":{"url":url,"auth_code":ACCESS_TOKEN}}).to_string()
    }

    #[tokio::test]
    async fn access_key_qr_poll_saves_generic_credentials_without_a_callback() {
        let responses = vec![
            (
                200,
                ticket_response("https://passport.bilibili.com/login?test=1"),
            ),
            (200, r#"{"code":86039}"#.into()),
            (200, r#"{"code":86090,"data":{}}"#.into()),
            (
                200,
                serde_json::json!({"code":0,"data":{"token_info":{"access_token":ACCESS_TOKEN}}})
                    .to_string(),
            ),
        ];
        let (client, task) = server(responses).await;
        let ticket = client.create_ticket().await.unwrap();
        assert_eq!(ticket.url(), "https://passport.bilibili.com/login?test=1");
        assert!(!format!("{ticket:?}").contains(ACCESS_TOKEN));
        assert!(matches!(
            client.poll(&ticket).await.unwrap(),
            QrLoginState::WaitingForScan
        ));
        assert!(matches!(
            client.poll(&ticket).await.unwrap(),
            QrLoginState::WaitingForConfirm
        ));
        let QrLoginState::Succeeded { credentials } = client.poll(&ticket).await.unwrap() else {
            panic!("expected a successful QR login");
        };
        assert_eq!(credentials.access_key.as_deref(), Some(ACCESS_TOKEN));
        assert!(credentials.cookie.is_none());
        assert!(credentials.tv_access_key.is_none());
        let requests = task.await.unwrap();
        assert!(requests[0].starts_with("GET /login?act=getauth "));
        for request in &requests[1..] {
            assert!(request.starts_with("POST /login?act=authpoll "));
            assert!(!request.lines().next().unwrap().contains(ACCESS_TOKEN));
            assert!(request.ends_with(&format!("auth_code={ACCESS_TOKEN}")));
            assert!(!request.to_lowercase().contains("\r\ncookie:"));
        }
    }

    #[tokio::test]
    async fn access_key_qr_rejects_untrusted_qr_origins() {
        for url in [
            "https://untrusted.invalid/login",
            "http://passport.bilibili.com/login",
            "https://user@passport.bilibili.com/login",
        ] {
            let (client, task) = server(vec![(200, ticket_response(url))]).await;
            let error = client.create_ticket().await.unwrap_err().to_string();
            assert!(error.contains("unexpected authorization origin"));
            assert!(!error.contains(url));
            assert!(!error.contains(ACCESS_TOKEN));
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn access_key_qr_handles_expiry_and_redacts_provider_errors() {
        let responses = vec![
            (200, ticket_response("https://passport.bilibili.com/login")),
            (200, r#"{"code":86038}"#.into()),
            (
                200,
                serde_json::json!({"code":-1,"message":ACCESS_TOKEN}).to_string(),
            ),
            (
                200,
                r#"{"code":0,"data":{"token_info":{"access_token":""}}}"#.into(),
            ),
        ];
        let (client, task) = server(responses).await;
        let ticket = client.create_ticket().await.unwrap();
        assert!(matches!(
            client.poll(&ticket).await.unwrap(),
            QrLoginState::Expired
        ));
        let error = client.poll(&ticket).await.unwrap_err().to_string();
        assert!(error.contains("code -1"));
        assert!(!error.contains(ACCESS_TOKEN));
        assert!(client.poll(&ticket).await.is_err());
        task.await.unwrap();
    }

    #[tokio::test]
    async fn access_key_qr_rejects_malformed_and_oversized_responses() {
        for body in [ACCESS_TOKEN.to_string(), "x".repeat(RESPONSE_LIMIT + 1)] {
            let (client, task) = server(vec![(200, body)]).await;
            let error = client.create_ticket().await.unwrap_err().to_string();
            assert!(error.contains("invalid response data") || error.contains("size limit"));
            assert!(!error.contains(ACCESS_TOKEN));
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn access_key_qr_does_not_follow_provider_redirects() {
        let (client, task) = server(vec![(302, ACCESS_TOKEN.to_string())]).await;
        let error = client.create_ticket().await.unwrap_err().to_string();
        assert!(error.contains("HTTP 302"));
        assert!(!error.contains(ACCESS_TOKEN));
        task.await.unwrap();
    }
}
