use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::Client;
use reqwest::multipart::{Form, Part};
use serde::{Deserialize, Serialize};
use tracing::info;

#[derive(Debug, Clone)]
pub struct TelegramClient {
    client: Client,
    token: String,
}

#[derive(Debug, Deserialize)]
pub struct Update {
    pub update_id: i64,
    pub message: Option<Message>,
}

#[derive(Debug, Deserialize)]
pub struct Message {
    pub chat: Chat,
    pub text: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Chat {
    pub id: i64,
    #[serde(rename = "type")]
    pub kind: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ApiResponse<T> {
    ok: bool,
    result: Option<T>,
    description: Option<String>,
}

#[derive(Debug, Serialize)]
struct SendMessageRequest {
    chat_id: i64,
    text: String,
    disable_web_page_preview: bool,
}

impl TelegramClient {
    pub fn new(token: String) -> Self {
        Self {
            client: Client::new(),
            token,
        }
    }

    pub async fn get_updates(
        &self,
        offset: Option<i64>,
        timeout_seconds: u64,
    ) -> Result<Vec<Update>> {
        let request_timeout = Duration::from_secs(timeout_seconds.saturating_add(10));
        let mut query = vec![("timeout", timeout_seconds.to_string())];
        if let Some(offset) = offset {
            query.push(("offset", offset.to_string()));
        }

        let response = self
            .client
            .get(self.api_url("getUpdates"))
            .query(&query)
            .timeout(request_timeout)
            .send()
            .await
            .map_err(strip_reqwest_url)
            .context("telegram getUpdates request failed")?
            .error_for_status()
            .map_err(strip_reqwest_url)
            .context("telegram getUpdates returned HTTP error")?
            .json::<ApiResponse<Vec<Update>>>()
            .await
            .map_err(strip_reqwest_url)
            .context("failed to decode telegram getUpdates response")?;

        if response.ok {
            Ok(response.result.unwrap_or_default())
        } else {
            bail!(
                "telegram getUpdates failed: {}",
                response
                    .description
                    .unwrap_or_else(|| "unknown error".to_string())
            );
        }
    }

    pub async fn send_message(&self, chat_id: i64, text: String) -> Result<()> {
        info!(
            chat_id,
            text = %redact_sensitive_text(&text),
            "telegram outbound message"
        );
        let payload = SendMessageRequest {
            chat_id,
            text,
            disable_web_page_preview: true,
        };

        let response = self
            .client
            .post(self.api_url("sendMessage"))
            .json(&payload)
            .timeout(Duration::from_secs(15))
            .send()
            .await
            .map_err(strip_reqwest_url)
            .context("telegram sendMessage request failed")?
            .error_for_status()
            .map_err(strip_reqwest_url)
            .context("telegram sendMessage returned HTTP error")?
            .json::<ApiResponse<serde::de::IgnoredAny>>()
            .await
            .map_err(strip_reqwest_url)
            .context("failed to decode telegram sendMessage response")?;

        if response.ok {
            Ok(())
        } else {
            bail!(
                "telegram sendMessage failed: {}",
                response
                    .description
                    .unwrap_or_else(|| "unknown error".to_string())
            );
        }
    }

    pub async fn send_photo(&self, chat_id: i64, caption: String, png: Vec<u8>) -> Result<()> {
        info!(
            chat_id,
            caption = %redact_sensitive_text(&caption),
            image_bytes = png.len(),
            "telegram outbound photo"
        );
        let photo = Part::bytes(png)
            .file_name("bbdown-login.png")
            .mime_str("image/png")
            .context("failed to build Telegram photo part")?;
        let form = Form::new()
            .text("chat_id", chat_id.to_string())
            .text("caption", caption)
            .part("photo", photo);

        let response = self
            .client
            .post(self.api_url("sendPhoto"))
            .multipart(form)
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .map_err(strip_reqwest_url)
            .context("telegram sendPhoto request failed")?
            .error_for_status()
            .map_err(strip_reqwest_url)
            .context("telegram sendPhoto returned HTTP error")?
            .json::<ApiResponse<serde::de::IgnoredAny>>()
            .await
            .map_err(strip_reqwest_url)
            .context("failed to decode telegram sendPhoto response")?;

        if response.ok {
            Ok(())
        } else {
            bail!(
                "telegram sendPhoto failed: {}",
                response
                    .description
                    .unwrap_or_else(|| "unknown error".to_string())
            );
        }
    }

    fn api_url(&self, method: &str) -> String {
        format!("https://api.telegram.org/bot{}/{method}", self.token)
    }
}

impl Chat {
    pub fn is_private(&self) -> bool {
        self.kind.as_deref() == Some("private")
    }
}

fn strip_reqwest_url(error: reqwest::Error) -> reqwest::Error {
    error.without_url()
}

fn redact_sensitive_text(text: &str) -> String {
    let mut redacted = String::with_capacity(text.len());
    for line in text.lines() {
        if !redacted.is_empty() {
            redacted.push('\n');
        }
        if line.contains("passport.bilibili.com") && line.contains("qrcode_key=") {
            redacted.push_str("<redacted Bilibili login QR URL>");
        } else {
            redacted.push_str(line);
        }
    }
    redacted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_private_chats() {
        assert!(
            Chat {
                id: 1,
                kind: Some("private".to_string())
            }
            .is_private()
        );
        assert!(
            !Chat {
                id: 1,
                kind: Some("group".to_string())
            }
            .is_private()
        );
        assert!(!Chat { id: 1, kind: None }.is_private());
    }

    #[test]
    fn redacts_bilibili_login_qr_urls() {
        assert_eq!(
            redact_sensitive_text(
                "Open:\nhttps://passport.bilibili.com/h5-app/passport/login/scan?qrcode_key=secret"
            ),
            "Open:\n<redacted Bilibili login QR URL>"
        );
        assert_eq!(
            redact_sensitive_text("https://www.bilibili.com/video/BV123"),
            "https://www.bilibili.com/video/BV123"
        );
    }
}
