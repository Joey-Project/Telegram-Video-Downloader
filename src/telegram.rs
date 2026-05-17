use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::Client;
use serde::{Deserialize, Serialize};

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

    fn api_url(&self, method: &str) -> String {
        format!("https://api.telegram.org/bot{}/{method}", self.token)
    }
}

fn strip_reqwest_url(error: reqwest::Error) -> reqwest::Error {
    error.without_url()
}
