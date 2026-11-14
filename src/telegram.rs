use std::time::Duration;

use anyhow::{Context, Result, bail};
#[cfg(test)]
use flate2::Compression;
#[cfg(test)]
use flate2::write::ZlibEncoder;
use reqwest::Client;
use reqwest::multipart::{Form, Part};
use reqwest::redirect::Policy;
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::io::Write;
use tracing::info;

use crate::redaction::redact_sensitive_text;

#[derive(Debug, Clone)]
pub struct TelegramClient {
    api_base_url: String,
    client: Client,
    file_client: Client,
    token: String,
}

#[cfg(test)]
pub const MAX_WIDGET_HTML_BYTES: usize = 32_768;
#[cfg(test)]
pub const MAX_WIDGET_COMPRESSED_HTML_BYTES: usize = 4_096;
pub const MAX_WIDGET_DATA_BYTES: usize = 1_024;
pub const MAX_SMALL_TELEGRAM_FILE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutboundTextPolicy {
    RedactCredentials,
    AllowAuthSecret,
}

#[derive(Debug, Deserialize)]
pub struct Update {
    pub update_id: i64,
    pub message: Option<Message>,
    pub callback_query: Option<CallbackQuery>,
    pub widget_query: Option<WidgetQuery>,
}

#[derive(Debug, Deserialize)]
pub struct Message {
    pub message_id: i64,
    pub chat: Chat,
    pub text: Option<String>,
    pub from: Option<User>,
    pub audio: Option<Audio>,
    pub document: Option<Document>,
}

#[derive(Debug, Deserialize)]
pub struct Audio {
    pub file_id: String,
    pub file_unique_id: String,
    pub file_size: Option<u64>,
    pub file_name: Option<String>,
    pub mime_type: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Document {
    pub file_id: String,
    pub file_unique_id: String,
    pub file_size: Option<u64>,
    pub file_name: Option<String>,
    pub mime_type: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct User {
    pub id: i64,
}

#[derive(Debug, Deserialize)]
pub struct Chat {
    pub id: i64,
    #[serde(rename = "type")]
    pub kind: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CallbackQuery {
    pub id: String,
    #[serde(default)]
    pub from: Option<User>,
    pub message: Option<Message>,
    pub data: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct WidgetQuery {
    pub id: String,
    pub from: User,
    pub chat: Option<Chat>,
    pub message_id: Option<i64>,
    pub inline_message_id: Option<String>,
    pub chat_instance: Option<String>,
    pub chat_type: Option<String>,
    #[serde(rename = "type")]
    pub r#type: WidgetQueryType,
    pub data: String,
    pub widget_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct WidgetQueryType {
    #[serde(rename = "type")]
    pub kind: String,
}

#[derive(Debug, Deserialize)]
struct TelegramFile {
    file_size: Option<u64>,
    file_path: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_markup: Option<InlineKeyboardMarkup>,
}

#[derive(Debug, Serialize)]
struct EditMessageTextRequest {
    chat_id: i64,
    message_id: i64,
    text: String,
    disable_web_page_preview: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_markup: Option<InlineKeyboardMarkup>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct BotCommand {
    pub command: String,
    pub description: String,
}

#[derive(Debug, Serialize)]
struct SetMyCommandsRequest {
    commands: Vec<BotCommand>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct InlineKeyboardMarkup {
    pub inline_keyboard: Vec<Vec<InlineKeyboardButton>>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct InlineKeyboardButton {
    pub text: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub callback_data: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub web_app: Option<WebAppInfo>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct WebAppInfo {
    pub url: String,
}

#[derive(Debug, Serialize)]
struct AnswerCallbackQueryRequest {
    callback_query_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    show_alert: bool,
}

#[derive(Debug, Serialize)]
#[cfg(test)]
struct SendWidgetRequest<'a> {
    chat_id: i64,
    html: &'a str,
    chat_data: &'a str,
    caption: &'a str,
    allow_queries: bool,
}

#[derive(Debug, Serialize)]
#[cfg(test)]
struct EditMessageWidgetDataRequest<'a> {
    chat_id: i64,
    message_id: i64,
    chat_data: &'a str,
}

#[derive(Debug, Serialize)]
#[cfg(test)]
struct EditMessageCaptionRequest<'a> {
    chat_id: i64,
    message_id: i64,
    caption: &'a str,
}

#[derive(Debug, Serialize)]
struct AnswerWidgetQueryRequest<'a> {
    widget_query_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<&'a str>,
}

#[derive(Debug, Serialize)]
struct GetFileRequest<'a> {
    file_id: &'a str,
}

impl TelegramClient {
    pub fn new(token: String) -> Self {
        Self::with_api_base_url(token, "https://api.telegram.org".to_string())
    }

    #[cfg(test)]
    pub(crate) fn with_test_api_base_url(token: String, api_base_url: String) -> Self {
        Self::with_api_base_url(token, api_base_url)
    }

    fn with_api_base_url(token: String, api_base_url: String) -> Self {
        Self {
            api_base_url: api_base_url.trim_end_matches('/').to_string(),
            client: Client::new(),
            file_client: Client::builder()
                .redirect(Policy::none())
                .build()
                .expect("static no-redirect Telegram file client configuration is valid"),
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

    pub async fn send_message(&self, chat_id: i64, text: String) -> Result<i64> {
        self.send_message_payload(chat_id, text, None, OutboundTextPolicy::RedactCredentials)
            .await
    }

    pub async fn send_auth_secret_message(&self, chat_id: i64, text: String) -> Result<i64> {
        self.send_message_payload(chat_id, text, None, OutboundTextPolicy::AllowAuthSecret)
            .await
    }

    pub async fn send_message_with_inline_keyboard(
        &self,
        chat_id: i64,
        text: String,
        reply_markup: InlineKeyboardMarkup,
    ) -> Result<i64> {
        self.send_message_payload(
            chat_id,
            text,
            Some(reply_markup),
            OutboundTextPolicy::RedactCredentials,
        )
        .await
    }

    /// Sends a static HTML widget in the private chat and enables tap requests.
    ///
    /// Telegram fixes widget HTML when the message is created, so both documented
    /// UTF-8 and zlib level-9 limits are checked before any request is sent.
    #[cfg(test)]
    pub async fn send_widget(
        &self,
        chat_id: i64,
        html: &str,
        chat_data: &str,
        caption: &str,
    ) -> Result<i64> {
        validate_widget_html(html)?;
        validate_widget_data(chat_data, "chat_data")?;
        if caption.chars().count() > 1_024 {
            bail!("telegram widget caption exceeds 1024 characters");
        }
        if caption.contains('\0') {
            bail!("telegram widget caption must not contain NUL");
        }

        let payload = SendWidgetRequest {
            chat_id,
            html,
            chat_data,
            caption,
            allow_queries: true,
        };
        let response = self
            .client
            .post(self.api_url("sendWidget"))
            .json(&payload)
            .timeout(Duration::from_secs(15))
            .send()
            .await
            .map_err(strip_reqwest_url)
            .context("telegram sendWidget request failed")?
            .error_for_status()
            .map_err(strip_reqwest_url)
            .context("telegram sendWidget returned HTTP error")?
            .json::<ApiResponse<Message>>()
            .await
            .map_err(strip_reqwest_url)
            .context("failed to decode telegram sendWidget response")?;

        telegram_required_result(response, "sendWidget").map(|message| message.message_id)
    }

    /// Replaces the chat-local state of an existing static widget message.
    #[cfg(test)]
    pub async fn edit_widget_data(
        &self,
        chat_id: i64,
        message_id: i64,
        chat_data: &str,
    ) -> Result<()> {
        validate_widget_data(chat_data, "chat_data")?;
        let payload = EditMessageWidgetDataRequest {
            chat_id,
            message_id,
            chat_data,
        };
        let response = self
            .client
            .post(self.api_url("editMessageWidgetData"))
            .json(&payload)
            .timeout(Duration::from_secs(15))
            .send()
            .await
            .map_err(strip_reqwest_url)
            .context("telegram editMessageWidgetData request failed")?;
        let status = response.status();
        let response = response
            .json::<ApiResponse<serde::de::IgnoredAny>>()
            .await
            .map_err(strip_reqwest_url)
            .context("failed to decode telegram editMessageWidgetData response")?;

        telegram_edit_result_for_method(status, response, "editMessageWidgetData")
    }

    /// Keeps unsupported-client and transcript captions current alongside widget state.
    #[cfg(test)]
    pub async fn edit_widget_caption(
        &self,
        chat_id: i64,
        message_id: i64,
        caption: &str,
    ) -> Result<()> {
        if caption.chars().count() > 1_024 || caption.contains('\0') {
            bail!("telegram widget caption must be at most 1024 characters and contain no NUL");
        }
        let response = self
            .client
            .post(self.api_url("editMessageCaption"))
            .json(&EditMessageCaptionRequest {
                chat_id,
                message_id,
                caption,
            })
            .timeout(Duration::from_secs(15))
            .send()
            .await
            .map_err(strip_reqwest_url)
            .context("telegram editMessageCaption request failed")?;
        let status = response.status();
        let response = response
            .json::<ApiResponse<serde::de::IgnoredAny>>()
            .await
            .map_err(strip_reqwest_url)
            .context("failed to decode telegram editMessageCaption response")?;

        telegram_edit_result_for_method(status, response, "editMessageCaption")
    }

    /// Answers one widget request. Callers should invoke this even when rejecting
    /// a request so Telegram does not leave the widget waiting for its 15-second deadline.
    pub async fn answer_widget_query(&self, query_id: &str, data: Option<&str>) -> Result<()> {
        if query_id.trim().is_empty() {
            bail!("telegram widget query id must not be empty");
        }
        if let Some(data) = data {
            validate_widget_data(data, "widget query answer data")?;
        }
        let payload = AnswerWidgetQueryRequest {
            widget_query_id: query_id,
            data,
        };
        let response = self
            .client
            .post(self.api_url("answerWidgetQuery"))
            .json(&payload)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .map_err(strip_reqwest_url)
            .context("telegram answerWidgetQuery request failed")?
            .error_for_status()
            .map_err(strip_reqwest_url)
            .context("telegram answerWidgetQuery returned HTTP error")?
            .json::<ApiResponse<serde::de::IgnoredAny>>()
            .await
            .map_err(strip_reqwest_url)
            .context("failed to decode telegram answerWidgetQuery response")?;

        telegram_optional_result(response, "answerWidgetQuery")
    }

    /// Downloads a Telegram-hosted file with a caller-selected bound no larger
    /// than the torrent feature's 4 MiB admission limit.
    pub async fn download_small_file(&self, file_id: &str, max_bytes: usize) -> Result<Vec<u8>> {
        if file_id.trim().is_empty() {
            bail!("telegram file id must not be empty");
        }
        if max_bytes == 0 || max_bytes > MAX_SMALL_TELEGRAM_FILE_BYTES {
            bail!("telegram small-file limit must be between 1 and 4 MiB");
        }

        let file = self
            .file_client
            .post(self.api_url("getFile"))
            .json(&GetFileRequest { file_id })
            .timeout(Duration::from_secs(15))
            .send()
            .await
            .map_err(strip_reqwest_url)
            .context("telegram getFile request failed")?
            .error_for_status()
            .map_err(strip_reqwest_url)
            .context("telegram getFile returned HTTP error")?
            .json::<ApiResponse<TelegramFile>>()
            .await
            .map_err(strip_reqwest_url)
            .context("failed to decode telegram getFile response")?;
        let file = telegram_required_result(file, "getFile")?;
        if file.file_size.is_some_and(|size| size > max_bytes as u64) {
            bail!("telegram file exceeds the configured download limit");
        }
        let path = file
            .file_path
            .as_deref()
            .context("telegram getFile response did not include a relative file_path")?;
        validate_telegram_file_path(path)?;

        let url = format!("{}/file/bot{}/{path}", self.api_base_url, self.token);
        let mut response = self
            .file_client
            .get(url)
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .map_err(strip_reqwest_url)
            .context("telegram file download request failed")?;
        if !response.status().is_success() {
            // Redirects are intentionally rejected. The bot token is part of the
            // URL path and must never be forwarded to a different host.
            bail!("telegram file download returned HTTP {}", response.status());
        }
        if response
            .content_length()
            .is_some_and(|length| length > max_bytes as u64)
        {
            bail!("telegram file exceeds the configured download limit");
        }

        let capacity = file.file_size.unwrap_or_default().min(max_bytes as u64) as usize;
        let mut body = Vec::with_capacity(capacity);
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(strip_reqwest_url)
            .context("failed to stream telegram file download")?
        {
            if body.len().saturating_add(chunk.len()) > max_bytes {
                bail!("telegram file exceeds the configured download limit");
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }

    async fn send_message_payload(
        &self,
        chat_id: i64,
        text: String,
        reply_markup: Option<InlineKeyboardMarkup>,
        policy: OutboundTextPolicy,
    ) -> Result<i64> {
        let text = prepare_outbound_text(text, policy);
        info!(
            chat_id,
            text = %outbound_log_text(&text, policy),
            "telegram outbound message"
        );
        let payload = SendMessageRequest {
            chat_id,
            text,
            disable_web_page_preview: true,
            reply_markup,
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
            .json::<ApiResponse<Message>>()
            .await
            .map_err(strip_reqwest_url)
            .context("failed to decode telegram sendMessage response")?;

        telegram_required_result(response, "sendMessage").map(|message| message.message_id)
    }

    pub async fn edit_message_text(
        &self,
        chat_id: i64,
        message_id: i64,
        text: String,
    ) -> Result<()> {
        self.edit_message_text_payload(chat_id, message_id, text, None)
            .await
    }

    pub async fn edit_message_text_with_inline_keyboard(
        &self,
        chat_id: i64,
        message_id: i64,
        text: String,
        reply_markup: InlineKeyboardMarkup,
    ) -> Result<()> {
        self.edit_message_text_payload(chat_id, message_id, text, Some(reply_markup))
            .await
    }

    pub async fn edit_message_text_without_inline_keyboard(
        &self,
        chat_id: i64,
        message_id: i64,
        text: String,
    ) -> Result<()> {
        self.edit_message_text_payload(
            chat_id,
            message_id,
            text,
            Some(InlineKeyboardMarkup {
                inline_keyboard: Vec::new(),
            }),
        )
        .await
    }

    async fn edit_message_text_payload(
        &self,
        chat_id: i64,
        message_id: i64,
        text: String,
        reply_markup: Option<InlineKeyboardMarkup>,
    ) -> Result<()> {
        let text = redact_sensitive_text(&text);
        info!(
            chat_id,
            message_id,
            text = %redact_sensitive_text(&text),
            "telegram outbound message edit"
        );
        let payload = EditMessageTextRequest {
            chat_id,
            message_id,
            text,
            disable_web_page_preview: true,
            reply_markup,
        };

        let response = self
            .client
            .post(self.api_url("editMessageText"))
            .json(&payload)
            .timeout(Duration::from_secs(15))
            .send()
            .await
            .map_err(strip_reqwest_url)
            .context("telegram editMessageText request failed")?;
        let status = response.status();
        let response = response
            .json::<ApiResponse<serde::de::IgnoredAny>>()
            .await
            .map_err(strip_reqwest_url)
            .context("failed to decode telegram editMessageText response")?;

        telegram_edit_result(status, response)
    }

    pub async fn set_my_commands(&self, commands: Vec<BotCommand>) -> Result<()> {
        let payload = SetMyCommandsRequest { commands };
        let response = self
            .client
            .post(self.api_url("setMyCommands"))
            .json(&payload)
            .timeout(Duration::from_secs(15))
            .send()
            .await
            .map_err(strip_reqwest_url)
            .context("telegram setMyCommands request failed")?
            .error_for_status()
            .map_err(strip_reqwest_url)
            .context("telegram setMyCommands returned HTTP error")?
            .json::<ApiResponse<serde::de::IgnoredAny>>()
            .await
            .map_err(strip_reqwest_url)
            .context("failed to decode telegram setMyCommands response")?;

        telegram_optional_result(response, "setMyCommands")
    }

    pub async fn answer_callback_query(
        &self,
        callback_query_id: String,
        text: String,
    ) -> Result<()> {
        let text = redact_sensitive_text(&text);
        let payload = AnswerCallbackQueryRequest {
            callback_query_id,
            text: (!text.trim().is_empty()).then_some(text),
            show_alert: false,
        };
        let response = self
            .client
            .post(self.api_url("answerCallbackQuery"))
            .json(&payload)
            .timeout(Duration::from_secs(15))
            .send()
            .await
            .map_err(strip_reqwest_url)
            .context("telegram answerCallbackQuery request failed")?
            .error_for_status()
            .map_err(strip_reqwest_url)
            .context("telegram answerCallbackQuery returned HTTP error")?
            .json::<ApiResponse<serde::de::IgnoredAny>>()
            .await
            .map_err(strip_reqwest_url)
            .context("failed to decode telegram answerCallbackQuery response")?;

        telegram_optional_result(response, "answerCallbackQuery")
    }

    pub async fn send_photo(&self, chat_id: i64, caption: String, png: Vec<u8>) -> Result<()> {
        let caption = redact_sensitive_text(&caption);
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

        telegram_optional_result(response, "sendPhoto")
    }

    fn api_url(&self, method: &str) -> String {
        format!("{}/bot{}/{method}", self.api_base_url, self.token)
    }
}

fn telegram_required_result<T>(response: ApiResponse<T>, method: &str) -> Result<T> {
    if response.ok {
        response
            .result
            .with_context(|| format!("telegram {method} response did not include result"))
    } else {
        bail!(
            "telegram {method} failed: {}",
            response
                .description
                .unwrap_or_else(|| "unknown error".to_string())
        );
    }
}

fn telegram_optional_result<T>(response: ApiResponse<T>, method: &str) -> Result<()> {
    if response.ok {
        Ok(())
    } else {
        bail!(
            "telegram {method} failed: {}",
            response
                .description
                .unwrap_or_else(|| "unknown error".to_string())
        );
    }
}

fn telegram_edit_result<T>(status: reqwest::StatusCode, response: ApiResponse<T>) -> Result<()> {
    telegram_edit_result_for_method(status, response, "editMessageText")
}

fn telegram_edit_result_for_method<T>(
    status: reqwest::StatusCode,
    response: ApiResponse<T>,
    method: &str,
) -> Result<()> {
    // An earlier edit can succeed even if its response is lost. Repeating the
    // same edit is safe; Telegram reports that the desired text already exists.
    if status == reqwest::StatusCode::BAD_REQUEST
        && !response.ok
        && response.description.as_deref().is_some_and(|description| {
            let description = description.to_ascii_lowercase();
            description == "message is not modified"
                || description.starts_with("bad request: message is not modified")
        })
    {
        return Ok(());
    }
    if !status.is_success() {
        bail!("telegram {method} returned HTTP {status}");
    }
    telegram_optional_result(response, method)
}

#[cfg(test)]
fn validate_widget_html(html: &str) -> Result<()> {
    let byte_len = html.len();
    if byte_len == 0 || byte_len > MAX_WIDGET_HTML_BYTES {
        bail!("telegram widget HTML must be between 1 and 32768 UTF-8 bytes");
    }
    if html.contains('\0') {
        bail!("telegram widget HTML must not contain NUL");
    }
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::best());
    encoder
        .write_all(html.as_bytes())
        .context("failed to measure compressed telegram widget HTML")?;
    let compressed_len = encoder
        .finish()
        .context("failed to finish compressed telegram widget HTML size check")?
        .len();
    if compressed_len > MAX_WIDGET_COMPRESSED_HTML_BYTES {
        bail!("telegram widget HTML exceeds 4096 bytes after zlib level-9 compression");
    }
    Ok(())
}

fn validate_widget_data(data: &str, field: &str) -> Result<()> {
    if data.len() > MAX_WIDGET_DATA_BYTES {
        bail!("telegram {field} exceeds 1024 UTF-8 bytes");
    }
    if data.contains('\0') {
        bail!("telegram {field} must not contain NUL");
    }
    Ok(())
}

fn validate_telegram_file_path(path: &str) -> Result<()> {
    if path.is_empty()
        || path.starts_with('/')
        || path.contains('\\')
        || !path.split('/').all(|component| {
            !component.is_empty()
                && component != "."
                && component != ".."
                && component
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        })
    {
        bail!("telegram getFile returned an unsafe relative file_path");
    }
    Ok(())
}

impl Chat {
    pub fn is_private(&self) -> bool {
        self.kind.as_deref() == Some("private")
    }
}

fn strip_reqwest_url(error: reqwest::Error) -> reqwest::Error {
    error.without_url()
}

fn prepare_outbound_text(text: String, policy: OutboundTextPolicy) -> String {
    match policy {
        OutboundTextPolicy::RedactCredentials => redact_sensitive_text(&text),
        OutboundTextPolicy::AllowAuthSecret => text,
    }
}

fn outbound_log_text(text: &str, policy: OutboundTextPolicy) -> String {
    match policy {
        OutboundTextPolicy::RedactCredentials => redact_sensitive_text(text),
        OutboundTextPolicy::AllowAuthSecret => "<redacted authorization message>".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    const SYNTHETIC_TELEGRAM_API_KEY: &str = "codex_synth_v1_api_key_a"; // synthetic-token-fixtures: joey-private-v3 / api-key-a, active

    struct MockReply {
        status: &'static str,
        headers: &'static str,
        body: Vec<u8>,
        close_delimited: bool,
    }

    impl MockReply {
        fn json(value: Value) -> Self {
            Self {
                status: "200 OK",
                headers: "content-type: application/json\r\n",
                body: value.to_string().into_bytes(),
                close_delimited: false,
            }
        }

        fn bytes(body: &[u8]) -> Self {
            Self {
                status: "200 OK",
                headers: "content-type: application/octet-stream\r\n",
                body: body.to_vec(),
                close_delimited: false,
            }
        }

        fn close_delimited_bytes(body: &[u8]) -> Self {
            Self {
                status: "200 OK",
                headers: "content-type: application/octet-stream\r\n",
                body: body.to_vec(),
                close_delimited: true,
            }
        }

        fn redirect() -> Self {
            Self {
                status: "302 Found",
                headers: "location: http://127.0.0.1:1/token-leak\r\n",
                body: Vec::new(),
                close_delimited: false,
            }
        }
    }

    async fn fake_api(
        replies: Vec<MockReply>,
    ) -> (
        TelegramClient,
        tokio::task::JoinHandle<Vec<(String, Value)>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            for reply in replies {
                let (mut stream, _) = listener.accept().await.unwrap();
                let (path, body) = read_mock_request(&mut stream).await.unwrap();
                requests.push((path, serde_json::from_slice(&body).unwrap_or(Value::Null)));
                let mut headers = format!(
                    "HTTP/1.1 {}\r\n{}connection: close\r\n",
                    reply.status, reply.headers
                );
                // A close-delimited test body exercises the streaming cap without
                // letting Content-Length reject it before the body is consumed.
                if reply.close_delimited {
                    headers.push_str("\r\n");
                } else {
                    headers.push_str(&format!("content-length: {}\r\n\r\n", reply.body.len()));
                }
                stream.write_all(headers.as_bytes()).await.unwrap();
                stream.write_all(&reply.body).await.unwrap();
            }
            requests
        });
        (
            TelegramClient::with_test_api_base_url(
                SYNTHETIC_TELEGRAM_API_KEY.to_string(),
                format!("http://{address}"),
            ),
            server,
        )
    }

    async fn read_mock_request(stream: &mut TcpStream) -> std::io::Result<(String, Vec<u8>)> {
        let mut request = Vec::new();
        let mut header_end = None;
        let mut content_length = 0usize;
        loop {
            let mut chunk = [0; 4096];
            let read = stream.read(&mut chunk).await?;
            if read == 0 {
                break;
            }
            request.extend_from_slice(&chunk[..read]);
            if header_end.is_none()
                && let Some(index) = request.windows(4).position(|window| window == b"\r\n\r\n")
            {
                let headers = String::from_utf8_lossy(&request[..index]);
                content_length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        (name.eq_ignore_ascii_case("content-length"))
                            .then(|| value.trim().parse().ok())
                            .flatten()
                    })
                    .unwrap_or_default();
                header_end = Some(index + 4);
            }
            if header_end.is_some_and(|index| request.len() >= index + content_length) {
                break;
            }
        }
        let header_end =
            header_end.ok_or_else(|| std::io::Error::other("missing HTTP request headers"))?;
        let headers = String::from_utf8_lossy(&request[..header_end]);
        let path = headers
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .unwrap_or_default()
            .to_string();
        Ok((path, request[header_end..].to_vec()))
    }

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
        assert_eq!(
            redact_sensitive_text(
                "https://www.biliplus.com/login?balh_auth=1&balh_auth_origin=https%3A%2F%2Fwww.bilibili.com"
            ),
            "<redacted BBDown access-key authorization URL>"
        );
        assert_eq!(
            redact_sensitive_text(
                "https://www.bilibili.com/callback?access_token=secret&refresh_token=refresh"
            ),
            "<redacted BBDown access-key callback URL>"
        );
        assert_eq!(
            redact_sensitive_text("https://www.bilibili.com/callback#access_key=secret"),
            "<redacted BBDown access-key callback URL>"
        );
        assert_eq!(
            redact_sensitive_text("balh-login-credentials: {\"access_key\":\"secret\"}"),
            "<redacted BBDown access-key callback message>"
        );
        assert_eq!(
            redact_sensitive_text("{\"access_key\":\"secret\",\"refresh_token\":\"refresh\"}"),
            "<redacted BBDown access-key callback message>"
        );
    }

    #[test]
    fn redacts_credentials_from_standard_payloads_but_preserves_explicit_auth_links() {
        let text = "Authorization link: https://example.test/?access_key=secret".to_string();

        let redacted = prepare_outbound_text(text.clone(), OutboundTextPolicy::RedactCredentials);
        let explicit = prepare_outbound_text(text.clone(), OutboundTextPolicy::AllowAuthSecret);

        assert!(!redacted.contains("secret"));
        assert_eq!(explicit, text);
    }

    #[test]
    fn authorization_links_reach_the_chat_without_reaching_logs() {
        // synthetic-token-fixtures: joey-private-v3 / access-a.
        let auth_code = "codex_synth_v1_access_a";
        let text = format!(
            "Authorization link:\nhttps://passport.bilibili.com/login/app/third?auth_code={auth_code}"
        );
        assert_eq!(
            prepare_outbound_text(text.clone(), OutboundTextPolicy::AllowAuthSecret),
            text
        );
        let log = outbound_log_text(&text, OutboundTextPolicy::AllowAuthSecret);
        assert!(!log.contains(auth_code));
        assert!(!log.contains("passport.bilibili.com"));
    }

    #[test]
    fn extracts_required_telegram_result() {
        let result = telegram_required_result(
            ApiResponse {
                ok: true,
                result: Some(123),
                description: None,
            },
            "sendMessage",
        )
        .expect("result should parse");

        assert_eq!(result, 123);
    }

    #[test]
    fn rejects_missing_required_telegram_result() {
        let err = telegram_required_result::<i64>(
            ApiResponse {
                ok: true,
                result: None,
                description: None,
            },
            "sendMessage",
        )
        .expect_err("missing result should fail");

        assert!(err.to_string().contains("did not include result"));
    }

    #[test]
    fn repeated_message_edit_is_confirmed_but_other_http_errors_fail() {
        let response = |description: &str| ApiResponse::<()> {
            ok: false,
            result: None,
            description: Some(description.to_string()),
        };
        assert!(telegram_edit_result(
            reqwest::StatusCode::BAD_REQUEST,
            response("Bad Request: message is not modified: specified new message content is exactly the same"),
        ).is_ok());
        assert!(
            telegram_edit_result(
                reqwest::StatusCode::BAD_REQUEST,
                response("Bad Request: message to edit not found"),
            )
            .is_err()
        );
        assert!(
            telegram_edit_result(
                reqwest::StatusCode::TOO_MANY_REQUESTS,
                response("message is not modified"),
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn widget_methods_use_the_configured_api_root_and_documented_payload_names() {
        let (client, server) = fake_api(vec![
            MockReply::json(serde_json::json!({
                "ok": true,
                "result": {"message_id": 73, "chat": {"id": 5, "type": "private"}}
            })),
            MockReply::json(serde_json::json!({"ok": true, "result": true})),
            MockReply::json(serde_json::json!({"ok": true, "result": true})),
            MockReply::json(serde_json::json!({"ok": true, "result": true})),
        ])
        .await;
        let html = "<html><body>Selection</body></html>";
        assert_eq!(
            client
                .send_widget(5, html, "{\"v\":1}", "Select music for import")
                .await
                .unwrap(),
            73
        );
        client.edit_widget_data(5, 73, "{\"v\":2}").await.unwrap();
        client
            .answer_widget_query("query-1", Some("{\"v\":2}"))
            .await
            .unwrap();
        client
            .edit_widget_caption(5, 73, "Torrent changed; reopen with /torrents")
            .await
            .unwrap();

        let requests = server.await.unwrap();
        assert_eq!(
            requests
                .iter()
                .map(|(path, _)| path.as_str())
                .collect::<Vec<_>>(),
            [
                "/botcodex_synth_v1_api_key_a/sendWidget",
                "/botcodex_synth_v1_api_key_a/editMessageWidgetData",
                "/botcodex_synth_v1_api_key_a/answerWidgetQuery",
                "/botcodex_synth_v1_api_key_a/editMessageCaption",
            ]
        );
        assert_eq!(requests[0].1["html"], html);
        assert_eq!(requests[0].1["allow_queries"], true);
        assert_eq!(requests[0].1["chat_data"], "{\"v\":1}");
        assert_eq!(requests[1].1["chat_data"], "{\"v\":2}");
        assert_eq!(requests[2].1["widget_query_id"], "query-1");
        assert_eq!(requests[2].1["data"], "{\"v\":2}");
        assert_eq!(
            requests[3].1["caption"],
            "Torrent changed; reopen with /torrents"
        );
    }

    #[tokio::test]
    async fn small_file_download_uses_safe_relative_paths_and_streams_under_the_bound() {
        let (client, server) = fake_api(vec![
            MockReply::json(serde_json::json!({
                "ok": true,
                "result": {"file_size": 7, "file_path": "documents/file_1.torrent"}
            })),
            MockReply::bytes(b"torrent"),
        ])
        .await;
        assert_eq!(
            client.download_small_file("document-1", 32).await.unwrap(),
            b"torrent"
        );
        let requests = server.await.unwrap();
        assert_eq!(requests[0].0, "/botcodex_synth_v1_api_key_a/getFile");
        assert_eq!(requests[0].1["file_id"], "document-1");
        assert_eq!(
            requests[1].0,
            "/file/botcodex_synth_v1_api_key_a/documents/file_1.torrent"
        );
    }

    #[tokio::test]
    async fn small_file_download_rejects_oversize_paths_and_redirects() {
        let (client, server) = fake_api(vec![]).await;
        assert!(
            client
                .download_small_file("document-1", MAX_SMALL_TELEGRAM_FILE_BYTES + 1)
                .await
                .is_err()
        );
        assert!(server.await.unwrap().is_empty());

        let (client, server) = fake_api(vec![MockReply::redirect()]).await;
        assert!(client.download_small_file("document-1", 8).await.is_err());
        assert_eq!(
            server.await.unwrap().len(),
            1,
            "getFile redirects must not be followed"
        );

        let (client, server) = fake_api(vec![MockReply::json(serde_json::json!({
            "ok": true,
            "result": {"file_size": 2, "file_path": "../secret"}
        }))])
        .await;
        assert!(client.download_small_file("document-1", 8).await.is_err());
        assert_eq!(server.await.unwrap().len(), 1);

        let (client, server) = fake_api(vec![
            MockReply::json(serde_json::json!({
                "ok": true,
                "result": {"file_size": 2, "file_path": "file.torrent"}
            })),
            MockReply::redirect(),
        ])
        .await;
        assert!(client.download_small_file("document-1", 8).await.is_err());
        let requests = server.await.unwrap();
        assert_eq!(
            requests.len(),
            2,
            "the file client must not follow redirects"
        );
    }

    #[tokio::test]
    async fn streamed_file_body_is_capped_when_content_length_is_absent() {
        let (client, server) = fake_api(vec![
            MockReply::json(serde_json::json!({
                "ok": true,
                "result": {"file_size": 2, "file_path": "file.torrent"}
            })),
            MockReply::close_delimited_bytes(b"oversize"),
        ])
        .await;
        assert!(client.download_small_file("document-1", 3).await.is_err());
        assert_eq!(server.await.unwrap().len(), 2);
    }
}
