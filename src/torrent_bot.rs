//! Private-owner Telegram admission, recovery commands, and Widget interactions.

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};

use crate::telegram::{
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message, WidgetQuery,
};
use crate::torrent_runtime::{card_presentation, phase_label};
use crate::torrent_types::{TorrentAction, TorrentMagnet, TorrentUpload};
use crate::torrent_widget::{TorrentWidgetAction, parse_widget_request};
use crate::{BotContext, answer_callback_or_log, human_bytes, send_or_log};

#[derive(Debug)]
pub(crate) struct TorrentAdmissionFailure;

impl std::fmt::Display for TorrentAdmissionFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("torrent task was not durably admitted")
    }
}

impl std::error::Error for TorrentAdmissionFailure {}

fn is_owner(context: &BotContext, chat_id: i64, user_id: Option<i64>, private: bool) -> bool {
    private
        && chat_id > 0
        && user_id == Some(chat_id)
        && context.config.telegram.allowed_chat_ids.contains(&chat_id)
}

pub(crate) fn upload_from_message(
    update_id: i64,
    message: &Message,
    max_bytes: usize,
) -> Result<Option<TorrentUpload>> {
    let Some(document) = &message.document else {
        return Ok(None);
    };
    let file_name = document
        .file_name
        .clone()
        .unwrap_or_else(|| "download.torrent".into());
    let torrent_extension = Path::new(&file_name)
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("torrent"));
    let torrent_mime = document.mime_type.as_deref().is_some_and(|mime| {
        mime.split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .eq_ignore_ascii_case("application/x-bittorrent")
    });
    if !torrent_extension && !torrent_mime {
        return Ok(None);
    }
    // An absent filename is only meaningful with the recognized MIME, not for every document.
    if document.file_name.is_none() && !torrent_mime {
        return Ok(None);
    }
    if file_name.is_empty() || file_name.len() > 1024 || file_name.chars().any(char::is_control) {
        bail!("The torrent filename is invalid. Please resend the .torrent file.");
    }
    let size = document
        .file_size
        .context("Telegram did not provide the torrent size. Please resend it as a file.")?;
    if size == 0 || size > max_bytes as u64 {
        bail!(
            "Torrent metadata must be between 1 byte and {}. This limit does not apply to the downloaded payload.",
            human_bytes(max_bytes as u64)
        );
    }
    for id in [&document.file_id, &document.file_unique_id] {
        if id.is_empty()
            || id.len() > 4096
            || id
                .chars()
                .any(|character| character.is_whitespace() || character.is_control())
        {
            bail!("Telegram file metadata is invalid. Please resend the .torrent file.");
        }
    }
    Ok(Some(TorrentUpload {
        update_id,
        chat_id: message.chat.id,
        message_id: message.message_id,
        user_id: message
            .from
            .as_ref()
            .context("Torrent uploads require an identified sender.")?
            .id,
        file_id: document.file_id.clone(),
        file_unique_id: document.file_unique_id.clone(),
        file_name,
        file_size: size,
    }))
}

/// Returns true only for a torrent document. A failed durable admission keeps the update pending.
pub(crate) async fn admit_document(
    context: &BotContext,
    update_id: i64,
    message: &Message,
) -> Result<bool> {
    let upload =
        match upload_from_message(update_id, message, context.config.torrent.max_torrent_bytes) {
            Ok(Some(upload)) => upload,
            Ok(None) => return Ok(false),
            Err(error) => {
                if is_owner(
                    context,
                    message.chat.id,
                    message.from.as_ref().map(|from| from.id),
                    message.chat.is_private(),
                ) {
                    send_or_log(&context.telegram, message.chat.id, error.to_string()).await;
                }
                return Ok(true);
            }
        };
    if !is_owner(
        context,
        upload.chat_id,
        Some(upload.user_id),
        message.chat.is_private(),
    ) {
        return Ok(true);
    }
    let Some(service) = &context.torrent else {
        send_or_log(
            &context.telegram,
            upload.chat_id,
            "Torrent downloads are disabled on this bot.".into(),
        )
        .await;
        return Ok(true);
    };
    service
        .enqueue(upload)
        .await
        .context(TorrentAdmissionFailure)?;
    Ok(true)
}

fn magnet_input(text: &str) -> Option<&str> {
    let text = text.trim();
    if text
        .get(..7)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("magnet:"))
    {
        return Some(text);
    }
    let command = text.split_whitespace().next()?;
    if command.split('@').next() == Some("/magnet") {
        return Some(text[command.len()..].trim());
    }
    None
}

pub(crate) async fn admit_magnet_text(
    context: &BotContext,
    update_id: i64,
    message_id: i64,
    user_id: Option<i64>,
    chat_id: i64,
    private: bool,
    text: &str,
) -> Result<bool> {
    let Some(uri) = magnet_input(text) else {
        return Ok(false);
    };
    if !is_owner(context, chat_id, user_id, private) {
        return Ok(true);
    }
    let parsed = match crate::torrent_magnet::parse_magnet_uri(uri) {
        Ok(parsed) => parsed,
        Err(_) => {
            send_or_log(&context.telegram, chat_id,
                "Invalid magnet link. Send one magnet URI with a valid BTIH or BTMH hash, or use /magnet LINK.".into()).await;
            return Ok(true);
        }
    };
    let Some(service) = &context.torrent else {
        send_or_log(
            &context.telegram,
            chat_id,
            "Torrent downloads are disabled on this bot.".into(),
        )
        .await;
        return Ok(true);
    };
    service
        .enqueue_magnet(TorrentMagnet {
            update_id,
            chat_id,
            message_id,
            user_id: chat_id,
            uri: parsed.normalized_uri,
        })
        .await
        .context(TorrentAdmissionFailure)?;
    Ok(true)
}

pub(crate) async fn handle_widget(context: BotContext, query: WidgetQuery) {
    let result = handle_widget_inner(&context, &query).await;
    let answer = match result {
        Ok(data) => data,
        Err(error) => {
            tracing::warn!(error = %error, "torrent widget operation was not confirmed");
            serde_json::json!({"error":"操作未确认。请刷新任务后重试；已保存的下载和导入不会丢失。"}).to_string()
        }
    };
    if let Err(error) = context
        .telegram
        .answer_widget_query(&query.id, Some(&answer))
        .await
    {
        // Query responses expire after 15 seconds. The durable SET/import intent is independent
        // of this short-lived UI reply, so an expired reply must never undo or repeat an import.
        tracing::warn!(error = %error, "could not answer torrent widget query");
    }
}

async fn handle_widget_inner(context: &BotContext, query: &WidgetQuery) -> Result<String> {
    let chat = query.chat.as_ref().context("widget chat is missing")?;
    if !is_owner(context, chat.id, Some(query.from.id), chat.is_private())
        || query.inline_message_id.is_some()
        || query.widget_id.is_some()
        || query.chat_type.is_some()
        || query.chat_instance.as_deref().is_none_or(str::is_empty)
        || query.r#type.kind != "data"
    {
        bail!("widget owner or private context is invalid");
    }
    let message_id = query
        .message_id
        .filter(|id| *id > 0)
        .context("widget message is missing")?;
    let request = parse_widget_request(&query.data).context("invalid torrent widget action")?;
    let service = context
        .torrent
        .as_ref()
        .context("torrent downloads are disabled")?;
    let action = match request.action {
        TorrentWidgetAction::Refresh => TorrentAction::Refresh,
        TorrentWidgetAction::Browse { folder_id, page } => {
            TorrentAction::Browse { folder_id, page }
        }
        TorrentWidgetAction::SetFile { index, selected } => {
            TorrentAction::SetFile { index, selected }
        }
        TorrentWidgetAction::SetFolder {
            folder_id,
            selected,
        } => TorrentAction::SetFolder {
            folder_id,
            selected,
        },
        TorrentWidgetAction::ClearSelection => TorrentAction::ClearSelection,
        TorrentWidgetAction::ReviewSelection { page } => TorrentAction::ReviewSelection { page },
        TorrentWidgetAction::ImportSelected => TorrentAction::ImportSelected,
        TorrentWidgetAction::RetryImports => TorrentAction::RetryImports,
    };
    let snapshot = tokio::time::timeout(
        Duration::from_secs(2),
        service.apply_action(
            chat.id,
            query.from.id,
            message_id,
            request.task_id,
            request.expected_revision,
            action,
        ),
    )
    .await
    .context("torrent state is still being saved; refresh the card")??;
    Ok(card_presentation(&snapshot)?.chat_data)
}

pub(crate) async fn handle_command(
    context: &BotContext,
    update_id: i64,
    user_id: Option<i64>,
    chat_id: i64,
    private: bool,
    text: &str,
) -> bool {
    let parts = text.split_whitespace().collect::<Vec<_>>();
    if !parts
        .first()
        .is_some_and(|command| command.split('@').next() == Some("/torrents"))
    {
        return false;
    }
    if !is_owner(context, chat_id, user_id, private) {
        return true;
    }
    if matches!(parts.as_slice(), [_] | [_, "list"]) {
        if let Some(user_id) = user_id {
            crate::torrent_catalog::open_catalog(context, update_id, user_id, chat_id).await;
        }
        return true;
    }
    let Some(service) = &context.torrent else {
        send_or_log(
            &context.telegram,
            chat_id,
            "Torrent downloads are disabled on this bot.".into(),
        )
        .await;
        return true;
    };
    match parts.as_slice() {
        [_, "saved"] => match service.list(chat_id).await {
            Ok(mut tasks) => {
                if tasks.is_empty() {
                    send_or_log(&context.telegram, chat_id, "No torrent tasks yet. Send a .torrent file to download its contents and choose music to import.".into()).await;
                    return true;
                }
                let total = tasks.len();
                tasks.sort_by_key(|task| std::cmp::Reverse(task.upload_message_id));
                let mut rows = Vec::new();
                let mut text = format!("Torrent tasks ({total})\nChoose a task to open its current music selection card.\n");
                for task in tasks.into_iter().take(12) {
                    let title = task.name.chars().take(48).collect::<String>();
                    let label = format!("{} · {}", phase_label(&task.phase), title);
                    text.push_str(&format!("\n{title}\n/torrents {}\n", task.task_id));
                    rows.push(vec![InlineKeyboardButton {
                        text: label.chars().take(64).collect(),
                        callback_data: format!("torrent:open:{}", task.task_id),
                        web_app: None,
                    }]);
                }
                if total > 12 { text.push_str("\nShowing the 12 most recent torrent uploads."); }
                if let Err(error) = context.telegram.send_message_with_inline_keyboard(chat_id, text, InlineKeyboardMarkup { inline_keyboard: rows }).await {
                    tracing::warn!(error = %error, "could not send torrent task list");
                }
            }
            Err(_) => send_or_log(&context.telegram, chat_id, "Saved torrent tasks are temporarily unavailable. Existing downloads and selections are retained; try /torrents saved again shortly.".into()).await,
        },
        [_, task_id] if valid_task_id(task_id) => {
            let result = service.reopen_card(chat_id, chat_id, (*task_id).into(), format!("update:{update_id}")).await;
            if result.is_err() {
                send_or_log(&context.telegram, chat_id, "This torrent card could not be opened. Refresh /torrents and choose the task again.".into()).await;
            }
        }
        _ => send_or_log(&context.telegram, chat_id, "Usage: /torrents or /torrents TASK_ID.".into()).await,
    }
    true
}

pub(crate) async fn handle_callback(context: &BotContext, query: &CallbackQuery) -> bool {
    if crate::torrent_catalog::handle_callback(context, query).await {
        return true;
    }
    if query
        .data
        .as_deref()
        .is_some_and(|data| data.starts_with("t1:"))
    {
        let result = handle_selection_callback(context, query).await;
        let response = if result.is_ok() {
            "Selection updated."
        } else {
            "This choice could not be confirmed. Refresh the current card or open /torrents."
        };
        answer_callback_or_log(&context.telegram, query.id.clone(), response.into()).await;
        return true;
    }
    let Some(task_id) = query
        .data
        .as_deref()
        .and_then(|data| data.strip_prefix("torrent:open:"))
    else {
        return false;
    };
    let response = match (&query.message, &query.from, &context.torrent) {
        (Some(message), Some(user), Some(service))
            if valid_task_id(task_id)
                && is_owner(
                    context,
                    message.chat.id,
                    Some(user.id),
                    message.chat.is_private(),
                ) =>
        {
            let request_key = format!("callback:{:x}", Sha256::digest(query.id.as_bytes()));
            match tokio::time::timeout(
                Duration::from_secs(2),
                service.reopen_card(message.chat.id, user.id, task_id.into(), request_key),
            )
            .await
            {
                Ok(Ok(_)) => "Opening the torrent selection card.",
                _ => "The card could not be opened yet. Try /torrents shortly.",
            }
        }
        _ => "This torrent choice is unavailable.",
    };
    answer_callback_or_log(&context.telegram, query.id.clone(), response.into()).await;
    true
}

async fn handle_selection_callback(context: &BotContext, query: &CallbackQuery) -> Result<()> {
    let message = query
        .message
        .as_ref()
        .context("torrent card message is missing")?;
    let user = query.from.as_ref().context("torrent sender is missing")?;
    if !is_owner(
        context,
        message.chat.id,
        Some(user.id),
        message.chat.is_private(),
    ) {
        bail!("torrent callback owner is invalid");
    }
    let request = parse_widget_request(query.data.as_deref().unwrap_or_default())?;
    let action = match request.action {
        TorrentWidgetAction::Refresh => TorrentAction::Refresh,
        TorrentWidgetAction::Browse { folder_id, page } => {
            TorrentAction::Browse { folder_id, page }
        }
        TorrentWidgetAction::SetFile { index, selected } => {
            TorrentAction::SetFile { index, selected }
        }
        TorrentWidgetAction::SetFolder {
            folder_id,
            selected,
        } => TorrentAction::SetFolder {
            folder_id,
            selected,
        },
        TorrentWidgetAction::ClearSelection => TorrentAction::ClearSelection,
        TorrentWidgetAction::ReviewSelection { page } => TorrentAction::ReviewSelection { page },
        TorrentWidgetAction::ImportSelected => TorrentAction::ImportSelected,
        TorrentWidgetAction::RetryImports => TorrentAction::RetryImports,
    };
    let service = context
        .torrent
        .as_ref()
        .context("torrent downloads are disabled")?;
    // The durable action owns its bounded filesystem permit even if this UI answer expires.
    tokio::time::timeout(
        Duration::from_secs(5),
        service.apply_action(
            message.chat.id,
            user.id,
            message.message_id,
            request.task_id,
            request.expected_revision,
            action,
        ),
    )
    .await
    .context("torrent action is still being saved")??;
    Ok(())
}

fn valid_task_id(value: &str) -> bool {
    value.len() == 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn magnet_text_entry_accepts_direct_or_command_without_stealing_other_messages() {
        let uri = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567";
        assert_eq!(magnet_input(uri), Some(uri));
        assert_eq!(magnet_input(&format!(" /magnet {uri} ")), Some(uri));
        assert_eq!(magnet_input(&format!("/magnet@test_bot {uri}")), Some(uri));
        assert_eq!(magnet_input("/magnet"), Some(""));
        assert_eq!(magnet_input("https://example.com/video"), None);
        assert_eq!(magnet_input("/torrents"), None);
    }

    fn document(name: Option<&str>, mime: Option<&str>, size: Option<u64>) -> Message {
        serde_json::from_value(serde_json::json!({
            "message_id":42,
            "chat":{"id":123456789,"type":"private"},
            "from":{"id":123456789},
            "document":{
                "file_id":"torrent-fixture-file",
                "file_unique_id":"torrent-fixture-unique",
                "file_name":name,"mime_type":mime,"file_size":size
            }
        }))
        .unwrap()
    }

    #[test]
    fn torrent_admission_recognizes_filename_or_explicit_mime_without_stealing_music() {
        let max = 4 * 1024 * 1024;
        let upload = upload_from_message(
            7,
            &document(Some("音楽 ALBUM.TORRENT"), None, Some(100)),
            max,
        )
        .unwrap()
        .unwrap();
        assert_eq!(upload.file_name, "音楽 ALBUM.TORRENT");
        assert_eq!(upload.user_id, 123456789);
        assert!(
            upload_from_message(
                7,
                &document(None, Some("application/x-bittorrent"), Some(100)),
                max
            )
            .unwrap()
            .is_some()
        );
        assert!(
            upload_from_message(
                7,
                &document(None, Some("application/octet-stream"), Some(100)),
                max
            )
            .unwrap()
            .is_none()
        );
        assert!(
            upload_from_message(
                7,
                &document(
                    Some("01. 愛♡スクリ～ム!.flac"),
                    Some("audio/flac"),
                    Some(100)
                ),
                max
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn torrent_admission_rejects_unknown_or_oversized_metadata_before_queueing() {
        let max = 4 * 1024 * 1024;
        for size in [None, Some(0), Some(max as u64 + 1)] {
            assert!(
                upload_from_message(7, &document(Some("album.torrent"), None, size), max).is_err()
            );
        }
        let mut file = document(Some("album.torrent"), None, Some(100));
        file.document.as_mut().unwrap().file_id.push('\0');
        assert!(upload_from_message(7, &file, max).is_err());
    }

    #[test]
    fn recovery_task_ids_are_fixed_ascii_and_cannot_be_paths() {
        assert!(valid_task_id(&"a".repeat(32)));
        for invalid in ["../album", "AABB", "", &"a".repeat(31), &"A".repeat(32)] {
            assert!(!valid_task_id(invalid));
        }
    }
}
