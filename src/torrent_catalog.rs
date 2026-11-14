//! Private, ephemeral catalog of tasks already known to qBittorrent.

use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::{
    Mutex, OnceLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};

use crate::telegram::{CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup};
use crate::torrent_runtime::TorrentService;
use crate::torrent_types::{TorrentExisting, TorrentRemoteSnapshot};
use crate::{BotContext, answer_callback_or_log, send_or_log};

const PAGE_SIZE: usize = 8;
const PAGE_FETCH: u32 = PAGE_SIZE as u32 + 1;
const MAX_SESSIONS: usize = 48;
const SESSION_TTL: Duration = Duration::from_secs(60 * 60);

#[derive(Clone)]
struct CatalogRow {
    hash: String,
    row_hash: [u8; 32],
    name: String,
    state: String,
    progress: f64,
    total_size: u64,
}

struct CatalogSession {
    nonce: String,
    chat_id: i64,
    user_id: i64,
    message_id: i64,
    revision: u32,
    page: u32,
    has_previous: bool,
    has_next: bool,
    rows: Vec<CatalogRow>,
    expires_at: Instant,
    inflight: bool,
}

fn session_is_current(
    session: &CatalogSession,
    chat_id: i64,
    user_id: i64,
    message_id: i64,
    revision: u32,
    now: Instant,
) -> bool {
    session.expires_at > now
        && session.chat_id == chat_id
        && session.user_id == user_id
        && session.message_id == message_id
        && session.revision == revision
        && !session.inflight
}

fn sessions() -> &'static Mutex<HashMap<String, CatalogSession>> {
    static SESSIONS: OnceLock<Mutex<HashMap<String, CatalogSession>>> = OnceLock::new();
    SESSIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn next_nonce(chat_id: i64, user_id: i64) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let values = (nanos, sequence, chat_id, user_id);
    let mut nonce = String::with_capacity(24);
    for _ in 0..3 {
        nonce.push_str(&format!(
            "{:016x}",
            std::collections::hash_map::RandomState::new().hash_one(values)
        ));
    }
    nonce.truncate(24);
    nonce
}

fn row_from_snapshot(snapshot: TorrentRemoteSnapshot) -> Result<CatalogRow> {
    if snapshot.hash.is_empty()
        || snapshot.hash.len() > 128
        || snapshot.hash.chars().any(char::is_control)
    {
        bail!("qBittorrent returned an invalid task identifier");
    }
    let safe_name = snapshot
        .name
        .chars()
        .filter(|ch| !ch.is_control())
        .take(80)
        .collect::<String>();
    let state = snapshot
        .state
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '_' || *ch == '-')
        .take(32)
        .collect::<String>();
    let row_hash = Sha256::digest(snapshot.hash.to_ascii_lowercase().as_bytes()).into();
    Ok(CatalogRow {
        hash: snapshot.hash,
        row_hash,
        name: if safe_name.is_empty() {
            "Unnamed torrent".into()
        } else {
            safe_name
        },
        state: if state.is_empty() {
            "unknown".into()
        } else {
            state
        },
        progress: if snapshot.progress.is_finite() {
            snapshot.progress.clamp(0.0, 1.0)
        } else {
            0.0
        },
        total_size: snapshot.total_size,
    })
}

async fn load_page(service: &TorrentService, page: u32) -> Result<(Vec<CatalogRow>, bool)> {
    let offset = page
        .checked_mul(PAGE_SIZE as u32)
        .context("catalog page is out of range")?;
    let mut snapshots = service.list_torrents(offset, PAGE_FETCH).await?;
    let has_next = snapshots.len() > PAGE_SIZE;
    snapshots.truncate(PAGE_SIZE);
    let rows = snapshots
        .into_iter()
        .map(row_from_snapshot)
        .collect::<Result<Vec<_>>>()?;
    Ok((rows, has_next))
}

fn render(session: &CatalogSession) -> (String, InlineKeyboardMarkup) {
    let mut text = format!("qBittorrent tasks · page {}\n", session.page + 1);
    if session.rows.is_empty() {
        text.push_str("No tasks on this page.");
    }
    let mut keyboard = Vec::new();
    for (index, row) in session.rows.iter().enumerate() {
        let percent = (row.progress * 100.0).round() as u32;
        let title = row.name.chars().take(42).collect::<String>();
        text.push_str(&format!(
            "\n{title}\n{} · {percent}% · {} bytes\n",
            row.state, row.total_size
        ));
        keyboard.push(vec![button(
            format!("Choose music · {title}"),
            &session.nonce,
            session.revision,
            index,
            'o',
        )]);
        let control = if matches!(
            row.state.as_str(),
            "pausedDL" | "pausedUP" | "stoppedDL" | "stoppedUP"
        ) {
            'r'
        } else {
            'p'
        };
        let label = if control == 'r' { "Resume" } else { "Pause" };
        keyboard.push(vec![button(
            format!("{label} · {title}"),
            &session.nonce,
            session.revision,
            index,
            control,
        )]);
    }
    let mut navigation = Vec::new();
    if session.has_previous {
        navigation.push(button(
            "‹ Previous".into(),
            &session.nonce,
            session.revision,
            0,
            'b',
        ));
    }
    navigation.push(button(
        "Refresh".into(),
        &session.nonce,
        session.revision,
        0,
        'f',
    ));
    if session.has_next {
        navigation.push(button(
            "Next ›".into(),
            &session.nonce,
            session.revision,
            0,
            'n',
        ));
    }
    if !navigation.is_empty() {
        keyboard.push(navigation);
    }
    text.push_str("\nOpen creates or reuses a private selection card. Pause and resume affect only the selected task.");
    (
        text,
        InlineKeyboardMarkup {
            inline_keyboard: keyboard,
        },
    )
}

fn button(
    text: String,
    nonce: &str,
    revision: u32,
    index: usize,
    action: char,
) -> InlineKeyboardButton {
    InlineKeyboardButton {
        text,
        callback_data: format!(
            "q1:{nonce}:{}:{}:{action}",
            base36(revision as usize),
            base36(index)
        ),
        web_app: None,
    }
}

fn base36(mut value: usize) -> String {
    if value == 0 {
        return "0".into();
    }
    let mut output = Vec::new();
    while value > 0 {
        let digit = (value % 36) as u8;
        output.push(if digit < 10 {
            b'0' + digit
        } else {
            b'a' + digit - 10
        });
        value /= 36;
    }
    output.reverse();
    String::from_utf8(output).expect("base36 is ASCII")
}

fn parse_base36(value: &str) -> Option<usize> {
    if value.is_empty() || value.len() > 8 {
        return None;
    }
    value.bytes().try_fold(0usize, |acc, byte| {
        let digit = match byte {
            b'0'..=b'9' => (byte - b'0') as usize,
            b'a'..=b'z' => (byte - b'a' + 10) as usize,
            _ => return None,
        };
        acc.checked_mul(36)?.checked_add(digit)
    })
}

#[derive(Debug, PartialEq, Eq)]
struct CatalogCallback {
    nonce: String,
    revision: u32,
    index: usize,
    action: char,
}

fn parse_callback(value: &str) -> Option<CatalogCallback> {
    if value.len() > 64 {
        return None;
    }
    let mut parts = value.split(':');
    if parts.next()? != "q1" {
        return None;
    }
    let nonce = parts.next()?;
    if nonce.len() != 24
        || !nonce
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return None;
    }
    let revision = u32::try_from(parse_base36(parts.next()?)?).ok()?;
    let index = parse_base36(parts.next()?)?;
    let action = parts.next()?;
    if action.len() != 1 {
        return None;
    }
    let action = action.as_bytes()[0] as char;
    if parts.next().is_some() || !matches!(action, 'o' | 'p' | 'r' | 'n' | 'b' | 'f') {
        return None;
    }
    Some(CatalogCallback {
        nonce: nonce.into(),
        revision,
        index,
        action,
    })
}

pub(crate) async fn open_catalog(context: &BotContext, update_id: i64, user_id: i64, chat_id: i64) {
    let Some(service) = &context.torrent else {
        send_or_log(
            &context.telegram,
            chat_id,
            "Torrent downloads are disabled on this bot.".into(),
        )
        .await;
        return;
    };
    match load_page(service, 0).await {
        Ok((rows, has_next)) => {
            let nonce = next_nonce(chat_id, user_id);
            let mut session = CatalogSession {
                nonce: nonce.clone(),
                chat_id,
                user_id,
                message_id: 0,
                revision: 1,
                page: 0,
                has_previous: false,
                has_next,
                rows,
                expires_at: Instant::now() + SESSION_TTL,
                inflight: false,
            };
            let (text, keyboard) = render(&session);
            match context
                .telegram
                .send_message_with_inline_keyboard(chat_id, text, keyboard)
                .await
            {
                Ok(message_id) => {
                    session.message_id = message_id;
                    let mut cache = sessions()
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    cache.retain(|_, entry| entry.expires_at > Instant::now());
                    if cache.len() >= MAX_SESSIONS
                        && let Some(oldest) = cache
                            .iter()
                            .min_by_key(|(_, entry)| entry.expires_at)
                            .map(|(key, _)| key.clone())
                    {
                        cache.remove(&oldest);
                    }
                    cache.insert(nonce, session);
                }
                Err(error) => {
                    tracing::warn!(error = %error, update_id, "could not send qBittorrent catalog")
                }
            }
        }
        Err(error) => {
            tracing::warn!(error = %error, update_id, "qBittorrent catalog unavailable");
            send_or_log(
                &context.telegram,
                chat_id,
                "The qBittorrent catalog is temporarily unavailable. Try /torrents again shortly."
                    .into(),
            )
            .await;
        }
    }
}

pub(crate) async fn handle_callback(context: &BotContext, query: &CallbackQuery) -> bool {
    let Some(data) = query.data.as_deref().filter(|data| data.starts_with("q1:")) else {
        return false;
    };
    let result =
        match tokio::time::timeout(Duration::from_secs(5), callback_inner(context, query, data))
            .await
        {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!(
                "catalog operation timed out; refresh to confirm current state"
            )),
        };
    let answer = if result.is_ok() {
        "Catalog updated."
    } else {
        "This catalog choice expired or could not be confirmed. Refresh /torrents."
    };
    if tokio::time::timeout(
        Duration::from_secs(4),
        answer_callback_or_log(&context.telegram, query.id.clone(), answer.into()),
    )
    .await
    .is_err()
    {
        tracing::warn!(query_id = %query.id, "catalog callback answer timed out");
    }
    true
}

async fn callback_inner(context: &BotContext, query: &CallbackQuery, data: &str) -> Result<()> {
    let request = parse_callback(data).context("invalid catalog callback")?;
    let message = query
        .message
        .as_ref()
        .context("catalog message is missing")?;
    let user = query.from.as_ref().context("catalog user is missing")?;
    if !message.chat.is_private()
        || message.chat.id <= 0
        || user.id != message.chat.id
        || !context
            .config
            .telegram
            .allowed_chat_ids
            .contains(&message.chat.id)
    {
        bail!("catalog owner is invalid");
    }
    let service = context
        .torrent
        .as_ref()
        .context("torrent service is disabled")?;
    let (row, page, action) = {
        let mut cache = sessions()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let session = cache
            .get_mut(&request.nonce)
            .context("catalog session expired")?;
        if !session_is_current(
            session,
            message.chat.id,
            user.id,
            message.message_id,
            request.revision,
            Instant::now(),
        ) {
            bail!("catalog callback is stale");
        }
        if matches!(request.action, 'o' | 'p' | 'r') && request.index >= session.rows.len() {
            bail!("catalog row is invalid");
        }
        session.inflight = true;
        (
            session.rows.get(request.index).cloned(),
            session.page,
            request.action,
        )
    };
    let mut reservation = InflightReservation::new(request.nonce.clone());
    let operation = async {
        match action {
            'o' => {
                let row = row.as_ref().context("catalog row is missing")?;
                verify_row(service, row).await?;
                let request_key = format!(
                    "catalog:{}:{}:{}",
                    request.nonce,
                    base36(request.revision as usize),
                    base36(request.index)
                );
                service
                    .adopt_existing(
                        TorrentExisting {
                            chat_id: message.chat.id,
                            user_id: user.id,
                            message_id: message.message_id,
                            hash: row.hash.clone(),
                        },
                        request_key,
                    )
                    .await?;
                Ok(())
            }
            'p' | 'r' => {
                let row = row.as_ref().context("catalog row is missing")?;
                verify_row(service, row).await?;
                reservation.invalidate_on_drop();
                service.set_active(row.hash.clone(), action == 'r').await?;
                let observed = service.remote_snapshot(row.hash.clone()).await?;
                ensure_same_row(row, &observed)?;
                Ok(())
            }
            'n' | 'b' | 'f' => Ok(()),
            _ => bail!("unsupported catalog operation"),
        }
    }
    .await;
    operation?;
    let next_page = match action {
        'n' => page.saturating_add(1),
        'b' => page.saturating_sub(1),
        _ => page,
    };
    let (rows, has_next) = match load_page(service, next_page).await {
        Ok(page) => page,
        Err(error) => return Err(error),
    };
    let (text, keyboard) = {
        let mut cache = sessions()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let session = cache
            .get_mut(&request.nonce)
            .context("catalog session expired")?;
        if session.message_id != message.message_id || session.revision != request.revision {
            bail!("catalog changed during operation");
        }
        session.revision = session.revision.saturating_add(1);
        session.page = next_page;
        session.has_previous = next_page > 0;
        session.has_next = has_next;
        session.rows = rows;
        session.expires_at = Instant::now() + SESSION_TTL;
        render(session)
    };
    context
        .telegram
        .edit_message_text_with_inline_keyboard(message.chat.id, message.message_id, text, keyboard)
        .await?;
    reservation.complete();
    Ok(())
}

async fn verify_row(service: &TorrentService, row: &CatalogRow) -> Result<()> {
    let current = service.remote_snapshot(row.hash.clone()).await?;
    ensure_same_row(row, &current)
}

fn ensure_same_row(row: &CatalogRow, current: &TorrentRemoteSnapshot) -> Result<()> {
    let digest: [u8; 32] = Sha256::digest(current.hash.to_ascii_lowercase().as_bytes()).into();
    if !current.hash.eq_ignore_ascii_case(&row.hash) || digest != row.row_hash {
        bail!("qBittorrent task changed since catalog capture");
    }
    Ok(())
}

fn release_inflight(nonce: &str) {
    if let Some(session) = sessions()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get_mut(nonce)
    {
        session.inflight = false;
    }
}

fn invalidate_revision(nonce: &str) {
    if let Some(session) = sessions()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get_mut(nonce)
    {
        session.revision = session.revision.saturating_add(1);
        session.inflight = false;
        session.expires_at = Instant::now() + SESSION_TTL;
    }
}

struct InflightReservation {
    nonce: String,
    invalidate_revision: bool,
    completed: bool,
}

impl InflightReservation {
    fn new(nonce: String) -> Self {
        Self {
            nonce,
            invalidate_revision: false,
            completed: false,
        }
    }

    fn invalidate_on_drop(&mut self) {
        self.invalidate_revision = true;
    }

    fn complete(&mut self) {
        release_inflight(&self.nonce);
        self.completed = true;
    }
}

impl Drop for InflightReservation {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        if self.invalidate_revision {
            invalidate_revision(&self.nonce);
        } else {
            release_inflight(&self.nonce);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn callback_protocol_is_compact_and_strict() {
        let data = "q1:0123456789abcdef01234567:1:z:o";
        let parsed = parse_callback(data).expect("valid catalog callback");
        assert_eq!(
            parsed,
            CatalogCallback {
                nonce: "0123456789abcdef01234567".into(),
                revision: 1,
                index: 35,
                action: 'o'
            }
        );
        assert!(data.len() <= 64);
        assert!(parse_callback("q1:0123456789abcdef0123456A:1:0:o").is_none());
        assert!(parse_callback("q1:0123456789abcdef01234567:1:0:o:extra").is_none());
        assert!(parse_callback("q1:0123456789abcdef01234567:1:0:oBAD").is_none());
        assert!(parse_callback(&format!("q1:{}:1:0:o", "0".repeat(50))).is_none());
    }

    #[test]
    fn callback_base36_roundtrips_bounds() {
        for value in [0, 1, 35, 36, u16::MAX as usize] {
            assert_eq!(parse_base36(&base36(value)), Some(value));
        }
        assert_eq!(parse_base36("-1"), None);
    }

    #[test]
    fn session_binding_rejects_wrong_owner_message_revision_and_expiry() {
        let now = Instant::now();
        let session = CatalogSession {
            nonce: "0123456789abcdef01234567".into(),
            chat_id: 10,
            user_id: 10,
            message_id: 20,
            revision: 3,
            page: 0,
            has_previous: false,
            has_next: false,
            rows: Vec::new(),
            expires_at: now + SESSION_TTL,
            inflight: false,
        };
        assert!(session_is_current(&session, 10, 10, 20, 3, now));
        assert!(!session_is_current(&session, 11, 10, 20, 3, now));
        assert!(!session_is_current(&session, 10, 11, 20, 3, now));
        assert!(!session_is_current(&session, 10, 10, 21, 3, now));
        assert!(!session_is_current(&session, 10, 10, 20, 2, now));
        assert!(!session_is_current(
            &session,
            10,
            10,
            20,
            3,
            now + SESSION_TTL
        ));
    }

    #[test]
    fn rendering_shows_only_safe_summary_and_page_controls() {
        let row = CatalogRow {
            hash: "0123456789abcdef".into(),
            row_hash: Sha256::digest(b"0123456789abcdef").into(),
            name: "Audio collection".into(),
            state: "downloading".into(),
            progress: 0.375,
            total_size: 4096,
        };
        let session = CatalogSession {
            nonce: "0123456789abcdef01234567".into(),
            chat_id: 10,
            user_id: 10,
            message_id: 20,
            revision: 4,
            page: 0,
            has_previous: false,
            has_next: true,
            rows: vec![row],
            expires_at: Instant::now() + SESSION_TTL,
            inflight: false,
        };
        let (text, keyboard) = render(&session);
        assert!(text.contains("Audio collection"));
        assert!(text.contains("38%"));
        assert!(!text.contains("0123456789abcdef"));
        assert!(!text.contains("save_path"));
        assert_eq!(keyboard.inline_keyboard.len(), 3);
        let nav = keyboard.inline_keyboard.last().expect("navigation row");
        assert_eq!(nav.len(), 2);
        assert_eq!(
            parse_callback(&nav[1].callback_data)
                .expect("next callback")
                .action,
            'n'
        );
        assert!(
            keyboard
                .inline_keyboard
                .iter()
                .flatten()
                .all(|button| button.callback_data.len() <= 64)
        );
    }

    #[test]
    fn dropping_catalog_reservation_releases_or_invalidates_revision() {
        let nonce = "fedcba987654321001234567";
        let session = CatalogSession {
            nonce: nonce.into(),
            chat_id: 30,
            user_id: 30,
            message_id: 40,
            revision: 7,
            page: 0,
            has_previous: false,
            has_next: false,
            rows: Vec::new(),
            expires_at: Instant::now() + SESSION_TTL,
            inflight: true,
        };
        sessions()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(nonce.into(), session);
        drop(InflightReservation::new(nonce.into()));
        {
            let cache = sessions()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let session = cache.get(nonce).expect("reservation session");
            assert!(!session.inflight);
            assert_eq!(session.revision, 7);
        }
        sessions()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get_mut(nonce)
            .unwrap()
            .inflight = true;
        let mut uncertain = InflightReservation::new(nonce.into());
        uncertain.invalidate_on_drop();
        drop(uncertain);
        let mut cache = sessions()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let session = cache.get(nonce).expect("reservation session");
        assert!(!session.inflight);
        assert_eq!(session.revision, 8);
        cache.remove(nonce);
    }
}
