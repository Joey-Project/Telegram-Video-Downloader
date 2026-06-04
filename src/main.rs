mod bilibili_auth;
mod config;
mod downloader;
mod router;
mod telegram;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::Client;
use tokio::sync::{Mutex, Notify, Semaphore, mpsc};
use tokio::time::{Instant, sleep, timeout as tokio_timeout};
use tracing::{error, info, warn};

use crate::config::AppConfig;
use crate::downloader::{
    JobProgress, VideoDuplicate, VideoDuplicateAction, find_video_duplicate, run_job,
    run_job_with_duplicate_action, run_video_job_staged_keep_both,
};
use crate::router::{BilibiliAuthCommand, JobRequest, RouteResult, route_message};
use crate::telegram::{
    BotCommand, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, TelegramClient,
};

static BILIBILI_LOGIN_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static BILIBILI_LOGIN_CANCEL_NOTIFY: OnceLock<Notify> = OnceLock::new();
static BILIBILI_AUTH_STATE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static BILIBILI_AUTH_GENERATION: AtomicU64 = AtomicU64::new(0);
static PENDING_DUPLICATE_JOBS: OnceLock<Mutex<HashMap<u64, PendingDuplicateJob>>> = OnceLock::new();
static DUPLICATE_CALLBACK_COUNTER: AtomicU64 = AtomicU64::new(1);
const BILIBILI_AUTH_MAX_HTTP_TIMEOUT_SECONDS: u64 = 30;
const DUPLICATE_DECISION_TTL: Duration = Duration::from_secs(30 * 60);
const MAX_PENDING_DUPLICATE_JOBS: usize = 256;

#[derive(Debug, Clone)]
struct PendingDuplicateJob {
    chat_id: i64,
    job_id: u64,
    job: JobRequest,
    duplicate: VideoDuplicate,
    created_at: Instant,
}

#[derive(Debug, Clone)]
struct DuplicateRun {
    action: VideoDuplicateAction,
    duplicate: VideoDuplicate,
}

#[derive(Debug, Clone)]
enum JobRunMode {
    Direct,
    StagedKeepBoth,
    Duplicate(DuplicateRun),
}

#[derive(Clone)]
struct JobDispatch {
    download_semaphore: Arc<Semaphore>,
    duplicate_scan_semaphore: Arc<Semaphore>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "telegram_video_downloader=info,info".into()),
        )
        .init();

    let args = std::env::args_os().skip(1).collect::<Vec<_>>();
    if args
        .first()
        .is_some_and(|arg| arg == std::ffi::OsStr::new("--replay-message"))
    {
        let config_path = args
            .get(1)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("config.toml"));
        let message = args
            .get(2..)
            .unwrap_or(&[])
            .iter()
            .map(|arg| arg.to_string_lossy())
            .collect::<Vec<_>>()
            .join(" ");
        return replay_message(config_path, message).await;
    }

    let config_path = args
        .first()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config.toml"));
    let config = Arc::new(AppConfig::load(&config_path)?);
    config.ensure_runtime_dirs()?;

    let telegram = TelegramClient::new(config.telegram.token.clone());
    if let Err(err) = telegram.set_my_commands(default_bot_commands()).await {
        warn!(error = %err, "failed to register Telegram bot commands");
    }
    let job_dispatch = JobDispatch {
        download_semaphore: Arc::new(Semaphore::new(config.bot.concurrency)),
        duplicate_scan_semaphore: Arc::new(Semaphore::new(config.bot.concurrency)),
    };
    let next_job_id = Arc::new(AtomicU64::new(1));
    let mut offset = None;

    info!(
        concurrency = config.bot.concurrency,
        "telegram local downloader started"
    );

    loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal.context("failed to listen for ctrl-c")?;
                info!("shutdown requested");
                break;
            }
            updates = telegram.get_updates(offset, config.bot.poll_timeout_seconds) => {
                match updates {
                    Ok(updates) => {
                        for update in updates {
                            offset = Some(update.update_id + 1);
                            if let Some(message) = update.message {
                                handle_message(
                                    telegram.clone(),
                                    Arc::clone(&config),
                                    job_dispatch.clone(),
                                    Arc::clone(&next_job_id),
                                    message.chat.id,
                                    message.chat.is_private(),
                                    message.text.as_deref(),
                                )
                                .await;
                            }
                            if let Some(callback_query) = update.callback_query {
                                handle_callback_query(
                                    telegram.clone(),
                                    Arc::clone(&config),
                                    Arc::clone(&job_dispatch.download_semaphore),
                                    callback_query,
                                )
                                .await;
                            }
                        }
                    }
                    Err(err) => {
                        warn!(error = %err, "failed to fetch telegram updates");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        }
    }

    Ok(())
}

async fn replay_message(config_path: PathBuf, text: String) -> Result<()> {
    if text.trim().is_empty() {
        bail!("usage: telegram-video-downloader --replay-message config.toml <message>");
    }

    let config = AppConfig::load(&config_path)?;
    config.ensure_runtime_dirs()?;

    match route_message(&text, &config.pdf.auto_domains) {
        RouteResult::Jobs(jobs) => {
            let mut failed_jobs = Vec::new();
            for (index, job) in jobs.iter().enumerate() {
                let job_id = index + 1;
                println!("Queued replay job #{job_id}: {}", job.label());
                println!("Started replay job #{job_id}: {}", job.label());
                let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<JobProgress>();
                let progress_handle = tokio::spawn(async move {
                    while let Some(progress) = progress_rx.recv().await {
                        println!("Progress replay job #{job_id}: {}", progress.message);
                    }
                });
                let result = match job {
                    JobRequest::Bilibili { .. } | JobRequest::Youtube { .. } => {
                        run_video_job_staged_keep_both(&config, job, Some(progress_tx)).await
                    }
                    JobRequest::Pdf { .. } => run_job(&config, job, Some(progress_tx)).await,
                };
                let _ = progress_handle.await;
                match result {
                    Ok(report) => {
                        println!(
                            "Finished replay job #{job_id}: {}\nSaved: {}",
                            job.label(),
                            report.saved_location
                        );
                        if !report.details.is_empty() {
                            println!("{}", report.details);
                        }
                    }
                    Err(err) => {
                        println!("Failed replay job #{job_id}: {}\n{err}", job.label());
                        failed_jobs.push(format!("#{job_id} {}", job.label()));
                    }
                }
            }
            if failed_jobs.is_empty() {
                Ok(())
            } else {
                bail!(
                    "{} replay job(s) failed: {}",
                    failed_jobs.len(),
                    failed_jobs.join(", ")
                )
            }
        }
        RouteResult::PdfUsage => bail!("usage: /pdf https://example.com"),
        RouteResult::Help => {
            println!("{}", help_message());
            Ok(())
        }
        RouteResult::BilibiliAuth(_) | RouteResult::BilibiliAuthUsage => {
            bail!("bbdown auth commands require Telegram bot chat")
        }
        RouteResult::UnsupportedLinks => bail!("no supported links found"),
        RouteResult::Empty => bail!("message did not contain text to route"),
    }
}

async fn handle_message(
    telegram: TelegramClient,
    config: Arc<AppConfig>,
    job_dispatch: JobDispatch,
    next_job_id: Arc<AtomicU64>,
    chat_id: i64,
    is_private_chat: bool,
    text: Option<&str>,
) {
    let Some(text) = text else {
        return;
    };

    if !config.telegram.is_chat_allowed(chat_id) {
        warn!(chat_id, "ignoring message from unauthorized chat");
        return;
    }

    match route_message(text, &config.pdf.auto_domains) {
        RouteResult::Jobs(jobs) => {
            for job in jobs {
                let job_id = next_job_id.fetch_add(1, Ordering::Relaxed);
                queue_or_prompt_job(
                    telegram.clone(),
                    Arc::clone(&config),
                    job_dispatch.clone(),
                    chat_id,
                    job_id,
                    job,
                );
            }
        }
        RouteResult::BilibiliAuth(command) => {
            handle_bilibili_auth_command(telegram, config, chat_id, is_private_chat, command).await;
        }
        RouteResult::Help => {
            send_or_log(&telegram, chat_id, help_message()).await;
        }
        RouteResult::BilibiliAuthUsage => {
            let message = if is_private_chat {
                bbdown_auth_usage()
            } else {
                "Please manage BBDown login state in a private chat with this bot.".to_string()
            };
            send_or_log(&telegram, chat_id, message).await;
        }
        RouteResult::PdfUsage => {
            send_or_log(
                &telegram,
                chat_id,
                "Usage: /pdf https://example.com".to_string(),
            )
            .await;
        }
        RouteResult::UnsupportedLinks => {
            send_or_log(
                &telegram,
                chat_id,
                "No supported links found. Send Bilibili/YouTube links directly, use /pdf URL, or configure a PDF auto-domain."
                    .to_string(),
            )
            .await;
        }
        RouteResult::Empty => {}
    }
}

fn default_bot_commands() -> Vec<BotCommand> {
    vec![
        BotCommand {
            command: "help".to_string(),
            description: "Show supported commands and link handling.".to_string(),
        },
        BotCommand {
            command: "pdf".to_string(),
            description: "Save a webpage as PDF.".to_string(),
        },
        BotCommand {
            command: "bbdown".to_string(),
            description: "Manage BBDown Bilibili login state.".to_string(),
        },
    ]
}

fn help_message() -> String {
    [
        "Telegram Local Downloader Bot",
        "",
        "Send Bilibili or YouTube links directly to download videos.",
        "Bilibili opus links and configured PDF domains are saved as PDF automatically.",
        "",
        "Commands:",
        "/help - Show this help.",
        "/pdf URL - Save a webpage as PDF.",
        "/bbdown login - Log in to Bilibili for BBDown downloads.",
        "/bbdown status - Show the saved BBDown login account.",
        "/bbdown logout - Clear the local BBDown login state.",
    ]
    .join("\n")
}

async fn handle_bilibili_auth_command(
    telegram: TelegramClient,
    config: Arc<AppConfig>,
    chat_id: i64,
    is_private_chat: bool,
    command: BilibiliAuthCommand,
) {
    if !is_private_chat {
        send_or_log(
            &telegram,
            chat_id,
            "Please manage BBDown login state in a private chat with this bot.".to_string(),
        )
        .await;
        return;
    }

    match command {
        BilibiliAuthCommand::Login => {
            let lock = BILIBILI_LOGIN_LOCK.get_or_init(|| Mutex::new(()));
            let guard = match lock.try_lock() {
                Ok(guard) => guard,
                Err(_) => {
                    send_or_log(
                        &telegram,
                        chat_id,
                        "BBDown login is already in progress. Finish or wait for the current QR login to expire.".to_string(),
                    )
                    .await;
                    return;
                }
            };
            let auth_generation = BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst);
            tokio::spawn(async move {
                let _guard = guard;
                run_bbdown_login(telegram, config, chat_id, auth_generation).await;
            });
        }
        BilibiliAuthCommand::Status => {
            tokio::spawn(async move {
                run_bbdown_status(telegram, config, chat_id).await;
            });
        }
        BilibiliAuthCommand::Logout => {
            run_bbdown_logout(telegram, config, chat_id).await;
        }
    }
}

async fn run_bbdown_login(
    telegram: TelegramClient,
    config: Arc<AppConfig>,
    chat_id: i64,
    auth_generation: u64,
) {
    send_or_log(
        &telegram,
        chat_id,
        "Preparing BBDown Bilibili login QR...".to_string(),
    )
    .await;

    let result = bbdown_login_flow(&telegram, &config, chat_id, auth_generation).await;
    let message = match result {
        Ok(state) => format!("BBDown logged in as {} (mid: {}).", state.uname, state.mid),
        Err(err) => format!(
            "BBDown login failed:\n{}",
            summarize_bbdown_auth_error(&err)
        ),
    };
    send_or_log(&telegram, chat_id, message).await;
}

async fn bbdown_login_flow(
    telegram: &TelegramClient,
    config: &AppConfig,
    chat_id: i64,
    auth_generation: u64,
) -> Result<bilibili_auth::AuthState> {
    let client = bbdown_auth_client(config)?;
    let login_qr =
        await_bbdown_login_active(auth_generation, bilibili_auth::generate_login_qr(&client))
            .await??;
    ensure_bbdown_login_active(auth_generation)?;
    let caption = format!(
        "Scan this Bilibili QR code in the app to authorize BBDown. It expires in {} seconds.",
        config.bilibili.auth.login_timeout_seconds
    );
    let send_photo_result = await_bbdown_login_active(
        auth_generation,
        telegram.send_photo(chat_id, caption, login_qr.png.clone()),
    )
    .await?;
    if let Err(err) = send_photo_result {
        warn!(chat_id, error = %err, "failed to send BBDown login QR image");
        send_or_log(telegram, chat_id, bbdown_qr_photo_failed_message()).await;
        bail!("failed to send Bilibili login QR image");
    }
    ensure_bbdown_login_active(auth_generation)?;

    let deadline = Instant::now() + Duration::from_secs(config.bilibili.auth.login_timeout_seconds);
    let poll_interval = Duration::from_secs(config.bilibili.auth.poll_interval_seconds);
    let mut sent_scanned_notice = false;

    loop {
        if BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst) != auth_generation {
            bail!("BBDown login was canceled by a later /bbdown logout");
        }
        if Instant::now() >= deadline {
            bail!("Bilibili login QR timed out");
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        let poll = await_bbdown_login_active(
            auth_generation,
            tokio_timeout(
                remaining,
                bilibili_auth::poll_login(&client, &login_qr.qrcode_key),
            ),
        )
        .await?
        .context("Bilibili login QR timed out")??;

        match poll {
            bilibili_auth::LoginPoll::Waiting => {}
            bilibili_auth::LoginPoll::Scanned => {
                if !sent_scanned_notice {
                    sent_scanned_notice = true;
                    ensure_bbdown_login_active(auth_generation)?;
                    send_or_log(
                        telegram,
                        chat_id,
                        "QR scanned. Confirm the login in the Bilibili app.".to_string(),
                    )
                    .await;
                }
            }
            bilibili_auth::LoginPoll::Expired => bail!("Bilibili login QR expired"),
            bilibili_auth::LoginPoll::Success { cookie } => {
                let state = bilibili_auth::verify_cookie(&client, &cookie).await?;
                let _state_guard = bbdown_auth_state_lock().lock().await;
                if BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst) != auth_generation {
                    bail!("BBDown login was canceled by a later /bbdown logout");
                }
                bilibili_auth::save_auth_state(&config.bilibili.auth.state_path, &state)?;
                return Ok(state);
            }
        }

        let now = Instant::now();
        if now >= deadline {
            bail!("Bilibili login QR timed out");
        }
        await_bbdown_login_active(auth_generation, sleep(poll_interval.min(deadline - now)))
            .await?;
    }
}

async fn await_bbdown_login_active<F, T>(auth_generation: u64, future: F) -> Result<T>
where
    F: std::future::Future<Output = T>,
{
    let cancel = bbdown_login_cancel_notify().notified();
    tokio::pin!(cancel);
    ensure_bbdown_login_active(auth_generation)?;
    let result = tokio::select! {
        result = future => result,
        () = &mut cancel => {
            bail!("BBDown login was canceled by a later /bbdown logout");
        }
    };
    ensure_bbdown_login_active(auth_generation)?;
    Ok(result)
}

fn ensure_bbdown_login_active(auth_generation: u64) -> Result<()> {
    if BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst) != auth_generation {
        bail!("BBDown login was canceled by a later /bbdown logout");
    }
    Ok(())
}

async fn run_bbdown_status(telegram: TelegramClient, config: Arc<AppConfig>, chat_id: i64) {
    let message = match bilibili_auth::load_auth_state(&config.bilibili.auth.state_path) {
        Ok(None) => "BBDown is not logged in. Use /bbdown login in private chat.".to_string(),
        Ok(Some(state)) => {
            let client = match bbdown_auth_client(&config) {
                Ok(client) => client,
                Err(err) => {
                    return send_or_log(
                        &telegram,
                        chat_id,
                        format!(
                            "Failed to prepare BBDown status check:\n{}",
                            truncate(&err.to_string())
                        ),
                    )
                    .await;
                }
            };
            match bilibili_auth::verify_cookie(&client, &state.cookie).await {
                Ok(verified) => {
                    format!(
                        "BBDown is logged in as {} (mid: {}).",
                        verified.uname, verified.mid
                    )
                }
                Err(err) => format!(
                    "Saved BBDown login is invalid or expired. Use /bbdown login again.\n{}",
                    truncate(&err.to_string())
                ),
            }
        }
        Err(err) => format!(
            "Failed to read BBDown login state:\n{}",
            truncate(&err.to_string())
        ),
    };
    send_or_log(&telegram, chat_id, message).await;
}

fn bbdown_auth_client(config: &AppConfig) -> Result<Client> {
    let timeout_seconds = config
        .bilibili
        .auth
        .login_timeout_seconds
        .clamp(1, BILIBILI_AUTH_MAX_HTTP_TIMEOUT_SECONDS);
    Client::builder()
        .timeout(Duration::from_secs(timeout_seconds))
        .connect_timeout(Duration::from_secs(timeout_seconds.min(10)))
        .build()
        .context("failed to create Bilibili auth HTTP client")
}

fn bbdown_auth_state_lock() -> &'static Mutex<()> {
    BILIBILI_AUTH_STATE_LOCK.get_or_init(|| Mutex::new(()))
}

fn bbdown_login_cancel_notify() -> &'static Notify {
    BILIBILI_LOGIN_CANCEL_NOTIFY.get_or_init(Notify::new)
}

async fn run_bbdown_logout(telegram: TelegramClient, config: Arc<AppConfig>, chat_id: i64) {
    BILIBILI_AUTH_GENERATION.fetch_add(1, Ordering::SeqCst);
    bbdown_login_cancel_notify().notify_waiters();
    let _state_guard = bbdown_auth_state_lock().lock().await;
    let message = match bilibili_auth::delete_auth_state(&config.bilibili.auth.state_path) {
        Ok(true) => "BBDown login state cleared.".to_string(),
        Ok(false) => "BBDown is not logged in.".to_string(),
        Err(err) => format!(
            "Failed to clear BBDown login state:\n{}",
            truncate(&err.to_string())
        ),
    };
    send_or_log(&telegram, chat_id, message).await;
}

fn bbdown_auth_usage() -> String {
    "Usage: /bbdown login | /bbdown status | /bbdown logout".to_string()
}

fn bbdown_qr_photo_failed_message() -> String {
    "Could not send the QR image. BBDown login canceled; try /bbdown login again after Telegram photo delivery is working.".to_string()
}

fn queue_or_prompt_job(
    telegram: TelegramClient,
    config: Arc<AppConfig>,
    job_dispatch: JobDispatch,
    chat_id: i64,
    job_id: u64,
    job: JobRequest,
) {
    tokio::spawn(process_job_after_duplicate_check(
        telegram,
        config,
        job_dispatch,
        chat_id,
        job_id,
        job,
    ));
}

async fn process_job_after_duplicate_check(
    telegram: TelegramClient,
    config: Arc<AppConfig>,
    job_dispatch: JobDispatch,
    chat_id: i64,
    job_id: u64,
    job: JobRequest,
) {
    if matches!(job, JobRequest::Pdf { .. }) {
        queue_job(
            telegram,
            config,
            Arc::clone(&job_dispatch.download_semaphore),
            chat_id,
            job_id,
            job,
            JobRunMode::Direct,
        )
        .await;
        return;
    }

    let duplicate_scan_permit = match Arc::clone(&job_dispatch.duplicate_scan_semaphore)
        .acquire_owned()
        .await
    {
        Ok(permit) => permit,
        Err(err) => {
            send_or_log(
                &telegram,
                chat_id,
                format!(
                    "Duplicate check unavailable for job #{job_id}; continuing without duplicate prompt.\n{}",
                    truncate(&err.to_string())
                ),
            )
            .await;
            let run_mode = default_run_mode(&job);
            queue_job(
                telegram,
                config,
                Arc::clone(&job_dispatch.download_semaphore),
                chat_id,
                job_id,
                job,
                run_mode,
            )
            .await;
            return;
        }
    };
    let duplicate_scan_result = find_video_duplicate_async(Arc::clone(&config), job.clone()).await;
    drop(duplicate_scan_permit);

    match duplicate_scan_result {
        Ok(Some(duplicate)) => {
            prompt_duplicate_choice(&telegram, chat_id, job_id, job, duplicate).await;
        }
        Ok(None) => {
            let run_mode = default_run_mode(&job);
            queue_job(
                telegram,
                config,
                Arc::clone(&job_dispatch.download_semaphore),
                chat_id,
                job_id,
                job,
                run_mode,
            )
            .await;
        }
        Err(err) => {
            send_or_log(
                &telegram,
                chat_id,
                format!(
                    "Duplicate check failed for job #{job_id}; continuing without duplicate prompt.\n{}",
                    truncate(&err.to_string())
                ),
            )
            .await;
            let run_mode = default_run_mode(&job);
            queue_job(
                telegram,
                config,
                Arc::clone(&job_dispatch.download_semaphore),
                chat_id,
                job_id,
                job,
                run_mode,
            )
            .await;
        }
    }
}

async fn find_video_duplicate_async(
    config: Arc<AppConfig>,
    job: JobRequest,
) -> Result<Option<VideoDuplicate>> {
    let scan_config = (*config).clone();
    tokio::task::spawn_blocking(move || find_video_duplicate(&scan_config, &job))
        .await
        .context("duplicate scan task failed")?
}

async fn prompt_duplicate_choice(
    telegram: &TelegramClient,
    chat_id: i64,
    job_id: u64,
    job: JobRequest,
    duplicate: VideoDuplicate,
) {
    let token = next_duplicate_callback_token(job_id);
    let prompt = duplicate_choice_message(job_id, job.label(), &duplicate);
    match telegram
        .send_message_with_inline_keyboard(
            chat_id,
            truncate(&prompt),
            duplicate_choice_keyboard(token),
        )
        .await
    {
        Ok(_) => {
            let now = Instant::now();
            let mut pending_jobs = pending_duplicate_jobs().lock().await;
            prune_expired_pending_duplicate_jobs(&mut pending_jobs, now);
            pending_jobs.insert(
                token,
                PendingDuplicateJob {
                    chat_id,
                    job_id,
                    job,
                    duplicate,
                    created_at: now,
                },
            );
            cap_pending_duplicate_jobs(&mut pending_jobs);
        }
        Err(err) => {
            warn!(chat_id, job_id, error = %err, "failed to send duplicate choice prompt");
            send_or_log(
                telegram,
                chat_id,
                format!("Duplicate found for job #{job_id}, but Telegram choice prompt failed. Job canceled; send the link again to retry."),
            )
            .await;
        }
    }
}

async fn queue_job(
    telegram: TelegramClient,
    config: Arc<AppConfig>,
    semaphore: Arc<Semaphore>,
    chat_id: i64,
    job_id: u64,
    job: JobRequest,
    run_mode: JobRunMode,
) {
    send_or_log(
        &telegram,
        chat_id,
        format!("Queued job #{job_id}: {}", job.label()),
    )
    .await;

    tokio::spawn(run_queued_job(
        telegram, config, semaphore, chat_id, job_id, job, run_mode,
    ));
}

async fn handle_callback_query(
    telegram: TelegramClient,
    config: Arc<AppConfig>,
    semaphore: Arc<Semaphore>,
    callback_query: CallbackQuery,
) {
    let callback_id = callback_query.id.clone();
    let Some(data) = callback_query.data.as_deref() else {
        answer_callback_or_log(&telegram, callback_id, "Unsupported button.".to_string()).await;
        return;
    };
    let Some(callback) = parse_duplicate_callback_data(data) else {
        answer_callback_or_log(&telegram, callback_id, "Unsupported button.".to_string()).await;
        return;
    };
    let Some(message) = callback_query.message else {
        answer_callback_or_log(
            &telegram,
            callback_id,
            "This choice has expired.".to_string(),
        )
        .await;
        return;
    };
    let chat_id = message.chat.id;
    if !config.telegram.is_chat_allowed(chat_id) {
        warn!(chat_id, "ignoring callback from unauthorized chat");
        answer_callback_or_log(&telegram, callback_id, "Unauthorized chat.".to_string()).await;
        return;
    }

    let pending = take_pending_duplicate_job(callback.token, chat_id).await;
    let Some(pending) = pending else {
        answer_callback_or_log(
            &telegram,
            callback_id,
            "This choice has expired.".to_string(),
        )
        .await;
        return;
    };

    match callback.action {
        DuplicateCallbackAction::Cancel => {
            answer_callback_or_log(&telegram, callback_id, "Canceled.".to_string()).await;
            edit_without_keyboard_or_send(
                &telegram,
                chat_id,
                message.message_id,
                format!("Canceled job #{}: {}", pending.job_id, pending.job.label()),
            )
            .await;
        }
        DuplicateCallbackAction::Run(action) => {
            let action_label = match action {
                VideoDuplicateAction::Overwrite => "overwrite",
                VideoDuplicateAction::KeepBoth => "keep both",
            };
            answer_callback_or_log(&telegram, callback_id, "Queued.".to_string()).await;
            edit_without_keyboard_or_send(
                &telegram,
                chat_id,
                message.message_id,
                format!(
                    "Selected {action_label} for job #{}: {}",
                    pending.job_id,
                    pending.job.label()
                ),
            )
            .await;
            queue_job(
                telegram,
                config,
                semaphore,
                chat_id,
                pending.job_id,
                pending.job,
                JobRunMode::Duplicate(DuplicateRun {
                    action,
                    duplicate: pending.duplicate,
                }),
            )
            .await;
        }
    }
}

fn default_run_mode(job: &JobRequest) -> JobRunMode {
    match job {
        JobRequest::Bilibili { .. } | JobRequest::Youtube { .. } => JobRunMode::StagedKeepBoth,
        JobRequest::Pdf { .. } => JobRunMode::Direct,
    }
}

impl From<DuplicateRun> for JobRunMode {
    fn from(value: DuplicateRun) -> Self {
        Self::Duplicate(value)
    }
}

fn pending_duplicate_jobs() -> &'static Mutex<HashMap<u64, PendingDuplicateJob>> {
    PENDING_DUPLICATE_JOBS.get_or_init(|| Mutex::new(HashMap::new()))
}

async fn take_pending_duplicate_job(token: u64, chat_id: i64) -> Option<PendingDuplicateJob> {
    let mut jobs = pending_duplicate_jobs().lock().await;
    prune_expired_pending_duplicate_jobs(&mut jobs, Instant::now());
    match jobs.get(&token) {
        Some(job) if job.chat_id == chat_id => jobs.remove(&token),
        _ => None,
    }
}

fn prune_expired_pending_duplicate_jobs(
    jobs: &mut HashMap<u64, PendingDuplicateJob>,
    now: Instant,
) {
    jobs.retain(|_, job| now.duration_since(job.created_at) <= DUPLICATE_DECISION_TTL);
}

fn cap_pending_duplicate_jobs(jobs: &mut HashMap<u64, PendingDuplicateJob>) {
    while jobs.len() > MAX_PENDING_DUPLICATE_JOBS {
        let Some(oldest_job_id) = jobs
            .iter()
            .min_by_key(|(_, job)| job.created_at)
            .map(|(job_id, _)| *job_id)
        else {
            break;
        };
        jobs.remove(&oldest_job_id);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DuplicateCallback {
    token: u64,
    action: DuplicateCallbackAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DuplicateCallbackAction {
    Run(VideoDuplicateAction),
    Cancel,
}

fn parse_duplicate_callback_data(data: &str) -> Option<DuplicateCallback> {
    let mut parts = data.split(':');
    let prefix = parts.next()?;
    let token = u64::from_str_radix(parts.next()?, 16).ok()?;
    let action = parts.next()?;
    if parts.next().is_some() || prefix != "dup" {
        return None;
    }
    let action = match action {
        "overwrite" => DuplicateCallbackAction::Run(VideoDuplicateAction::Overwrite),
        "keep" => DuplicateCallbackAction::Run(VideoDuplicateAction::KeepBoth),
        "cancel" => DuplicateCallbackAction::Cancel,
        _ => return None,
    };
    Some(DuplicateCallback { token, action })
}

fn next_duplicate_callback_token(job_id: u64) -> u64 {
    let counter = DUPLICATE_CALLBACK_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    nanos ^ counter.rotate_left(17) ^ job_id.rotate_left(32) ^ (std::process::id() as u64)
}

fn duplicate_callback_data(token: u64, action: &str) -> String {
    format!("dup:{token:016x}:{action}")
}

fn duplicate_choice_keyboard(token: u64) -> InlineKeyboardMarkup {
    InlineKeyboardMarkup {
        inline_keyboard: vec![
            vec![
                InlineKeyboardButton {
                    text: "Overwrite".to_string(),
                    callback_data: duplicate_callback_data(token, "overwrite"),
                },
                InlineKeyboardButton {
                    text: "Keep both".to_string(),
                    callback_data: duplicate_callback_data(token, "keep"),
                },
            ],
            vec![InlineKeyboardButton {
                text: "Cancel".to_string(),
                callback_data: duplicate_callback_data(token, "cancel"),
            }],
        ],
    }
}

fn duplicate_choice_message(job_id: u64, job_label: &str, duplicate: &VideoDuplicate) -> String {
    format!(
        "Existing video found for job #{job_id}: {job_label}\nIdentity: {} {}\n\nChoose how to handle it:\n{}",
        duplicate.identity.provider.as_str(),
        duplicate.identity.id,
        duplicate.describe_existing_videos(5)
    )
}

async fn answer_callback_or_log(
    telegram: &TelegramClient,
    callback_query_id: String,
    text: String,
) {
    if let Err(err) = telegram
        .answer_callback_query(callback_query_id, text)
        .await
    {
        warn!(error = %err, "failed to answer telegram callback query");
    }
}

async fn run_queued_job(
    telegram: TelegramClient,
    config: Arc<AppConfig>,
    semaphore: Arc<Semaphore>,
    chat_id: i64,
    job_id: u64,
    job: JobRequest,
    run_mode: JobRunMode,
) {
    let permit = match semaphore.acquire_owned().await {
        Ok(permit) => permit,
        Err(err) => {
            error!(job_id, error = %err, "job semaphore closed");
            return;
        }
    };

    let status_message_id = send_or_log_message_id(
        &telegram,
        chat_id,
        job_status_message(job_id, job.label(), "Started", None),
    )
    .await;

    let (progress_tx, progress_rx) = mpsc::unbounded_channel();
    let progress_task = tokio::spawn(forward_progress(
        telegram.clone(),
        chat_id,
        job_id,
        job.label(),
        status_message_id,
        progress_rx,
    ));
    let result = match run_mode {
        JobRunMode::Duplicate(duplicate_run) => {
            run_job_with_duplicate_action(
                &config,
                &job,
                duplicate_run.action,
                &duplicate_run.duplicate,
                Some(progress_tx),
            )
            .await
        }
        JobRunMode::StagedKeepBoth => {
            run_video_job_staged_keep_both(&config, &job, Some(progress_tx)).await
        }
        JobRunMode::Direct => run_job(&config, &job, Some(progress_tx)).await,
    };
    let _ = progress_task.await;
    drop(permit);

    let message = match result {
        Ok(report) => {
            let details = if report.details.is_empty() {
                String::new()
            } else {
                format!("\n{}", report.details)
            };
            format!(
                "Finished job #{job_id}: {}\nSaved: {}{}",
                job.label(),
                report.saved_location,
                details
            )
        }
        Err(err) => format!(
            "Failed job #{job_id}: {}\n{}",
            job.label(),
            truncate(&err.to_string())
        ),
    };

    if let Some(message_id) = status_message_id {
        edit_or_send(&telegram, chat_id, message_id, message).await;
    } else {
        send_or_log(&telegram, chat_id, message).await;
    }
}

async fn forward_progress(
    telegram: TelegramClient,
    chat_id: i64,
    job_id: u64,
    job_label: &'static str,
    status_message_id: Option<i64>,
    mut progress_rx: mpsc::UnboundedReceiver<JobProgress>,
) {
    let mut delivery = ProgressDelivery::from_message_id(status_message_id);
    while let Some(progress) = progress_rx.recv().await {
        let message = job_status_message(job_id, job_label, "Running", Some(&progress.message));
        match delivery {
            ProgressDelivery::Edit(message_id) => {
                if edit_or_log(&telegram, chat_id, message_id, message).await {
                    continue;
                }
                delivery = delivery.after_edit_result(false);
                send_or_log(
                    &telegram,
                    chat_id,
                    progress_fallback_message(job_id, &progress.message),
                )
                .await;
            }
            ProgressDelivery::Send => {
                send_or_log(
                    &telegram,
                    chat_id,
                    progress_fallback_message(job_id, &progress.message),
                )
                .await;
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProgressDelivery {
    Edit(i64),
    Send,
}

impl ProgressDelivery {
    fn from_message_id(message_id: Option<i64>) -> Self {
        message_id.map_or(Self::Send, Self::Edit)
    }

    fn after_edit_result(self, succeeded: bool) -> Self {
        if succeeded { self } else { Self::Send }
    }
}

async fn send_or_log(telegram: &TelegramClient, chat_id: i64, text: String) {
    let _ = send_or_log_message_id(telegram, chat_id, text).await;
}

async fn send_or_log_message_id(
    telegram: &TelegramClient,
    chat_id: i64,
    text: String,
) -> Option<i64> {
    match telegram.send_message(chat_id, truncate(&text)).await {
        Ok(message_id) => Some(message_id),
        Err(err) => {
            warn!(chat_id, error = %err, "failed to send telegram message");
            None
        }
    }
}

async fn edit_or_log(
    telegram: &TelegramClient,
    chat_id: i64,
    message_id: i64,
    text: String,
) -> bool {
    match telegram
        .edit_message_text(chat_id, message_id, truncate(&text))
        .await
    {
        Ok(()) => true,
        Err(err) => {
            warn!(
                chat_id,
                message_id,
                error = %err,
                "failed to edit telegram message"
            );
            false
        }
    }
}

async fn edit_or_send(telegram: &TelegramClient, chat_id: i64, message_id: i64, text: String) {
    if let Err(err) = telegram
        .edit_message_text(chat_id, message_id, truncate(&text))
        .await
    {
        warn!(
            chat_id,
            message_id,
            error = %err,
            "failed to edit telegram message; sending a new message"
        );
        send_or_log(telegram, chat_id, text).await;
    }
}

async fn edit_without_keyboard_or_send(
    telegram: &TelegramClient,
    chat_id: i64,
    message_id: i64,
    text: String,
) {
    if let Err(err) = telegram
        .edit_message_text_without_inline_keyboard(chat_id, message_id, truncate(&text))
        .await
    {
        warn!(
            chat_id,
            message_id,
            error = %err,
            "failed to edit telegram message without inline keyboard; sending a new message"
        );
        send_or_log(telegram, chat_id, text).await;
    }
}

fn job_status_message(job_id: u64, job_label: &str, state: &str, progress: Option<&str>) -> String {
    let mut message = format!("{state} job #{job_id}: {job_label}");
    if let Some(progress) = progress.filter(|progress| !progress.trim().is_empty()) {
        message.push('\n');
        message.push_str(progress);
    }
    message
}

fn progress_fallback_message(job_id: u64, progress: &str) -> String {
    format!("Progress job #{job_id}: {progress}")
}

fn truncate(text: &str) -> String {
    const MAX_CHARS: usize = 3500;
    let mut chars = text.chars();
    let truncated: String = chars.by_ref().take(MAX_CHARS).collect();
    if chars.next().is_some() {
        format!("{truncated}\n... <truncated>")
    } else {
        truncated
    }
}

fn summarize_bbdown_auth_error(error: &anyhow::Error) -> String {
    truncate(&redact_bilibili_login_qr_urls(&error.to_string()))
}

fn redact_bilibili_login_qr_urls(text: &str) -> String {
    text.lines()
        .map(|line| {
            if line.contains("passport.bilibili.com") && line.contains("qrcode_key=") {
                "<redacted Bilibili login QR URL>"
            } else {
                line
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;

    use super::*;

    #[test]
    fn redacts_bilibili_login_qr_urls_from_auth_errors() {
        let summary = summarize_bbdown_auth_error(&anyhow!(
            "failed after https://passport.bilibili.com/h5-app/passport/login/scan?qrcode_key=secret"
        ));

        assert!(!summary.contains("secret"));
        assert!(!summary.contains("qrcode_key="));
        assert!(summary.contains("<redacted Bilibili login QR URL>"));
    }

    #[test]
    fn qr_photo_failure_message_does_not_include_login_url() {
        let message = bbdown_qr_photo_failed_message();

        assert!(!message.contains("passport.bilibili.com"));
        assert!(!message.contains("qrcode_key="));
    }

    #[test]
    fn help_message_lists_supported_commands() {
        let message = help_message();

        for expected in ["/help", "/pdf URL", "/bbdown login", "/bbdown status"] {
            assert!(message.contains(expected), "missing {expected}");
        }
    }

    #[test]
    fn default_bot_commands_match_supported_commands() {
        let commands = default_bot_commands();

        assert_eq!(
            commands
                .iter()
                .map(|command| command.command.as_str())
                .collect::<Vec<_>>(),
            vec!["help", "pdf", "bbdown"]
        );
    }

    #[test]
    fn job_status_message_includes_progress_when_present() {
        assert_eq!(
            job_status_message(7, "Bilibili download", "Started", None),
            "Started job #7: Bilibili download"
        );
        assert_eq!(
            job_status_message(7, "Bilibili download", "Running", Some("BBDown: 42%")),
            "Running job #7: Bilibili download\nBBDown: 42%"
        );
    }

    #[test]
    fn progress_delivery_falls_back_to_send_after_edit_failure() {
        assert_eq!(
            ProgressDelivery::from_message_id(Some(42)),
            ProgressDelivery::Edit(42)
        );
        assert_eq!(
            ProgressDelivery::Edit(42).after_edit_result(true),
            ProgressDelivery::Edit(42)
        );
        assert_eq!(
            ProgressDelivery::Edit(42).after_edit_result(false),
            ProgressDelivery::Send
        );
        assert_eq!(
            progress_fallback_message(7, "BBDown: 42%"),
            "Progress job #7: BBDown: 42%"
        );
    }

    #[test]
    fn parses_duplicate_callback_data() {
        assert_eq!(
            parse_duplicate_callback_data("dup:000000000000002a:overwrite"),
            Some(DuplicateCallback {
                token: 42,
                action: DuplicateCallbackAction::Run(VideoDuplicateAction::Overwrite)
            })
        );
        assert_eq!(
            parse_duplicate_callback_data("dup:000000000000002a:keep"),
            Some(DuplicateCallback {
                token: 42,
                action: DuplicateCallbackAction::Run(VideoDuplicateAction::KeepBoth)
            })
        );
        assert_eq!(
            parse_duplicate_callback_data("dup:000000000000002a:cancel"),
            Some(DuplicateCallback {
                token: 42,
                action: DuplicateCallbackAction::Cancel
            })
        );
        assert_eq!(parse_duplicate_callback_data("dup:nothex:keep"), None);
        assert_eq!(parse_duplicate_callback_data("other:42:keep"), None);
        assert_eq!(parse_duplicate_callback_data("dup:42:unknown"), None);
    }

    #[test]
    fn builds_duplicate_choice_keyboard() {
        let keyboard = duplicate_choice_keyboard(42);
        let data = keyboard
            .inline_keyboard
            .iter()
            .flatten()
            .map(|button| button.callback_data.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            data,
            vec![
                "dup:000000000000002a:overwrite",
                "dup:000000000000002a:keep",
                "dup:000000000000002a:cancel"
            ]
        );
        assert!(data.iter().all(|value| value.len() <= 64));
    }

    fn pending_duplicate_job(job_id: u64, created_at: Instant) -> PendingDuplicateJob {
        PendingDuplicateJob {
            chat_id: 1,
            job_id,
            job: JobRequest::Youtube {
                url: format!("https://youtu.be/{job_id}"),
            },
            duplicate: VideoDuplicate {
                identity: crate::downloader::VideoIdentity {
                    provider: crate::downloader::VideoProvider::Youtube,
                    id: job_id.to_string(),
                },
                existing_videos: Vec::new(),
            },
            created_at,
        }
    }

    #[test]
    fn pending_duplicate_jobs_expire_and_cap() {
        let now = Instant::now();
        let expired = now
            .checked_sub(DUPLICATE_DECISION_TTL + Duration::from_secs(1))
            .expect("test instant should support subtraction");
        let mut jobs = HashMap::from([
            (1, pending_duplicate_job(1, expired)),
            (2, pending_duplicate_job(2, now)),
        ]);

        prune_expired_pending_duplicate_jobs(&mut jobs, now);
        assert!(!jobs.contains_key(&1));
        assert!(jobs.contains_key(&2));

        for index in 3..=(MAX_PENDING_DUPLICATE_JOBS as u64 + 3) {
            jobs.insert(index, pending_duplicate_job(index, now));
        }
        cap_pending_duplicate_jobs(&mut jobs);
        assert!(jobs.len() <= MAX_PENDING_DUPLICATE_JOBS);
    }

    #[tokio::test]
    async fn login_cancel_check_handles_future_notify_waiters() {
        let generation = BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst);
        BILIBILI_AUTH_GENERATION.fetch_add(1, Ordering::SeqCst);
        let result = await_bbdown_login_active(generation, async { "completed" }).await;
        BILIBILI_AUTH_GENERATION.store(generation, Ordering::SeqCst);

        let err = result.expect_err("stale generation should cancel immediately");
        assert!(err.to_string().contains("canceled"));
    }
}
