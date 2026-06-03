mod bilibili_auth;
mod config;
mod downloader;
mod router;
mod telegram;

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
use crate::downloader::{JobProgress, run_job};
use crate::router::{BilibiliAuthCommand, RouteResult, route_message};
use crate::telegram::TelegramClient;

static BILIBILI_LOGIN_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static BILIBILI_LOGIN_CANCEL_NOTIFY: OnceLock<Notify> = OnceLock::new();
static BILIBILI_AUTH_STATE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static BILIBILI_AUTH_GENERATION: AtomicU64 = AtomicU64::new(0);
const BILIBILI_AUTH_MAX_HTTP_TIMEOUT_SECONDS: u64 = 30;

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
    let semaphore = Arc::new(Semaphore::new(config.bot.concurrency));
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
                                    Arc::clone(&semaphore),
                                    Arc::clone(&next_job_id),
                                    message.chat.id,
                                    message.chat.is_private(),
                                    message.text.as_deref(),
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
                let result = run_job(&config, job, Some(progress_tx)).await;
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
    semaphore: Arc<Semaphore>,
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
                send_or_log(
                    &telegram,
                    chat_id,
                    format!("Queued job #{job_id}: {}", job.label()),
                )
                .await;

                tokio::spawn(run_queued_job(
                    telegram.clone(),
                    Arc::clone(&config),
                    Arc::clone(&semaphore),
                    chat_id,
                    job_id,
                    job,
                ));
            }
        }
        RouteResult::BilibiliAuth(command) => {
            handle_bilibili_auth_command(telegram, config, chat_id, is_private_chat, command).await;
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
        Err(err) => format!("BBDown login failed:\n{}", truncate(&err.to_string())),
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
    let login_qr = bilibili_auth::generate_login_qr(&client).await?;
    let caption = format!(
        "Scan this Bilibili QR code in the app to authorize BBDown. It expires in {} seconds.",
        config.bilibili.auth.login_timeout_seconds
    );
    if let Err(err) = telegram
        .send_photo(chat_id, caption, login_qr.png.clone())
        .await
    {
        warn!(chat_id, error = %err, "failed to send BBDown login QR image");
        send_or_log(
            telegram,
            chat_id,
            format!(
                "Could not send the QR image. Open this URL to scan instead:\n{}",
                login_qr.url
            ),
        )
        .await;
    }

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
        let poll = tokio::select! {
            result = tokio_timeout(
                remaining,
                bilibili_auth::poll_login(&client, &login_qr.qrcode_key),
            ) => result.context("Bilibili login QR timed out")??,
            () = bbdown_login_cancel_notify().notified() => {
                bail!("BBDown login was canceled by a later /bbdown logout");
            }
        };

        match poll {
            bilibili_auth::LoginPoll::Waiting => {}
            bilibili_auth::LoginPoll::Scanned => {
                if !sent_scanned_notice {
                    sent_scanned_notice = true;
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
        tokio::select! {
            () = sleep(poll_interval.min(deadline - now)) => {}
            () = bbdown_login_cancel_notify().notified() => {
                bail!("BBDown login was canceled by a later /bbdown logout");
            }
        }
    }
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

async fn run_queued_job(
    telegram: TelegramClient,
    config: Arc<AppConfig>,
    semaphore: Arc<Semaphore>,
    chat_id: i64,
    job_id: u64,
    job: router::JobRequest,
) {
    let permit = match semaphore.acquire_owned().await {
        Ok(permit) => permit,
        Err(err) => {
            error!(job_id, error = %err, "job semaphore closed");
            return;
        }
    };

    send_or_log(
        &telegram,
        chat_id,
        format!("Started job #{job_id}: {}", job.label()),
    )
    .await;

    let (progress_tx, progress_rx) = mpsc::unbounded_channel();
    let progress_task = tokio::spawn(forward_progress(
        telegram.clone(),
        chat_id,
        job_id,
        progress_rx,
    ));
    let result = run_job(&config, &job, Some(progress_tx)).await;
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

    send_or_log(&telegram, chat_id, message).await;
}

async fn forward_progress(
    telegram: TelegramClient,
    chat_id: i64,
    job_id: u64,
    mut progress_rx: mpsc::UnboundedReceiver<JobProgress>,
) {
    while let Some(progress) = progress_rx.recv().await {
        send_or_log(
            &telegram,
            chat_id,
            format!("Progress job #{job_id}: {}", progress.message),
        )
        .await;
    }
}

async fn send_or_log(telegram: &TelegramClient, chat_id: i64, text: String) {
    if let Err(err) = telegram.send_message(chat_id, truncate(&text)).await {
        warn!(chat_id, error = %err, "failed to send telegram message");
    }
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
