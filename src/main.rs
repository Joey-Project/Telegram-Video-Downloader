mod config;
mod downloader;
mod router;
mod telegram;

use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

use crate::config::AppConfig;
use crate::downloader::run_job;
use crate::router::{RouteResult, route_message};
use crate::telegram::TelegramClient;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "telegram_video_downloader=info,info".into()),
        )
        .init();

    let config_path = std::env::args_os()
        .nth(1)
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

async fn handle_message(
    telegram: TelegramClient,
    config: Arc<AppConfig>,
    semaphore: Arc<Semaphore>,
    next_job_id: Arc<AtomicU64>,
    chat_id: i64,
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

    let result = run_job(&config, &job).await;
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
