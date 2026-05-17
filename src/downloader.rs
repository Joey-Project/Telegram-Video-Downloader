use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use tokio::process::Command;

use crate::config::AppConfig;
use crate::router::JobRequest;

#[derive(Debug, Clone)]
pub struct JobReport {
    pub saved_location: String,
    pub details: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSpec {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub cwd: PathBuf,
}

pub async fn run_job(config: &AppConfig, job: &JobRequest) -> Result<JobReport> {
    let spec = command_spec(config, job);
    let output = Command::new(&spec.program)
        .args(&spec.args)
        .current_dir(&spec.cwd)
        .kill_on_drop(true)
        .output()
        .await
        .with_context(|| format!("failed to run {}", spec.program.display()))?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !output.status.success() {
        bail!(
            "{} exited with status {}\n{}",
            spec.program.display(),
            output.status,
            summarize_output(&stdout, &stderr)
        );
    }

    Ok(match job {
        JobRequest::Bilibili { .. } => JobReport {
            saved_location: config.downloads.video_dir.display().to_string(),
            details: tail_lines(&stdout, 6),
        },
        JobRequest::Youtube { .. } => {
            let saved_location = last_nonempty_line(&stdout)
                .filter(|line| Path::new(line).is_absolute())
                .map(str::to_string)
                .unwrap_or_else(|| config.downloads.video_dir.display().to_string());
            JobReport {
                saved_location,
                details: tail_lines(&stderr, 6),
            }
        }
        JobRequest::Pdf { .. } => {
            let saved_location = last_nonempty_line(&stdout)
                .ok_or_else(|| anyhow!("pdf helper finished without printing output path"))?
                .to_string();
            JobReport {
                saved_location,
                details: tail_lines(&stderr, 6),
            }
        }
    })
}

pub fn command_spec(config: &AppConfig, job: &JobRequest) -> CommandSpec {
    match job {
        JobRequest::Bilibili { url } => CommandSpec {
            program: config.tools.bbdown.clone(),
            args: vec![url.clone()],
            cwd: config.downloads.video_dir.clone(),
        },
        JobRequest::Youtube { url } => CommandSpec {
            program: config.tools.yt_dlp.clone(),
            args: vec![
                "--no-playlist".to_string(),
                "-P".to_string(),
                config.downloads.video_dir.display().to_string(),
                "--print".to_string(),
                "after_move:filepath".to_string(),
                url.clone(),
            ],
            cwd: config.downloads.video_dir.clone(),
        },
        JobRequest::Pdf { url } => CommandSpec {
            program: config.tools.uv.clone(),
            args: vec![
                "run".to_string(),
                "python".to_string(),
                config
                    .resolve_project_path(&config.tools.pdf_helper)
                    .display()
                    .to_string(),
                "--url".to_string(),
                url.clone(),
                "--output-dir".to_string(),
                config.downloads.pdf_dir.display().to_string(),
                "--chrome".to_string(),
                config.tools.chrome.display().to_string(),
            ],
            cwd: config.resolve_project_path(Path::new(".")),
        },
    }
}

fn summarize_output(stdout: &str, stderr: &str) -> String {
    let stderr_tail = tail_lines(stderr, 10);
    let stdout_tail = tail_lines(stdout, 10);
    match (stderr_tail.is_empty(), stdout_tail.is_empty()) {
        (true, true) => "no command output captured".to_string(),
        (false, true) => format!("stderr:\n{stderr_tail}"),
        (true, false) => format!("stdout:\n{stdout_tail}"),
        (false, false) => format!("stderr:\n{stderr_tail}\nstdout:\n{stdout_tail}"),
    }
}

fn last_nonempty_line(text: &str) -> Option<&str> {
    text.lines()
        .rev()
        .map(str::trim)
        .find(|line| !line.is_empty())
}

fn tail_lines(text: &str, max_lines: usize) -> String {
    let lines: Vec<_> = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect();
    let start = lines.len().saturating_sub(max_lines);
    lines[start..].join("\n")
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::config::AppConfig;

    use super::*;

    fn test_config() -> AppConfig {
        AppConfig::load(Path::new("config.example.toml")).expect("example config should parse")
    }

    #[test]
    fn builds_youtube_command() {
        let config = test_config();
        let spec = command_spec(
            &config,
            &JobRequest::Youtube {
                url: "https://youtu.be/abc".to_string(),
            },
        );

        assert_eq!(spec.program, PathBuf::from("/Users/joey/.local/bin/yt-dlp"));
        assert!(spec.args.contains(&"--no-playlist".to_string()));
        assert!(spec.args.contains(&"after_move:filepath".to_string()));
        assert_eq!(spec.cwd, PathBuf::from("/Users/joey/Movies/Downloads"));
    }

    #[test]
    fn builds_pdf_command_with_uv() {
        let config = test_config();
        let spec = command_spec(
            &config,
            &JobRequest::Pdf {
                url: "https://example.com".to_string(),
            },
        );

        assert_eq!(spec.program, PathBuf::from("uv"));
        assert_eq!(spec.args[0], "run");
        assert_eq!(spec.args[1], "python");
        assert!(
            spec.args
                .iter()
                .any(|arg| arg.ends_with("scripts/pdf_helper.py"))
        );
        assert!(spec.args.contains(&"--chrome".to_string()));
    }

    #[test]
    fn tails_nonempty_lines() {
        assert_eq!(tail_lines("a\n\nb\nc\n", 2), "b\nc");
    }
}
