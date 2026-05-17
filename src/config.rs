use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub telegram: TelegramConfig,
    #[serde(default)]
    pub downloads: DownloadsConfig,
    #[serde(default)]
    pub tools: ToolsConfig,
    #[serde(default)]
    pub bot: BotConfig,
    #[serde(skip)]
    project_dir: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TelegramConfig {
    pub token: String,
    #[serde(default)]
    pub allowed_chat_ids: Vec<i64>,
    #[serde(default)]
    pub allow_all_chats: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DownloadsConfig {
    #[serde(default = "default_video_dir")]
    pub video_dir: PathBuf,
    #[serde(default = "default_pdf_dir")]
    pub pdf_dir: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ToolsConfig {
    #[serde(default = "default_bbdown")]
    pub bbdown: PathBuf,
    #[serde(default = "default_yt_dlp")]
    pub yt_dlp: PathBuf,
    #[serde(default = "default_uv")]
    pub uv: PathBuf,
    #[serde(default = "default_pdf_helper")]
    pub pdf_helper: PathBuf,
    #[serde(default = "default_chrome")]
    pub chrome: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BotConfig {
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    #[serde(default = "default_poll_timeout_seconds")]
    pub poll_timeout_seconds: u64,
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("failed to read config file {}", path.display()))?;
        let project_dir = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        Self::from_toml_str(&content, project_dir)
    }

    pub fn ensure_runtime_dirs(&self) -> Result<()> {
        fs::create_dir_all(&self.downloads.video_dir).with_context(|| {
            format!(
                "failed to create video download directory {}",
                self.downloads.video_dir.display()
            )
        })?;
        fs::create_dir_all(&self.downloads.pdf_dir).with_context(|| {
            format!(
                "failed to create pdf download directory {}",
                self.downloads.pdf_dir.display()
            )
        })?;
        Ok(())
    }

    pub fn resolve_project_path(&self, path: &Path) -> PathBuf {
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.project_dir.join(path)
        }
    }

    fn from_toml_str(content: &str, project_dir: PathBuf) -> Result<Self> {
        let mut config: Self = toml::from_str(content).context("failed to parse config TOML")?;
        config.project_dir = project_dir;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.telegram.token.trim().is_empty() {
            bail!("telegram.token must not be empty");
        }
        if !self.telegram.allow_all_chats && self.telegram.allowed_chat_ids.is_empty() {
            bail!("telegram.allowed_chat_ids must not be empty unless allow_all_chats is true");
        }
        if self.bot.concurrency == 0 {
            bail!("bot.concurrency must be at least 1");
        }
        if self.bot.poll_timeout_seconds == 0 {
            bail!("bot.poll_timeout_seconds must be at least 1");
        }
        Ok(())
    }
}

impl TelegramConfig {
    pub fn is_chat_allowed(&self, chat_id: i64) -> bool {
        self.allow_all_chats || self.allowed_chat_ids.contains(&chat_id)
    }
}

impl Default for DownloadsConfig {
    fn default() -> Self {
        Self {
            video_dir: default_video_dir(),
            pdf_dir: default_pdf_dir(),
        }
    }
}

impl Default for ToolsConfig {
    fn default() -> Self {
        Self {
            bbdown: default_bbdown(),
            yt_dlp: default_yt_dlp(),
            uv: default_uv(),
            pdf_helper: default_pdf_helper(),
            chrome: default_chrome(),
        }
    }
}

impl Default for BotConfig {
    fn default() -> Self {
        Self {
            concurrency: default_concurrency(),
            poll_timeout_seconds: default_poll_timeout_seconds(),
        }
    }
}

fn default_video_dir() -> PathBuf {
    PathBuf::from("/Users/joey/Movies/Downloads")
}

fn default_pdf_dir() -> PathBuf {
    PathBuf::from("/Users/joey/Documents/Downloads")
}

fn default_bbdown() -> PathBuf {
    PathBuf::from("/Users/joey/.dotnet/tools/BBDown")
}

fn default_yt_dlp() -> PathBuf {
    PathBuf::from("/Users/joey/.local/bin/yt-dlp")
}

fn default_uv() -> PathBuf {
    PathBuf::from("uv")
}

fn default_pdf_helper() -> PathBuf {
    PathBuf::from("scripts/pdf_helper.py")
}

fn default_chrome() -> PathBuf {
    PathBuf::from("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
}

fn default_concurrency() -> usize {
    2
}

fn default_poll_timeout_seconds() -> u64 {
    50
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_defaults() {
        let config = AppConfig::from_toml_str(
            r#"
            [telegram]
            token = "token"
            allow_all_chats = true
            "#,
            PathBuf::from("/tmp/project"),
        )
        .expect("config should parse");

        assert_eq!(
            config.downloads.video_dir,
            PathBuf::from("/Users/joey/Movies/Downloads")
        );
        assert_eq!(
            config.downloads.pdf_dir,
            PathBuf::from("/Users/joey/Documents/Downloads")
        );
        assert_eq!(
            config.tools.pdf_helper,
            PathBuf::from("scripts/pdf_helper.py")
        );
        assert_eq!(config.bot.concurrency, 2);
        assert_eq!(config.bot.poll_timeout_seconds, 50);
    }

    #[test]
    fn rejects_zero_concurrency() {
        let err = AppConfig::from_toml_str(
            r#"
            [telegram]
            token = "token"
            allow_all_chats = true

            [bot]
            concurrency = 0
            "#,
            PathBuf::from("."),
        )
        .expect_err("zero concurrency should fail");

        assert!(err.to_string().contains("bot.concurrency"));
    }

    #[test]
    fn resolves_relative_project_path() {
        let config = AppConfig::from_toml_str(
            r#"
            [telegram]
            token = "token"
            allow_all_chats = true
            "#,
            PathBuf::from("/tmp/project"),
        )
        .expect("config should parse");

        assert_eq!(
            config.resolve_project_path(Path::new("scripts/pdf_helper.py")),
            PathBuf::from("/tmp/project/scripts/pdf_helper.py")
        );
    }

    #[test]
    fn requires_chat_allowlist_by_default() {
        let err = AppConfig::from_toml_str(
            r#"
            [telegram]
            token = "token"
            "#,
            PathBuf::from("."),
        )
        .expect_err("missing allowlist should fail");

        assert!(err.to_string().contains("telegram.allowed_chat_ids"));
    }

    #[test]
    fn checks_allowed_chat_ids() {
        let config = AppConfig::from_toml_str(
            r#"
            [telegram]
            token = "token"
            allowed_chat_ids = [10, 20]
            "#,
            PathBuf::from("."),
        )
        .expect("config should parse");

        assert!(config.telegram.is_chat_allowed(10));
        assert!(!config.telegram.is_chat_allowed(30));
    }
}
