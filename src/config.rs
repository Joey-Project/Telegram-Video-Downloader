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
    pub pdf: PdfConfig,
    #[serde(default)]
    pub video: VideoConfig,
    #[serde(default)]
    pub bilibili: BilibiliConfig,
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
    #[serde(default = "default_ffmpeg")]
    pub ffmpeg: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PdfConfig {
    #[serde(default = "default_auto_pdf_domains")]
    pub auto_domains: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VideoConfig {
    #[serde(default = "default_subtitle_languages")]
    pub subtitle_languages: Vec<String>,
    #[serde(default = "default_true")]
    pub write_nfo: bool,
    #[serde(default = "default_true")]
    pub keep_sidecars: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BilibiliConfig {
    #[serde(default = "default_bilibili_extra_args")]
    pub extra_args: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BotConfig {
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    #[serde(default = "default_poll_timeout_seconds")]
    pub poll_timeout_seconds: u64,
    #[serde(default = "default_progress_update_seconds")]
    pub progress_update_seconds: u64,
    #[serde(default = "default_command_timeout_seconds")]
    pub command_timeout_seconds: u64,
    #[serde(default = "default_command_idle_timeout_seconds")]
    pub command_idle_timeout_seconds: u64,
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
        if self.bot.progress_update_seconds == 0 {
            bail!("bot.progress_update_seconds must be at least 1");
        }
        if self.bot.command_timeout_seconds == 0 {
            bail!("bot.command_timeout_seconds must be at least 1");
        }
        if self.bot.command_idle_timeout_seconds == 0 {
            bail!("bot.command_idle_timeout_seconds must be at least 1");
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
            ffmpeg: default_ffmpeg(),
        }
    }
}

impl Default for PdfConfig {
    fn default() -> Self {
        Self {
            auto_domains: default_auto_pdf_domains(),
        }
    }
}

impl Default for VideoConfig {
    fn default() -> Self {
        Self {
            subtitle_languages: default_subtitle_languages(),
            write_nfo: true,
            keep_sidecars: true,
        }
    }
}

impl Default for BotConfig {
    fn default() -> Self {
        Self {
            concurrency: default_concurrency(),
            poll_timeout_seconds: default_poll_timeout_seconds(),
            progress_update_seconds: default_progress_update_seconds(),
            command_timeout_seconds: default_command_timeout_seconds(),
            command_idle_timeout_seconds: default_command_idle_timeout_seconds(),
        }
    }
}

impl Default for BilibiliConfig {
    fn default() -> Self {
        Self {
            extra_args: default_bilibili_extra_args(),
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

fn default_ffmpeg() -> PathBuf {
    PathBuf::from("/opt/homebrew/bin/ffmpeg")
}

fn default_concurrency() -> usize {
    2
}

fn default_poll_timeout_seconds() -> u64 {
    50
}

fn default_progress_update_seconds() -> u64 {
    30
}

fn default_command_timeout_seconds() -> u64 {
    7200
}

fn default_command_idle_timeout_seconds() -> u64 {
    300
}

fn default_auto_pdf_domains() -> Vec<String> {
    vec!["mp.weixin.qq.com".to_string()]
}

fn default_subtitle_languages() -> Vec<String> {
    ["zh-Hans", "zh-Hant", "zh", "zh-CN", "zh-TW", "en", "ja"]
        .into_iter()
        .map(str::to_string)
        .collect()
}

fn default_true() -> bool {
    true
}

fn default_bilibili_extra_args() -> Vec<String> {
    vec!["--video-ascending".to_string(), "--skip-mux".to_string()]
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
        assert_eq!(
            config.tools.ffmpeg,
            PathBuf::from("/opt/homebrew/bin/ffmpeg")
        );
        assert_eq!(config.bot.concurrency, 2);
        assert_eq!(config.bot.poll_timeout_seconds, 50);
        assert_eq!(config.bot.progress_update_seconds, 30);
        assert_eq!(config.bot.command_timeout_seconds, 7200);
        assert_eq!(config.bot.command_idle_timeout_seconds, 300);
        assert_eq!(config.pdf.auto_domains, vec!["mp.weixin.qq.com"]);
        assert_eq!(
            config.video.subtitle_languages,
            vec!["zh-Hans", "zh-Hant", "zh", "zh-CN", "zh-TW", "en", "ja"]
        );
        assert!(config.video.write_nfo);
        assert!(config.video.keep_sidecars);
        assert_eq!(
            config.bilibili.extra_args,
            vec!["--video-ascending", "--skip-mux"]
        );
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
    fn rejects_zero_command_timeout() {
        let err = AppConfig::from_toml_str(
            r#"
            [telegram]
            token = "token"
            allow_all_chats = true

            [bot]
            command_timeout_seconds = 0
            "#,
            PathBuf::from("."),
        )
        .expect_err("zero command timeout should fail");

        assert!(err.to_string().contains("bot.command_timeout_seconds"));
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
