use std::path::{Path, PathBuf};
use std::{env, fs};

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
    #[serde(default)]
    pub auth: BilibiliAuthConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BilibiliAuthConfig {
    #[serde(default = "default_bilibili_auth_state_path")]
    pub state_path: PathBuf,
    #[serde(default = "default_bilibili_login_timeout_seconds")]
    pub login_timeout_seconds: u64,
    #[serde(default = "default_bilibili_poll_interval_seconds")]
    pub poll_interval_seconds: u64,
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
        let config_path = fs::canonicalize(path)
            .with_context(|| format!("failed to resolve config file {}", path.display()))?;
        let project_dir = config_path
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
        config.expand_config_paths();
        config.validate()?;
        Ok(config)
    }

    fn expand_config_paths(&mut self) {
        self.downloads.video_dir = expand_home_path(&self.downloads.video_dir);
        self.downloads.pdf_dir = expand_home_path(&self.downloads.pdf_dir);
        self.tools.bbdown = expand_home_path(&self.tools.bbdown);
        self.tools.yt_dlp = expand_home_path(&self.tools.yt_dlp);
        self.tools.uv = expand_home_path(&self.tools.uv);
        self.tools.pdf_helper = expand_home_path(&self.tools.pdf_helper);
        self.tools.chrome = expand_home_path(&self.tools.chrome);
        self.tools.ffmpeg = expand_home_path(&self.tools.ffmpeg);
        let state_path = expand_home_path(&self.bilibili.auth.state_path);
        self.bilibili.auth.state_path = self.resolve_project_path(&state_path);
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
        if self.bilibili.auth.login_timeout_seconds == 0 {
            bail!("bilibili.auth.login_timeout_seconds must be at least 1");
        }
        if self.bilibili.auth.poll_interval_seconds == 0 {
            bail!("bilibili.auth.poll_interval_seconds must be at least 1");
        }
        if self.bilibili.auth.poll_interval_seconds >= self.bilibili.auth.login_timeout_seconds {
            bail!(
                "bilibili.auth.poll_interval_seconds must be less than bilibili.auth.login_timeout_seconds"
            );
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
            auth: BilibiliAuthConfig::default(),
        }
    }
}

impl Default for BilibiliAuthConfig {
    fn default() -> Self {
        Self {
            state_path: default_bilibili_auth_state_path(),
            login_timeout_seconds: default_bilibili_login_timeout_seconds(),
            poll_interval_seconds: default_bilibili_poll_interval_seconds(),
        }
    }
}

fn default_video_dir() -> PathBuf {
    home_path(&["Movies", "Downloads"], "video-downloads")
}

fn default_pdf_dir() -> PathBuf {
    home_path(&["Documents", "Downloads"], "pdf-downloads")
}

fn default_bbdown() -> PathBuf {
    PathBuf::from("BBDown")
}

fn default_yt_dlp() -> PathBuf {
    PathBuf::from("yt-dlp")
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
    PathBuf::from("ffmpeg")
}

fn home_path(parts: &[&str], fallback: &str) -> PathBuf {
    let Some(mut path) = home_dir() else {
        return PathBuf::from(fallback);
    };

    for part in parts {
        path.push(part);
    }
    path
}

fn home_dir() -> Option<PathBuf> {
    env::var_os("HOME")
        .filter(|home| !home.is_empty())
        .map(PathBuf::from)
}

fn expand_home_path(path: &Path) -> PathBuf {
    let text = path.to_string_lossy();
    let Some(home) = home_dir() else {
        return path.to_path_buf();
    };

    let value = text.as_ref();
    if matches!(value, "~" | "$HOME" | "${HOME}") {
        return home;
    }

    for prefix in ["~/", "$HOME/", "${HOME}/"] {
        if let Some(suffix) = value.strip_prefix(prefix) {
            return home.join(suffix);
        }
    }

    path.to_path_buf()
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
    vec![
        "--video-ascending".to_string(),
        "--skip-mux".to_string(),
        "--multi-thread".to_string(),
        "false".to_string(),
    ]
}

fn default_bilibili_auth_state_path() -> PathBuf {
    home_path(
        &[
            ".local",
            "state",
            "telegram-video-downloader",
            "bilibili-auth.json",
        ],
        "bilibili-auth.json",
    )
}

fn default_bilibili_login_timeout_seconds() -> u64 {
    180
}

fn default_bilibili_poll_interval_seconds() -> u64 {
    2
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use super::*;

    struct CurrentDirGuard {
        original: PathBuf,
    }

    impl CurrentDirGuard {
        fn change_to(path: &Path) -> Self {
            let original = env::current_dir().expect("current dir should be available");
            env::set_current_dir(path).expect("current dir should change");
            Self { original }
        }
    }

    impl Drop for CurrentDirGuard {
        fn drop(&mut self) {
            let _ = env::set_current_dir(&self.original);
        }
    }

    fn temp_test_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after UNIX_EPOCH")
            .as_nanos();
        env::temp_dir().join(format!("telegram-video-downloader-config-{label}-{nanos}"))
    }

    #[test]
    fn loads_defaults() {
        let home = home_dir().expect("HOME should be set during tests");
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
            home.join("Movies").join("Downloads")
        );
        assert_eq!(
            config.downloads.pdf_dir,
            home.join("Documents").join("Downloads")
        );
        assert_eq!(config.tools.bbdown, PathBuf::from("BBDown"));
        assert_eq!(config.tools.yt_dlp, PathBuf::from("yt-dlp"));
        assert_eq!(
            config.tools.pdf_helper,
            PathBuf::from("scripts/pdf_helper.py")
        );
        assert_eq!(config.tools.ffmpeg, PathBuf::from("ffmpeg"));
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
            vec!["--video-ascending", "--skip-mux", "--multi-thread", "false"]
        );
        assert_eq!(
            config.bilibili.auth.state_path,
            home.join(".local")
                .join("state")
                .join("telegram-video-downloader")
                .join("bilibili-auth.json")
        );
        assert_eq!(config.bilibili.auth.login_timeout_seconds, 180);
        assert_eq!(config.bilibili.auth.poll_interval_seconds, 2);
    }

    #[test]
    fn preserves_explicit_bilibili_multi_thread_setting() {
        let config = AppConfig::from_toml_str(
            r#"
            [telegram]
            token = "token"
            allow_all_chats = true

            [bilibili]
            extra_args = ["--video-ascending", "--skip-mux", "--multi-thread", "true"]
            "#,
            PathBuf::from("/tmp/project"),
        )
        .expect("config should parse");

        assert_eq!(
            config.bilibili.extra_args,
            vec!["--video-ascending", "--skip-mux", "--multi-thread", "true"]
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
    fn rejects_zero_bilibili_auth_timeout() {
        let err = AppConfig::from_toml_str(
            r#"
            [telegram]
            token = "token"
            allow_all_chats = true

            [bilibili.auth]
            login_timeout_seconds = 0
            "#,
            PathBuf::from("."),
        )
        .expect_err("zero auth timeout should fail");

        assert!(
            err.to_string()
                .contains("bilibili.auth.login_timeout_seconds")
        );
    }

    #[test]
    fn rejects_bilibili_auth_poll_interval_at_or_above_timeout() {
        let err = AppConfig::from_toml_str(
            r#"
            [telegram]
            token = "token"
            allow_all_chats = true

            [bilibili.auth]
            login_timeout_seconds = 5
            poll_interval_seconds = 5
            "#,
            PathBuf::from("."),
        )
        .expect_err("slow auth polling should fail");

        assert!(
            err.to_string()
                .contains("bilibili.auth.poll_interval_seconds")
        );
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
    fn resolves_relative_bilibili_auth_state_path_to_project_dir() {
        let config = AppConfig::from_toml_str(
            r#"
            [telegram]
            token = "token"
            allow_all_chats = true

            [bilibili.auth]
            state_path = "state/bilibili-auth.json"
            "#,
            PathBuf::from("/tmp/project"),
        )
        .expect("config should parse");

        assert_eq!(
            config.bilibili.auth.state_path,
            PathBuf::from("/tmp/project/state/bilibili-auth.json")
        );
    }

    #[test]
    fn load_resolves_relative_config_and_auth_state_to_absolute_paths() {
        let root = temp_test_dir("relative-load");
        fs::create_dir_all(&root).expect("temp config dir should be created");
        fs::write(
            root.join("config.toml"),
            r#"
            [telegram]
            token = "token"
            allow_all_chats = true

            [bilibili.auth]
            state_path = "state/bilibili-auth.json"
            "#,
        )
        .expect("config should be written");
        let expected_root = fs::canonicalize(&root).expect("temp config dir should canonicalize");
        let guard = CurrentDirGuard::change_to(&root);

        let config = AppConfig::load(Path::new("config.toml")).expect("config should load");

        assert!(config.bilibili.auth.state_path.is_absolute());
        assert_eq!(
            config.bilibili.auth.state_path,
            expected_root.join("state/bilibili-auth.json")
        );
        drop(guard);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn expands_home_paths() {
        let home = home_dir().expect("HOME should be set during tests");
        let config = AppConfig::from_toml_str(
            r#"
            [telegram]
            token = "token"
            allow_all_chats = true

            [downloads]
            video_dir = "~/Movies/Bot"
            pdf_dir = "$HOME/Documents/Bot"

            [tools]
            bbdown = "${HOME}/.dotnet/tools/BBDown"

            [bilibili.auth]
            state_path = "~/Library/Application Support/Bot/bilibili-auth.json"
            "#,
            PathBuf::from("."),
        )
        .expect("config should parse");

        assert_eq!(config.downloads.video_dir, home.join("Movies").join("Bot"));
        assert_eq!(config.downloads.pdf_dir, home.join("Documents").join("Bot"));
        assert_eq!(
            config.tools.bbdown,
            home.join(".dotnet").join("tools").join("BBDown")
        );
        assert_eq!(
            config.bilibili.auth.state_path,
            home.join("Library")
                .join("Application Support")
                .join("Bot")
                .join("bilibili-auth.json")
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
