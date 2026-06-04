# Telegram Local Downloader Bot

这是一个个人本机使用的 Telegram bot。把支持的视频链接发给 bot 后，它会调用本机 CLI 下载；对图文网页，可以使用 `/pdf URL`，也可以配置自动 PDF 域名让 bot 直接保存 PDF。

## 功能

- 普通消息中的 Bilibili 链接调用 `BBDown`，保存到视频下载目录。
- 普通消息中的 Bilibili `opus` 文章链接会规范化为 `www.bilibili.com/opus/<id>` 并保存为 PDF。
- 私聊中可以用 `/bbdown login`、`/bbdown status`、`/bbdown logout` 管理 BBDown 使用的 Bilibili 登录态。
- `/help` 会显示 bot 支持的命令；启动时也会向 Telegram 注册 slash command 提示。
- 普通消息中的 YouTube 链接调用 `yt-dlp`，保存到视频下载目录，并尽量写入 metadata、封面、字幕和媒体库 sidecar。
- 普通消息会从整段文本里扫描 HTTP(S) URL；标题、说明和 URL 外层标点会被忽略。
- `/pdf URL` 调用 uv 管理的 Python Playwright helper，使用系统 Chrome 打印 PDF；`pdf.auto_domains` 里的域名会自动走 PDF。
- 全局并发由配置控制，超出的任务会排队。
- 外部命令会流式采集 stdout/stderr，并监控输出目录文件大小；长时间无输出且无文件增长会自动失败，避免任务一直停在 `Started`。
- 任务开始后会发送一条状态消息，后续下载/混流进度会尽量通过 Telegram edit message 在同一条消息中刷新。

## 配置

复制示例配置并填入 Telegram token：

```sh
cp config.example.toml config.toml
```

`config.toml` 包含本机路径和 token，已经被 `.gitignore` 忽略。示例配置使用 `~` 表示当前用户 home；程序也支持在路径开头使用 `~`、`$HOME` 或 `${HOME}`。默认下载目录是：

- 视频：`~/Movies/Downloads`
- PDF：`~/Documents/Downloads`

`telegram.allowed_chat_ids` 必须配置为允许使用这个 bot 的 chat id。个人私聊通常是你的用户 chat id；群组使用群组 chat id。确实需要临时放开时，可以显式设置 `allow_all_chats = true`。

`pdf.auto_domains` 默认包含 `mp.weixin.qq.com`。Bilibili 视频和 YouTube 链接始终优先按视频处理，不会被 PDF 白名单吞掉；Bilibili `opus` 文章链接会自动走 PDF，并丢弃分享 query 参数。

`video.subtitle_languages` 默认按中文、英文、日语优先。YouTube 会先找人工字幕；如果这些语言没有人工字幕，再使用自动字幕。`write_nfo = true` 会为视频生成同 basename 的 `.nfo`，`keep_sidecars = true` 会让 yt-dlp 保留 `.info.json`、`.description` 和封面 sidecar。

`bilibili.extra_args` 默认包含 `--video-ascending` 和 `--skip-mux`。BBDown 负责下载音视频流，bot 再调用 `tools.ffmpeg` 做受控混流；这样混流也会受到同一套进度、idle timeout 和进程清理保护。需要追求更高码率时可以调整 `--video-ascending`，但建议保留 `--skip-mux`。

`bilibili.auth.state_path` 是 bot 管理的 Bilibili Web cookie 状态文件，默认写到 `~/.local/state/telegram-video-downloader/bilibili-auth.json`。`/bbdown login` 会发送 Bilibili 扫码二维码，登录成功后 Bilibili 下载会通过私有临时 `--config-file` 给 BBDown 注入 `--cookie`；如果视频下载目录存在 `BBDown.config`，或 `bilibili.extra_args` 显式指定了 `--config-file`，bot 会先合并原配置再追加 cookie。`/bbdown logout` 只清理本机状态，不远端注销账号。

`bot.progress_update_seconds` 控制进度回复频率；`bot.command_timeout_seconds` 是单个外部命令的总超时；`bot.command_idle_timeout_seconds` 是没有 stdout/stderr 且输出目录文件也没有增长的 idle 超时。

## 运行

```sh
cargo run -- config.toml
```

发送示例：

```text
https://www.bilibili.com/video/BV...
Title https://www.bilibili.com/video/BV...
https://youtu.be/...
/pdf https://example.com/article
https://mp.weixin.qq.com/s?...
https://m.bilibili.com/opus/1206098216310800386?share_source=COPY
/help
/bbdown status
```

本地重放一条 Telegram 文本、不走真实 Telegram API：

```sh
cargo run -- --replay-message config.toml "Title https://b23.tv/..."
```

## macOS 自启动

用户级 LaunchAgent 可以让 bot 在当前用户的 launchd session 里自动启动和保活。安装脚本会构建 release binary，并把 plist 写入 `~/Library/LaunchAgents`：

```sh
scripts/launch_agent.sh install
```

常用操作：

```sh
scripts/launch_agent.sh status
scripts/launch_agent.sh restart
scripts/launch_agent.sh logs
scripts/launch_agent.sh uninstall
```

脚本默认使用：

- label：`io.github.telegram-local-downloader.bot`
- config：`./config.toml`
- binary：`./target/release/telegram-video-downloader`
- logs：`~/Library/Logs/TelegramVideoDownloader/`
- launchd domain：`user/$(id -u)`

这些都可以通过环境变量覆盖，例如：

```sh
BOT_LABEL=com.example.telegram-downloader BOT_CONFIG=/path/to/config.toml scripts/launch_agent.sh install
```

## 验证

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
uv run ruff format --check
uv run ruff check
uv run python -m unittest discover -s tests
```
