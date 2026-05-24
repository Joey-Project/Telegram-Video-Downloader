# Telegram Local Downloader Bot

这是一个个人本机使用的 Telegram bot。把支持的视频链接发给 bot 后，它会调用本机 CLI 下载；对图文网页，可以使用 `/pdf URL`，也可以配置自动 PDF 域名让 bot 直接保存 PDF。

## 功能

- 普通消息中的 Bilibili 链接调用 `BBDown`，保存到视频下载目录。
- 普通消息中的 YouTube 链接调用 `yt-dlp`，保存到视频下载目录，并尽量写入 metadata、封面、字幕和媒体库 sidecar。
- 普通消息会从整段文本里扫描 HTTP(S) URL；标题、说明和 URL 外层标点会被忽略。
- `/pdf URL` 调用 uv 管理的 Python Playwright helper，使用系统 Chrome 打印 PDF；`pdf.auto_domains` 里的域名会自动走 PDF。
- 全局并发由配置控制，超出的任务会排队。
- 外部命令会流式采集 stdout/stderr，并监控输出目录文件大小；长时间无输出且无文件增长会自动失败，避免任务一直停在 `Started`。

## 配置

复制示例配置并填入 Telegram token：

```sh
cp config.example.toml config.toml
```

`config.toml` 包含本机路径和 token，已经被 `.gitignore` 忽略。默认目录是：

- 视频：`/Users/joey/Movies/Downloads`
- PDF：`/Users/joey/Documents/Downloads`

`telegram.allowed_chat_ids` 必须配置为允许使用这个 bot 的 chat id。个人私聊通常是你的用户 chat id；群组使用群组 chat id。确实需要临时放开时，可以显式设置 `allow_all_chats = true`。

`pdf.auto_domains` 默认包含 `mp.weixin.qq.com`。Bilibili 和 YouTube 链接始终优先按视频处理，不会被 PDF 白名单吞掉。

`video.subtitle_languages` 默认按中文、英文、日语优先。YouTube 会先找人工字幕；如果这些语言没有人工字幕，再使用自动字幕。`write_nfo = true` 会为视频生成同 basename 的 `.nfo`，`keep_sidecars = true` 会让 yt-dlp 保留 `.info.json`、`.description` 和封面 sidecar。

`bilibili.extra_args` 默认包含 `--video-ascending`。这会让 BBDown 在同一清晰度下优先选择更小的视频编码；对后台非 TTY 下载更稳。需要追求更高码率时可以在 `config.toml` 里设置为空数组。

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
```

本地重放一条 Telegram 文本、不走真实 Telegram API：

```sh
cargo run -- --replay-message config.toml "Title https://b23.tv/..."
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
