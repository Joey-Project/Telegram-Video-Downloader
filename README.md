# Telegram Local Downloader Bot

这是一个个人本机使用的 Telegram bot。把支持的视频链接发给 bot 后，它会调用本机 CLI 下载；对图文网页，使用 `/pdf URL` 让 Chrome 加载页面并保存成 PDF。

## 功能

- 普通消息中的 Bilibili 链接调用 `BBDown`，保存到视频下载目录。
- 普通消息中的 YouTube 链接调用 `yt-dlp`，保存到视频下载目录。
- `/pdf URL` 调用 uv 管理的 Python Playwright helper，使用系统 Chrome 打印 PDF。
- 全局并发由配置控制，超出的任务会排队。

## 配置

复制示例配置并填入 Telegram token：

```sh
cp config.example.toml config.toml
```

`config.toml` 包含本机路径和 token，已经被 `.gitignore` 忽略。默认目录是：

- 视频：`/Users/joey/Movies/Downloads`
- PDF：`/Users/joey/Documents/Downloads`

`telegram.allowed_chat_ids` 必须配置为允许使用这个 bot 的 chat id。个人私聊通常是你的用户 chat id；群组使用群组 chat id。确实需要临时放开时，可以显式设置 `allow_all_chats = true`。

## 运行

```sh
cargo run -- config.toml
```

发送示例：

```text
https://www.bilibili.com/video/BV...
https://youtu.be/...
/pdf https://example.com/article
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
