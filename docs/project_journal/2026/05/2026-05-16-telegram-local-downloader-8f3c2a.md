---
id: 20260516-8f3c2a
title: Telegram Local Downloader Bot
status: active
created: 2026-05-16
updated: 2026-05-16
branch:
pr:
supersedes: []
superseded_by:
---

# Telegram Local Downloader Bot

## Summary
- 第一版目标是个人本机长驻 Telegram bot：把支持的链接发给 bot 后，本机自动保存视频或网页 PDF。
- 主服务使用 Rust；PDF 打印 helper 使用 uv 管理的 Python Playwright，以降低 Chrome 自动化和 lazy-loading 滚动处理成本。
- 真实 `config.toml` 会包含 Telegram token，但必须被忽略；仓库只提交 `config.example.toml`。

## Confirmed Design
- Bilibili 路由：普通消息里的 `bilibili.com`、其子域名、`b23.tv` 链接调用本机 `BBDown`，工作目录为 `/Users/joey/Movies/Downloads`。
- YouTube 路由：普通消息里的 `youtube.com`、其常见子域名、`youtu.be` 等链接调用本机 `yt-dlp`，输出目录为 `/Users/joey/Movies/Downloads`。
- PDF 路由：图文网页必须显式使用 `/pdf URL`；helper 使用 Chrome 打开页面、逐步下滑到高度稳定后保存 PDF 到 `/Users/joey/Documents/Downloads`。
- 任务调度：全局并发限制由配置控制，默认 `2`；超过限制的任务排队。
- 状态反馈：bot 对每个任务回复已入队、开始处理、成功保存位置或失败摘要。
- 第一版不内置 cookie 管理；BBDown 和 yt-dlp 沿用本机 CLI 既有登录/cookie 配置。

## Implementation Plan
- 搭建 Rust 项目，包含配置加载、Telegram `getUpdates` polling、URL 路由、全局 semaphore 队列和外部命令执行。
- 搭建 uv Python 项目，提供 `scripts/pdf_helper.py` 和最小单元测试；依赖 Playwright 与 ruff。
- 提供 `config.example.toml`、`.gitignore` 和 README，明确 token、本机路径、CLI 路径、运行与验证方法。
- 实现 Rust 单元测试覆盖路由、命令分类和配置默认值；Python 单元测试覆盖 PDF 文件名清理等纯函数。

## Validation Plan
- Rust: `cargo fmt --check`
- Rust: `cargo clippy --all-targets -- -D warnings`
- Rust: `cargo test`
- Python: `uv run ruff format --check`
- Python: `uv run ruff check`
- Python: `uv run python -m unittest`

## Current State
- 设计与计划已记录在 journal 中。
- 下一步是在一个后续提交中实现项目骨架、bot 逻辑、PDF helper、README 和测试。

## Next Steps
- 提交本 journal checkpoint。
- 继续实现代码与测试。
- 运行验证；如 Cargo/uv 需要网络或沙箱外缓存访问，按需申请窄授权。

## Evidence
- 本机已确认存在 `BBDown`、`yt-dlp`、Chrome、Rust/Cargo、clippy、rustfmt、uv、pnpm、ffmpeg。
- 计划来源：用户在 2026-05-16 要求实现 Telegram Local Downloader Bot，并随后要求先按 project-journal 记录设计和计划并提交。
