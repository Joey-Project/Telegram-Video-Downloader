# Project State

## Current State
- Telegram 本地下载 bot 已实现第二轮增强；当前工作树正在把 BBDown-rust Bilibili 迁移从 CLI 调用改为直接 `bbdown-core` crate API：全文 URL 扫描、微信文章自动 PDF 白名单、YouTube metadata/封面/字幕/sidecar、Bilibili 番剧/intl 下载、BBDown-rust web/tv/access-key 登录管理、外部命令进度转发、文件活性监控和超时保护。
- 最新 workstream 记录在 `docs/project_journal/2026/06/2026-06-18-bbdown-rust-migration-bbd04f.md`；原始 bot workstream 记录在 `docs/project_journal/2026/05/2026-05-16-telegram-local-downloader-8f3c2a.md`。

## Recovery Pointers
- Latest workstream: `docs/project_journal/2026/06/2026-06-18-bbdown-rust-migration-bbd04f.md`
- Base bot workstream: `docs/project_journal/2026/05/2026-05-16-telegram-local-downloader-8f3c2a.md`

## Global Blockers
- 暂无 repo-wide blocker；最终 live Telegram 验收仍需要使用本机真实 `config.toml`、Telegram 消息流和 Bilibili app 扫码运行。

## Notes
- 普通任务进展写入 workstream journal；此文件只保留仓库级恢复入口。
