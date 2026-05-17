# Project State

## Current State
- Telegram 本地下载 bot 第一版已实现：Rust 主服务负责 Telegram polling、路由、队列和下载命令；uv Python helper 负责 Chrome PDF 打印。
- 当前 workstream 记录在 `docs/project_journal/2026/05/2026-05-16-telegram-local-downloader-8f3c2a.md`。

## Recovery Pointers
- Active workstream: `docs/project_journal/2026/05/2026-05-16-telegram-local-downloader-8f3c2a.md`

## Global Blockers
- 暂无 repo-wide blocker；live Telegram 验收需要本机 `config.toml` 填入真实 bot token 后运行。

## Notes
- 普通任务进展写入 workstream journal；此文件只保留仓库级恢复入口。
