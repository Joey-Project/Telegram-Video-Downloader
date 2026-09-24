---
id: 20260924-cux01
title: Bilibili Collection Progress UX
status: completed
created: 2026-09-24
updated: 2026-09-24
branch: wip/bilibili-collection-ux
pr:
supersedes: []
superseded_by:
---

# Bilibili Collection Progress UX

## Summary
- 将合集解析结果从“所有条目流规格的去重汇总”改为分页条目清单，避免把不同视频的画质、帧率和音频码率混在同一行。
- 清单显示每个条目的标题、时长、选定视频/音频格式、预计媒体大小和同步状态，并显示本次计划下载的总估算大小。
- 下载消息拆分为合集总览与当前条目：条目开始时新建一条可编辑消息，条目下载完成后将其固定，再为下一条创建新消息。

## Current State
- BBDown-rust worker 到父进程的进度通道新增可靠生命周期事件，避免 watch 状态被连续的 `EntryCompleted` / `EntryStarted` 覆盖。
- Telegram 清单页支持 Previous / Next 内联按钮；状态仅保存在内存中，进程重启后分页按钮会自然过期。
- 条目完成表示媒体流下载完成；合集层面的本地 mux、sidecar 整理和发布仍由最终作业状态确认。
- Rust 与 Python 本地验证均已通过；持久队列/恢复没有混入本次 UX 改动。

## Next Steps
- 在部署版本上以真实 Bilibili 合集做 Telegram smoke test，检查分页、条目消息冻结和总览进度。
- 后续独立工作流实现持久任务队列与显式恢复；不能把现有自动清理的暂存目录直接当作可安全恢复状态。

## Evidence
- Rust validation: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test` (`449 passed`, `10 ignored`).
- Python validation: `uv run ruff format --check`, `uv run ruff check`, `uv run python -m unittest discover -s tests` (`20 passed`).
