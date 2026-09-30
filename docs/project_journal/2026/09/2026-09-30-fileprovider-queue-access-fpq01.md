---
id: 20260930-fpq01
title: File Provider 队列访问
status: completed
created: 2026-09-30
updated: 2026-09-30
branch: wip/fileprovider-queue-hydration
pr:
supersedes: []
superseded_by:
---

# File Provider 队列访问

## 摘要
- macOS 队列元数据通过 `NSFileCoordinator` 读写，使 File Provider 能在访问回调内按需物化云端内容。
- 保留 `RootedFs` 的路径约束、私有权限和描述符身份校验；发生协调错误时返回可重试提示，不重建或删除原队列数据。

## 当前状态
- 文件对象在协调回调内部绑定；占位文件物化前后的 inode 变化不作为内容篡改判断，现有描述符校验仍保护实际读取的对象和内容。
- mock Telegram E2E 覆盖一次 `EDEADLK`、后续 update 继续处理、用户看到 Finder 下载指引以及 `/queue` 重试成功。
- Rust CI 的 `cargo test --all-targets --quiet` 已覆盖新增 E2E，无需增加另一条 workflow。

## 检查记录
- `cargo fmt --all --check`、`cargo check --all-targets`、`cargo clippy --all-targets -- -D warnings`、`cargo build --quiet` 和 `git diff --check` 通过。
- `cargo test --all-targets --quiet` 通过：472 passed、11 ignored；包含 mock Telegram File Provider 回归 E2E。
- LaunchAgent `stderr.log` 中的队列文件读取失败为 `Resource deadlock avoided (os error 11)`；Finder pin 后该低层未协调读取仍会失败。
