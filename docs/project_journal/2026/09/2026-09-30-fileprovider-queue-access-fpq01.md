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
- 接受协调器提供的根目录内 accessor URL，并在 `RootedFs` 下重新绑定；拒绝下载根目录外的路径，同时保留私有权限和描述符身份校验。
- 队列写入统一使用 replacement 协调，且只在回调中探测目标文件，避免云端占位文件未物化时的未协调访问。
- Telegram 仅确认已向用户说明的 File Provider 访问错误；其他队列失败保持 update offset 不变以便重试，多链接 update 使用稳定任务 ID 避免重放重复建任务。
- 启动时遇到已分类的 File Provider 队列访问错误会每 5 秒重试，并可响应 shutdown；其他启动错误仍立即返回。
- 出错时保留既有队列和暂存数据，不重建或删除它们。

## 当前状态
- 文件对象在协调回调内部绑定；占位文件物化前后的 inode 变化不作为内容篡改判断，现有描述符校验仍保护实际读取的对象和内容。
- mock Telegram E2E 覆盖可操作的 File Provider 错误提示、队列写入瞬时失败后的 update 重放、以及重放期间多链接任务 ID 幂等。
- mock provider 测试覆盖启动时 File Provider 暂时失败后重开队列，以及普通配置错误不重试。
- Rust CI 的 `cargo test --all-targets --quiet` 已覆盖新增 E2E，无需增加另一条 workflow。

## 检查记录
- `cargo fmt --all --manifest-path Cargo.toml -- --check`、`cargo check --all-targets --quiet --locked --offline`、`cargo clippy --all-targets --quiet --locked --offline -- -D warnings`、`cargo build --quiet --locked --offline` 和 `git diff --check` 通过。
- `cargo test --all-targets --quiet --locked --offline` 通过：477 passed、11 ignored；包含 mock Telegram、瞬时队列写入和启动重试回归测试。因测试使用 macOS 文件协调和 localhost，完整套件在沙盒外运行。
- LaunchAgent `stderr.log` 中的队列文件读取失败为 `Resource deadlock avoided (os error 11)`；Finder pin 后该低层未协调读取仍会失败。
