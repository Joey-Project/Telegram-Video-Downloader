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
- `claims.lock` 和 `owner.lock` 的检查、创建、打开与路径身份复核都在协调访问中完成；文件锁等待在回调外进行，避免占用协调期间阻塞。
- Telegram 仅确认已向用户说明的 File Provider 访问错误；其他队列失败保持 update offset 不变以便重试，多链接 update 使用稳定任务 ID 避免重放重复建任务。
- 启动队列和读取重启摘要处遇到已分类的 File Provider 错误时，每 5 秒重试；同步协调调用在独立线程执行，以便 shutdown 及时返回；其他启动错误仍立即返回。
- 每次扫描活动队列目录都通过协调读取完成，并在读取前后验证已绑定目录身份与 owner-private 权限，覆盖 `/queue`、恢复和任务查找路径。
- Telegram 拉取或处理更新失败后的 5 秒退避会同时监听 shutdown，避免 SIGTERM 或 Ctrl-C 被退避延迟。
- 出错时保留既有队列和暂存数据，不重建或删除它们。

## 当前状态
- 文件对象在协调回调内部绑定；占位文件物化前后的 inode 变化不作为内容篡改判断，现有描述符校验仍保护实际读取的对象和内容。
- mock Telegram E2E 覆盖可操作的 File Provider 错误提示、队列写入瞬时失败后的 update 重放、以及重放期间多链接任务 ID 幂等。
- mock provider 测试覆盖启动和重启摘要中的 File Provider 暂时失败后重试、普通配置错误不重试、队列锁文件及 `/queue` 目录的协调访问，以及启动和 Telegram 退避期间的 shutdown 中断。
- Rust CI 的 `cargo test --all-targets --quiet` 已覆盖新增 E2E，无需增加另一条 workflow。

## 检查记录
- `cargo fmt --all --manifest-path Cargo.toml -- --check`、`cargo check --all-targets --quiet --locked --offline`、`cargo clippy --all-targets --quiet --locked --offline -- -D warnings`、`cargo build --quiet --locked --offline` 和 `git diff --check` 通过。
- `cargo test --all-targets --quiet --locked --offline` 通过：481 passed、11 ignored；包含 mock Telegram、队列锁与目录协调、启动摘要重试、瞬时队列写入和 shutdown 中断回归测试。因测试使用 macOS 文件协调和 localhost，完整套件在沙盒外运行。
