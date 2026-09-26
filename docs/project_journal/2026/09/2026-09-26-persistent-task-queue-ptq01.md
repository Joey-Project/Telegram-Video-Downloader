---
id: 20260926-ptq01
title: 持久任务队列与恢复
status: completed
created: 2026-09-26
updated: 2026-09-26
branch: wip/persistent-task-queue
pr:
supersedes: []
superseded_by:
---

# 持久任务队列与恢复

## 摘要
- 为视频和 PDF 任务增加持久队列。记录保留 Telegram update、来源消息、聊天、提交者、最近的 bot 状态或提示消息、规范化任务、进度和重试状态。
- 增加 `/queue` 页面，提供恢复、重试失败任务、取消和历史记录操作。Telegram update ID 用于避免重复处理；轮询偏移量仅在 update 处理完后推进。
- 重启时将未完成任务标记为中断。恢复任务会重新探测元数据或合集计划、重新检查已存在的 Bilibili 合集文件；媒体身份、所选格式、精确大小、分辨率或编码发生变化时要求用户确认。
- 任务完成前对已发布的主媒体文件计算 SHA-256。Bilibili 合集同步会同时校验已存在和新下载的媒体。
- 队列记录保存在各下载根目录下仅当前用户可访问的隐藏 JSON 目录中。完成记录会移动到已发布文件旁边。NFO 继续用于媒体库元数据，不承担会频繁变化的队列状态。
- 隐藏的 `.telegram-video-downloader-staging` 根目录及每次尝试目录仅当前用户可访问。失败、取消和未决尝试永久保留以便手动恢复；后续成功任务不会清理它们。

## 当前状态
- 视频尝试通过可靠的进度生命周期事件记录暂存路径；下载器在开始网络请求前写入恢复标记。
- SHA-256 校验会拒绝缺失、被替换、非普通文件、空文件或大小变化的输出。Unix 上检测到元数据时间戳变化时会再次计算内容哈希；只有内容哈希不同或文件身份/大小变化时才拒绝文件。
- 持久记录通过 `/queue` 支持恢复、失败重试、取消和历史分页；恢复前会重新验证任务计划和已发布媒体。
- 失败、取消和未决视频尝试永久保留在下载根目录下的隐藏私有 staging 目录中。

## 后续事项
- 无额外本地后续事项。真实 Telegram 和真实媒体下载未执行。

## 检查记录
- 已检查 `src/queue.rs`、`src/main.rs`、`src/downloader.rs`、`src/safe_fs.rs`、`src/telegram.rs` 和 `src/router.rs` 的相关实现。
- `cargo fmt --all --check`、`cargo build --quiet`、`cargo clippy --all-targets -- -D warnings`、`cargo test --all-targets --quiet` 和 `git diff --check` 均通过。
- 全量测试结果：453 passed、10 ignored；覆盖持久队列重启恢复和 mock Telegram 交互 E2E，以及合集进度生命周期和分页 mock Telegram E2E。
