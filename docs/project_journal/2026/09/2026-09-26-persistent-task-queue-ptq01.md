---
id: 20260926-ptq01
title: 持久任务队列与恢复
status: completed
created: 2026-09-26
updated: 2026-09-26
branch: wip/persistent-task-queue
pr: https://github.com/Joey-Project/Telegram-Video-Downloader/pull/20
supersedes: []
superseded_by:
---

# 持久任务队列与恢复

## 摘要
- 为视频和 PDF 任务增加持久队列。记录保留 Telegram update、来源消息、聊天、提交者、最近的 bot 状态或提示消息、规范化任务、进度和重试状态。
- 增加 `/queue` 页面，提供恢复、重试失败任务、取消和历史记录操作。Telegram update ID 用于避免重复处理；轮询偏移量仅在 update 处理完后推进。
- 重启时将未完成任务标记为中断。恢复任务会重新探测元数据或合集计划、重新检查已存在的 Bilibili 合集文件；媒体身份、所选格式、精确大小、分辨率或编码发生变化时要求用户确认。
- 任务完成前对已发布的主媒体文件计算 SHA-256。Bilibili 合集同步会同时校验已存在和新下载的媒体。
- 队列记录保存在各下载根目录下仅当前用户可访问的隐藏 JSON 目录中。已完成任务记录会移动到已发布文件旁边；取消或没有媒体 sidecar 的终态记录会归档到私有历史子目录。索引最多保留最近 10,000 条历史引用，并受索引字节上限约束；淘汰索引引用不会删除磁盘上的记录文件。NFO 继续用于媒体库元数据，不承担会频繁变化的队列状态。
- 隐藏的 `.telegram-video-downloader-staging` 根目录及每次尝试目录仅当前用户可访问。失败、取消和未决尝试永久保留以便手动恢复；后续成功任务不会清理它们。

## 当前状态
- 视频尝试通过可靠的进度生命周期事件记录暂存路径；下载器在开始网络请求前写入恢复标记。
- SHA-256 校验会拒绝缺失、被替换、非普通文件、空文件或大小变化的输出。Unix 上检测到元数据时间戳变化时会再次计算内容哈希；只有内容哈希不同或文件身份/大小变化时才拒绝文件。
- 持久记录通过 `/queue` 支持恢复、失败重试、取消和历史分页；恢复前会重新验证任务计划和已发布媒体。
- 完成状态在 sidecar 迁移前持久化；视频与 PDF 根目录指向同一目录对象时共用队列，并把规范化媒体路径映射回配置路径。
- 队列索引 v2 使用相对下载根目录的记录路径，迁移旧版绝对路径，并支持通过同一根目录的符号链接别名重启。
- 运行中取消先记录为请求；若下载结果已就绪，则优先验证已发布文件，再决定完成状态。
- `/queue` 限制每个 URL 预览，并把完整消息限制在 3,500 个 UTF-16 单元以内。
- Telegram 回复发送失败只记录日志，不再阻止后续 update 处理和轮询 offset 前进；每个队列管理器在整个生命周期内独占其下载根目录的队列 owner 锁，启动恢复仅由成功取得锁的进程执行；任务索引操作仍通过操作锁序列化。
- 选择和重复任务回调按记录当前状态做条件转换；任务恢复后到达的旧回调会过期，不会把运行中任务重新排队或启动第二个 worker。`Preparing` 状态只显示取消操作，不展示无法成功的恢复按钮。重启时会把已持久化的取消请求恢复为已取消，而不是中断。
- 大合集的主媒体哈希会在任务记录中压缩为确定性 SHA-256 清单，避免单条记录超过 2 MiB 上限；Bilibili 计划身份同时纳入 CID 和 EPID。
- 失败、取消和未决视频尝试永久保留在下载根目录下的隐藏私有 staging 目录中。

## 后续事项
- 无额外本地后续事项。真实 Telegram 和真实媒体下载未执行。

## 检查记录
- 已检查 `src/queue.rs`、`src/main.rs`、`src/downloader.rs`、`src/safe_fs.rs`、`src/telegram.rs` 和 `src/router.rs` 的相关实现。
- `cargo fmt --all --check`、`cargo build --quiet`、`cargo clippy --all-targets -- -D warnings`、`cargo test --all-targets --quiet` 和 `git diff --check` 均通过。
- 全量测试结果：471 passed、11 ignored；覆盖持久队列重启恢复、旧索引与根目录别名迁移、跨进程队列 owner 独占、中断的终态归档恢复、恢复后旧选择回调失效、`Preparing` 状态操作按钮、重启时完成已请求取消、历史索引限额和文件保留、超大哈希清单、Bilibili CID/EPID 计划身份、完成与取消竞态、队列消息长度限制，以及 mock Telegram 交互 E2E、失败回复后继续处理 update、合集进度生命周期和分页 mock Telegram E2E。
- GitHub Actions 的 macOS Rust CI workflow 在 PR 和 `master` 更新时运行格式检查、严格 Clippy 与 `cargo test --all-targets --quiet`；新增测试由该全量测试命令自动覆盖，包含 mock Telegram E2E。
