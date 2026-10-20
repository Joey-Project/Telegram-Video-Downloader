---
id: 20261020-mls01
title: 媒体库设置、下载目标与迁移
status: completed
created: 2026-10-20
updated: 2026-10-20
branch: "codex/media-library-settings"
pr: "https://github.com/Joey-Project/Telegram-Video-Downloader/pull/47"
supersedes: []
superseded_by: ""
---

# 媒体库设置、下载目标与迁移

## 摘要

- 记录已确认的 `/settings`、逐次下载目标选择和媒体库迁移体验，范围为单用户、单部署机；保留现有 Cloudflare Worker + D1 与本机主动出站 Rust 架构。
- 已确认设计于 2026-10-20 完成实现及验证；设置、任务目标冻结、媒体/任务迁移和持久恢复均有默认执行的 mock Telegram/cloud E2E。隔离 Cloudflare + Rust 迁移验证也已通过；生产部署另行安排。

## 已确认范围

- 多个已登记媒体库可以并存，其中一个是默认库。历史媒体库仍可浏览和管理，也可以作为新任务目标；扫描只在已登记根目录内进行，并保持有界。
- 修改默认库或默认分类只影响之后创建的普通下载。每项任务在入队时固定媒体库、根路径和分类目标；显式迁移是唯一允许改动已选任务目标的流程。
- 首期不提供登录凭证管理。设置快照要区分已应用值、离线时最后一次同步快照和待验证请求；本机不可达时，待验证项不显示为已应用，只有 Rust 检查成功后才能使用。

## `/settings` 体验

- 设置首页显示当前默认媒体库和分类。媒体库列表逐项显示名称、路径、默认标记、可用/不可用状态、媒体数量和最近扫描时间。
- 详情页显示名称、路径、状态和默认分类，并提供设为默认、迁移和取消登记操作。取消登记只移除注册，不删除磁盘内容；仍有关联任务时阻止取消登记。
- 新增表单包含名称、部署机器上的路径、默认分类，以及默认关闭的“同时设为默认”选择。Rust 检查路径、访问权限和可用空间；路径不存在时提供明确的创建目录操作。
- 可登记已有媒体目录；登记后扫描，不自动重命名文件或改写 metadata。拒绝嵌套或重叠的根目录；实现须考虑别名解析后的路径身份。
- 多库状态或设置检查失败时保留最后同步快照，并将待检查和已应用状态分别展示。UI 不提供凭证输入或更换入口。

## 单次下载目标

- 用户照常向 bot 粘贴下载链接时沿用当前默认目标，不增加必填的目的地提示；入队确认消息显示实际保存位置。
- `/file` 文件夹工具栏中的“下载到这里”打开相同 Mini App 的 URL 输入表单，显示当前媒体库和分类且允许修改，支持视频或合集链接并提交入队。
- 整个合集使用选定目标及既有合集布局：合集或多 P 任务使用一个目录；合集内多 P 不再额外嵌套目录。
- 入队时保存所选目的地。若入队失败，表单保留已输入的链接和选择，便于修正或重试。
- 通知仍按既有预算发送：入队消息，以及每个视频一条持续编辑的消息。显式恢复的新 generation 可以开启新一轮通知。

## 媒体库迁移

### 选择和预览

- 可选择整个来源库，或选定媒体/合集；再指定目标库并查看预览。预览显示条目数、媒体与 sidecar 体积、目标剩余空间、实际源/目标路径、冲突，以及保留后的分类和合集结构。
- 迁移媒体时将视频与其 NFO、弹幕、字幕和封面作为关联组处理；不重新下载视频，也不转码。跨卷复制须先验证副本，并在清理每项源文件前复验其对象身份、内容及访问策略仍符合已确认状态，再清理对应源文件。
- 仅当所选范围关联未完成任务时，才显示“同时迁移未完成任务及暂存文件”选择；默认关闭，并显示任务数量及暂存字节数。另有独立且默认关闭的选项，可在迁移成功后将目标库设为默认。
- 冲突逐项选择跳过或保留两份，默认跳过。媒体和 sidecar 的决定保持一致；保留两份时新名称仍维持关联，跳过项留在源位置。
- 预览以及开始执行前均重新核对源对象、目标、空间和冲突，不依赖过期快照。

### 未完成任务与恢复

- 未勾选未完成任务选项时，任务、下载片段和恢复状态继续留在原库，状态保持不变，原库继续登记。
- 勾选后，先暂停受影响的运行中任务，再通过明确、可验证的检查点迁移片段、恢复状态和任务关联，完成后恢复原任务状态。原运行任务继续，原排队任务回到队列并按正常调度执行，原失败及手动暂停任务保持原状态；迁移操作不将这些状态统一改为运行。
- 私有暂存片段及恢复信息遵循永久保留约定；显式迁移将片段保留在目标库。先停止写入并固定来源文件计划，复制和验证后重建目标控制绑定，再安装目标队列记录；迁移引擎确认交接后，逐项复验并移除来源 payload。来源控制记录和目录可继续保留为恢复依据；不得盲拷贝旧锁状态来建立目标绑定。
- 错误或中断必须保留数据与可恢复进度。多文件迁移允许部分完成并可恢复，不承诺全批原子回滚；空库也不会自动取消登记。

### 执行控制与结果

- 执行中提供暂停（之后可恢复）、停止剩余迁移（已完成项保留，其余留在源位置）和仅重试失败项。
- Mini App 显示进度以及成功、跳过、失败和实际位置；Telegram 只创建一条迁移进度消息，并持续编辑到最终结果。

## 实施顺序

1. 建立媒体库注册表；兼容既有单根配置；持久化每项任务的固定目标，并让队列可从全部已登记根目录分派。
2. 实现设置快照和操作，再接入 Mini App 的媒体库列表、详情及新增流程。
3. 增加“下载到这里”和入队目的地显示。
4. 实现媒体迁移；随后接入可选的未完成任务、片段和恢复状态迁移及持久恢复界面。

实施前，`AppConfig`、`QueueManager` 和 `LibraryManager` 在启动时绑定单一根目录，普通 `TaskRecord` 没有统一持久化的冻结目标。本轮同时接入多根注册、目标持久化、旧记录兼容和锁/恢复数据交接；PDF 继续采用独立配置。

## 验收范围

- 使用 mock Telegram/cloud 验证默认目标与显式目标、入队后修改默认值、重启恢复及任务目标冻结之间的交错行为。
- 覆盖离线快照、待检查与已应用状态、重复提交、无效路径、目录创建、访问/空间检查、重叠根拒绝及失败后表单输入保留。
- 覆盖迁移媒体与 sidecar、合集/分类结构、冲突跳过/保留两份、跨卷复制验证，以及源清理前后中断。
- 覆盖未完成任务选择框不存在/关闭/开启三种情况，并检查运行、失败、手动暂停、排队任务及片段/恢复状态的预期保留或恢复。
- 在复制、验证、源清理各检查点测试暂停、停止、失败重试和进程重启；验证私有暂存状态保留及单条 Telegram 消息编辑次数和结果。
- Cloudflare 真实环境验证和生产部署属于之后单独授权的阶段；本地 mock 不代表生产连通或部署完成。

## 实施检查点

- 在已有隔离 worktree 上从 `origin/master` 建立本功能分支；保留原设计文档，未切换 canonical checkout 或生产 bot。
- GPT-6.1 协调集成，GPT-6 Luna Max 分别实现媒体库/队列核心、迁移引擎和 Cloud/Mini App；主入口及 mock E2E 由协调者集成。
- 注册表保留旧单根配置兼容；普通下载目标在入队时持久化，显式任务迁移需暂停写入、验证目标文件、交接队列记录及重建目标控制绑定。
- 私有 staging owner、retention、worker lifecycle 和 publication metadata 含 inode 绑定，迁移必须验证并明确重建/交接，不能视作普通 JSON 原样拷贝。
- 已记录现有生产二进制 designated requirement；所有本地 Cargo 操作显式使用本 worktree 的 `target`。记录和后续有界验证日志位于本地忽略目录 `.codex-tmp/mls01/`。
- Rust/Worker 合同、迁移后台控制和重启恢复已接入；完整静态检查、单测及 mock Telegram/cloud E2E 已通过，详见下方本地验证。真实 Cloudflare 验证与生产部署仍分开安排。
- Cloud 本地类型检查通过、98 项测试通过、Wrangler deployment dry-run 通过（37 个静态资源）；包含逐项冲突选项、离线快照保留，以及保留 AUTOINCREMENT 高水位的 D1 `0003` 请求类型约束迁移。未使用这些结果宣称真实云端可达或已部署。
- 本机服务 Python 测试 101 项通过，LaunchAgent integration 测试与 shell syntax 检查通过；项目日志 validator 通过。
- 整合回归发现并修正可信 macOS 系统别名与旧配置访问路径的差异、目标路径重写、视频/PDF 同根锁共享，以及快照 metadata 压缩遗漏标记。旧 File Provider 故障重试测试也纳入全量回归；中间检查不作为最终验收结果。
- 已加入设置/跨库文件操作、默认变更后冻结目标、未完成任务选项、队列迁移后继续、逐项冲突与路径重写、暂停/停止/重启/epoch，以及真实保留片段在复制中及队列交接后重启的 mock E2E；另覆盖源/目标对象、内容及权限变化、孤立 stage、发布/删除检查点重放和正常目录子项变化。
- 迁移先停止所选任务写入，持有来源与目标的输出锁；扫描通过显式验证的现有锁进行，避免重复申请同一 flock。活动迁移期间文件扫描/修改快速返回提示，避免将暂停请求堵在等待锁的扫描之后。
- 默认库更新使用迁移 ID 的持久化幂等记录。源媒体清理前要求目标副本的持久化 inode、内容与访问策略验证；未落盘确认的孤立 stage 保留，并改用新名称继续。
- 在任务停止写入并取得来源/目标输出锁后，再次扫描核对已确认范围；任务或整库范围新增了未批准媒体时暂停并要求新预览，不先移动媒体或删除片段。
- 迁移取得输出锁前检查 staging：拒绝常规启动恢复会删除的未保留片段、待恢复 publication，以及旧路径配置/命名 worker request；拒绝时源片段和控制文件逐字节保留。正常下载的既有启动恢复策略保持原样。
- File Provider 临时读取失败保留可重试的队列库，不将读取失败转换为任务不存在；实际根目录绑定失败才标记不可用。冻结目标解析在后台重试，并核对任务 generation。

## 本地验证

| 范围 | 最终结果 |
| --- | --- |
| Rust 1.99.1 格式及差异空白检查 | `cargo fmt --all -- --check`、`git diff --check` 通过 |
| Rust 全量测试 | `cargo test --locked --offline --all-targets`：1,124 通过、0 失败、16 个既有忽略项 |
| Rust 静态检查及构建 | `cargo clippy --all-targets --locked --offline -- -D warnings`、`cargo build --locked --offline` 通过 |
| Cloud / Node 25.8.2 | 类型检查通过；120 项测试通过；Wrangler 4.149.0 dry-run 通过，41 个静态资源 |
| 本机服务 / Python 3.14.5 | 101 项测试、LaunchAgent mock integration 和 shell syntax 检查通过 |

- Cargo gate 显式使用本 worktree 的 `target` 和既有共享 build-dir；未覆盖已安装程序。审查回归日志保留于 `.codex-tmp/mls01-review2/` 和 `.codex-tmp/mls01-review3/`，后者包含最新队列、重启恢复与生产迁移渲染回归。服务辅助验证日志保留于首次实施的 `.codex-tmp/mls01/`。
- 最新 Rust 验证日志为 `mls01-review3/full-tests-final.log`、`clippy-final.log` 和
  `build-final.log`；Cloud 的测试、类型检查和 dry-run 日志也在 `mls01-review3/` 下。
- 新增媒体库/迁移 E2E 没有忽略项。代表性覆盖包括 `media_library_migration_bundles_and_unfinished_choice_mock_telegram_e2e`（视频/NFO/ASS、未勾选时任务及片段保留）、`media_library_migration_uses_per_bundle_conflict_choices_mock_e2e`（实际 Skip/KeepBoth）、`partial_task_migration_keep_both_rewrites_published_paths_mock_e2e`（已发布路径重写）和两项片段/队列交接后的恢复用例。
- 大片段恢复用例使用 8–16 MiB 的合成文件；独立运行整体耗时约 15 秒。迁移完成等待设为明确的 60 秒上限，以容纳全量测试共享输出槽及调试模式校验开销，并保留超时状态诊断。全量测试随后通过；未改动生产迁移等待逻辑。
- 测试使用临时本地目录、合成媒体/片段、fake 下载工具及 mock Telegram/cloud。恢复测试通过重建 runtime/queue 和持久状态重放模拟中断；未向真实 Telegram/Cloudflare 发送请求，未操作生产媒体或终止部署进程。

## PR regression coverage

- Restart reconciles task ownership for a migration already persisted as Stopped. A shared
  video/PDF root retains its PDF owner lock after its video registration is removed; a separate
  process verifies both contention and eventual release.
- Migration default updates latch a settings revision once and durably record applied or skipped
  CAS outcomes. Newer user settings survive execution and registry replay.
- Conflict refresh durably supersedes the exact old preview, supports interrupted refresh replay,
  and rejects the old confirmation after reopening the library managers. Failed task
  handoffs expose retry and stop controls even when media failures are zero.
- Cloud migration history is capped at 32 entries without deleting local recovery records.
  Explicit blank download categories remain distinct from omitted categories in Worker validation
  and Rust dispatch.
- The new CAS E2E exposed a real missing-directory creation error. The rooted filesystem now
  receives an absolute child path; nested missing-root creation has a positive regression. The
  original E2E still requests directory creation rather than pre-creating its fixture.
- The original 16 feature mock E2Es run by default through the existing Rust CI command. The three added
  recovery cases are `stopped_migration_reconciles_pending_task_transfer_after_restart`,
  `migration_default_update_preserves_a_newer_user_library_choice`, and
  `refreshed_migration_preview_stales_old_inbox_receipt_and_confirmation`.
- Cloud task summaries include task kind and the frozen or legacy-resolved library ID. Each
  library publishes an authorized incomplete-video count independently of the global task-list
  limit; failed tasks remain retryable dependencies. Unknown counts and staging sizes remain
  distinct from zero, and PDF tasks do not enable video-task migration.
- Removing a library checks retryable failed tasks as well as other active video dependencies.
  Task configuration revalidates the registered root's persisted device/inode before opening
  replacement-path queue state; rejected replacements preserve the original records and staging.
- A failed library scan retains its cached snapshot and marks only that library unavailable.
  Data-changing controls remain read-only until a successful refresh, while scanning and library
  switching remain available. Preview validation errors do not mark a library offline.
- `cloud_snapshot_projects_video_library_ids_and_authorized_incomplete_counts` exercises the
  production snapshot builder with legacy, failed, PDF, malformed-staging, and unauthorized-chat
  records; `failed_non_default_library_open_keeps_cache_unavailable_until_scoped_recovery`
  exercises a propagated File Provider root-open denial, retained cache, read-only state, and
  scoped recovery. Both run by default in Rust CI. The existing 101-task test also verifies a
  count of 101 while task rows remain capped at 100.
- The HTTP state-reporter regression compares the complete cached item JSON with only presence
  changed to unavailable, keeping revision and scan time intact; recovery must restore the
  original item hash. Its expected digest uses the same JSON representation as the receiver.
- Migration rendering preserves the backend's active/recoverable priority before applying its
  display limit. Production markup is exercised for active, paused, completed, stopped, failed,
  and partial states so a missing status binding cannot abort the page.
- Terminal migration status does not permit removing participating libraries while queue handoffs
  remain pending. The source manifest stays discoverable through restart and acknowledgement.
- `terminal_migration_keeps_library_registered_until_queue_handoff_ack_after_restart` persists
  target installation and engine completion with a pending queue token, reopens the runtime,
  checks the handoff-specific removal refusal, then completes recovery and removes the source
  registration. The moved task remains failed at its target without an automatic retry.
- Default-library changes require an available, identity-matched registered root; rejecting a
  replacement preserves the selected default, settings revision, and original queue data.

## Isolated Cloudflare and Rust E2E at `08f56d4`

- A disposable Worker and D1 database ran the Worker code at `08f56d4` and migrations `0001`–`0003`.
  The worktree Rust debug binary used only a private temporary configuration and synthetic media.
  Its SHA-256 was `ec1f569fa8bd51ac39aa968ac4d0ee696a24ec35e1e6a80ff9fa475a65c1d7e5`.
- The live probe passed all 21 checks: authenticated owner API and static assets, creation and
  registration of a missing library directory, default/category changes, scanning, migration
  preview and confirmation, fixed source/target selection after a default change, and verified
  MP4/NFO migration with source payload cleanup. Both file hashes were preserved.
- The two-file synthetic bundle was 1,095 bytes. This exercised actual Worker/D1 requests and
  Rust filesystem operations; it does not claim physical cross-volume, File Provider, Telegram
  client/WebView, or real downloader validation. Local feature E2Es mock Telegram; the live probe
  denied Telegram egress and verified that policy with a rejected CONNECT request.
- The first runtime start rejected a probe-only two-second polling setting before submitting any
  action. The probe was corrected to the supported five-second minimum; the complete rerun passed
  in 14.07 seconds and its runtime exited through SIGINT with all child processes reaped.
- Final local logs and the sanitized live report are retained under `.codex-tmp/mls01-pr/`:
  `rust-full-final.log`, `rust-clippy-final.log`, `rust-build-final.log`, and `e2e-report.json`.
  Production Worker, webhook, LaunchAgent, configuration, credentials, and media were not changed.
- The disposable Worker and D1 database were deleted after the successful probe. Generated test
  credentials, runtime configuration, and synthetic media were removed; redacted logs and the
  sanitized report remain as local evidence.

## 状态

- 本批实现及本地验证完成；最终项目日志 validator 与差异检查通过。PR 审查、真实 Cloudflare 验证和部署不由本地 mock 结果代替。
- 上线前先应用 D1 `0003` 请求类型迁移；原 Cloud inbox 仍绑定原配置根目录，需保留其私有元数据目录可访问。操作说明见 [Media libraries](../../../MEDIA_LIBRARIES.md)。
- 原文件管理主线与其 PR/审查/运行历史继续记录在 [文件管理 Mini App 设计与实施计划](2026-10-07-file-management-mini-app-fmm01.md)。
