---
id: 20261020-mls01
title: 媒体库设置、下载目标与迁移
status: completed
created: 2026-10-20
updated: 2026-10-21
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
- 私有暂存片段及恢复信息遵循永久保留约定；显式迁移将片段保留在目标库。先停止写入并固定来源文件计划，复制和验证后重建目标控制绑定，再安装目标队列记录；迁移引擎确认交接后，逐项复验并移除来源 payload。队列确认交接后，再通过对象身份绑定的持久检查点清理来源控制记录和空目录；清理未结束时继续登记来源库。不得盲拷贝旧锁状态来建立目标绑定。
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
| Rust 全量测试 | `cargo test --locked --offline --all-targets`：1,185 通过、0 失败、16 个既有忽略项 |
| Rust 静态检查及构建 | `cargo clippy --all-targets --locked --offline -- -D warnings`、`cargo build --locked --offline --bin telegram-video-downloader` 通过 |
| Cloud / Node 25.8.2 | 类型检查通过；196 项测试通过；Wrangler 4.149.0 dry-run 通过，51 个静态资源 |
| 本机服务 / Python 3.14.5 | 101 项测试、LaunchAgent mock integration 和 shell syntax 检查通过 |

- Cargo gate 显式使用本 worktree 的 `target` 和既有共享 build-dir；未覆盖已安装程序。审查回归日志保留于 `.codex-tmp/mls01-review2/` 至 `.codex-tmp/mls01-review11/`；后者记录 26 项定向回归，新增目录对象绑定和取消登记后同路径复用的扫描缓存回归。服务辅助验证日志保留于首次实施的 `.codex-tmp/mls01/`。
- 最新 Rust 验证日志位于 `.codex-tmp/mls01-review12/`：`rust-race-manager-review12-final.log`、`rust-race-availability-review12-final.log`、`rust-fmt-review12-final3.log`、`rust-clippy-review12-final3.log`、`rust-build-review12-final.log` 和 `rust-tests-review12-final.log`。新增确定性的目录验证后替换回归；完整 1,185 项测试通过，用时 138.78 秒。生产入口改为消费已验证句柄后，将旧的路径构造入口及其 import 限定为测试编译；两次中间 Clippy 失败分别保留为 `rust-clippy-review12-final.log` 和 `rust-clippy-review12-final2.log`，未抑制警告。
- 上游合并后的 Rust 验证日志保留于 `.codex-tmp/mls01-ci-update/`。多 P 选择改进要求既有目标冻结 E2E 补齐 `page_count` 字段；该定向回归和当时 1,184 项全量测试通过。
- 前一轮审查的 Rust 验证日志为 `mls01-review11/tests-review11-final.log`、`clippy-review11-final.log`、`build-review11-final.log` 和 `focused-review11-final2.log`。该轮首次定向编译缺少测试用的 `serde_json::Value` import，修正后重跑通过，首次失败日志独立保留为 `focused-review11-final.log`。更早的共享 build-dir 沙盒访问失败也单独保留；之后的 Cargo 命令从首次执行起使用窄提权。
- 最新 Cloud 本地全量测试、类型检查和 dry-run 日志位于 `.codex-tmp/mls01-review12/`，分别为 `cloud-test-final.log`、`cloud-typecheck-final.log` 和 `cloud-build-final.log`。新增 5 项下载历史回执回归和 3 项时间戳单位回归；完整 196 项测试通过。该轮仅使用本地 mock，打包命令明确以 `--dry-run` 结束。
- 前一轮 Cloud 日志保留于 `.codex-tmp/mls01-review11/`，覆盖跨标签页永久拒绝、队列移除失败、401/403 与暂态重试，以及目录对象替换和旧快照隔离。首次全量测试发现草稿同步遗漏目录身份字段，修正后通过；测试替身及声明的类型错误也已修正，失败日志分别保留为 `full-cloud-tests.log` 和 `cloud-typecheck.log`。
- 新增媒体库/迁移 E2E 没有忽略项。代表性覆盖包括 `media_library_migration_bundles_and_unfinished_choice_mock_telegram_e2e`（视频/NFO/ASS、未勾选时任务及片段保留）、`media_library_migration_uses_per_bundle_conflict_choices_mock_e2e`（实际 Skip/KeepBoth）、`partial_task_migration_keep_both_rewrites_published_paths_mock_e2e`（已发布路径重写）和两项片段/队列交接后的恢复用例。
- 大片段恢复用例使用 8–16 MiB 的合成文件；独立运行整体耗时约 15 秒。迁移完成等待设为明确的 60 秒上限，以容纳全量测试共享输出槽及调试模式校验开销，并保留超时状态诊断。全量测试随后通过；未改动生产迁移等待逻辑。
- 测试使用临时本地目录、合成媒体/片段、fake 下载工具及 mock Telegram/cloud。恢复测试通过重建 runtime/queue 和持久状态重放模拟中断；未向真实 Telegram/Cloudflare 发送请求，未操作生产媒体或终止部署进程。

## PR regression coverage

- Registered cloud file and migration managers consume the queue root's verified open directory
  descriptor before recovery metadata initialization, closing the validation/path-reopen interval.
  Directory object identity remains separate from ordinary child-entry changes.
- An accepted File download outside the complete recent operation window exposes an explicit
  browser-waiting-notice release. Its outcome remains unknown, and clearing the notice neither
  changes the accepted request nor sends another action; pending, stale and incomplete views
  cannot establish eviction.
- Settings relative-time rendering uses Worker milliseconds for desktop contact and inbox arrival,
  and Rust seconds for local operation updates. Absolute scan timestamps retain their seconds contract.
- The frozen-destination callback E2E supplies the total page count required by
  the updated multi-page selection prompt, retaining its P2 selection and
  destination-library/category assertions after upstream merges.
- Browser request outcomes distinguish accepted sequences from permanent rejections. A helping
  tab persists a rejection before dequeue, allowing the origin to leave pending state without
  restoring the rejected payload; retryable errors and owner-authentication gates remain distinct.
- Settings draft/focus keys include an opaque persisted-directory binding. A same-path replacement
  receives a different binding, while name/category changes, child churn and reopening preserve
  the original binding. Successful unregister evicts scoped scan/error caches under the scan
  publication gate and clears the unscoped legacy fallback; failed unregister preserves caches.
- Concurrent tabs on the same Mini App surface retain a request receipt before dequeue. The
  originating download or conflict-refresh tracker can recover its sequence after a lost response
  and a second tab's deduplicated submission. Browser-local receipt metadata stays off the wire.
- Settings polling preserves dirty name/category fields independently while clean fields follow
  the applied snapshot. Removed or rebound registrations discard their drafts; an older operation
  result cannot overwrite a newer edit. Migration target selection survives ordinary re-renders.
- Migration manager opening and discovery verify registered root availability and saved directory
  identity for both endpoints before metadata access. Replacement-path regressions preserve the
  original media and refuse replacement metadata writes; restoration and child-entry churn work.
- Invalid unfinished-task conflict choices are rejected before replacement-preview persistence or
  prior-preview supersession. The old preview remains confirmable, no orphan candidate is written,
  and a valid refresh retains durable restart replay. Media-only failed/partial jobs expose Stop.
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
- Migration scans now return an internal completeness proof without changing the public snapshot.
  Whole-library previews require a complete bounded scan; selected bundles are rejected only when
  their media or attachment paths intersect a known incomplete directory or unreadable path, or
  when the scan cannot localize its scope. Provider-unavailable reads, entry/media limits, and the time budget are
  incomplete; missing optional metadata and low-confidence metadata are not. Scanner regressions:
  `migration_scan_completeness_localizes_incomplete_directory`,
  `migration_scan_completeness_localizes_entry_and_media_limits`,
  `migration_scan_completeness_localizes_timeout_and_reports_unknown_scope`,
  `migration_scan_completeness_blocks_provider_unavailable_paths`, and
  `migration_scan_completeness_does_not_require_optional_metadata`.
- Migration completeness keeps separate path markers so root-level unreadable files and unbuilt
  timeout tails do not alter public Presence, missing-history, hardlink or rename reconciliation.
  Existing replacement and hardlink-tail regressions retain their original snapshot assertions and
  also verify the exact migration blockers.
- Migration task membership and post-confirm scope expansion also use the same completeness proof.
  Whole-library task moves require complete scans; selected moves reject affected published paths
  within incomplete directories while allowing component-distinct unaffected subtrees.
- Enqueue revalidates the selected library root identity before queue state is written. A newly
  connected independent queue store remains unavailable until startup recovery finishes and its
  root identity is verified.
- Conflict refresh writes the replacement preview durably before superseding the prior revision;
  restart replay preserves that order and keeps the old confirmation stale.
- The current master's Bilibili page-selection flow keeps the task-specific download context.
  `bilibili_page_choice_keeps_frozen_destination_after_default_switch` verifies that selection and
  queue reopening preserve the original library root and category after changing the default.
- New video-task creation refuses uncertain task-ID absence when any registered queue is offline;
  positive existing-task replay remains idempotent. A durable-record/index-failure regression covers
  offline source, changed default, no second record and restoration of the original destination.
- File-preview projection preserves the exact bounded source library ID in both full and compact
  envelopes after the operation-result budget removes full details. Explicit malformed IDs are
  omitted and counted; genuinely absent legacy IDs retain their existing compatibility behavior.
- Shared browser actions carry local surface ownership, so `/file` and `/settings` retain their own
  receipts before dequeuing submissions. The wire action schema is unchanged. Media summaries use
  the full reported count while disclosing retained-only list filters and pagination; migration
  previews require both endpoint IDs to resolve to registered descriptors before Start is enabled.
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
- Category changes apply the same registered-root identity gate. Ordinary child-entry churn is
  accepted while a replacement root is rejected without changing the category or revision.
- Before a scoped cloud management closure opens a library manager, dispatch refreshes the selected
  registration and verifies its saved root identity and availability. Missing, unavailable or
  replaced roots fail before scan or library-metadata writes and mark only that library unavailable;
  child-entry churn remains supported, and a scoped refresh recovers when the original bound root
  returns. `media_library_dispatch_rejects_replaced_roots_and_recovers_scoped_roots` covers these
  cases, including scan/file-preview refusal without replacement metadata and successful recovery.
- The HTTP recovery regression compares complete state content after normalizing only freshly
  sampled per-library `free_bytes`. Cached media, revision, scan time, warnings and all other fields
  remain exact; availability must recover, and refreshed capacity must be numeric. A fresh sample
  may be equal to the earlier capacity or differ as parallel tests allocate space on the same volume.
- The migration's expected default-settings revision is persisted with confirmation, before
  task quiescence or output-lease waits. Restart and confirmation replay cannot adopt a later
  explicit user choice as the migration's new baseline.
- Oversized cloud snapshots compact completed handoffs and conflict choices as well as media
  details. Omission and error totals survive repeated compaction, recovery controls remain
  available, and the final hydrated JSON must fit the upload budget.
- `cloud_snapshot_compacts_large_migration_task_details_with_stable_aggregates` covers
  task-only history, error samples, conflict overrides and repeated upload reduction;
  `cloud_snapshot_reduction_converges_across_maximum_migration_history` covers 32 retained
  migrations with 2,048 media rows and 1,024 task handoffs each. Whole-history halving has a
  pass bound derived from the largest retained array, with one full serialization per pass.
- Publication resumes after an interrupted pre-rename mode change only for the checkpointed
  staging object and verified access proof. Replacements, unexpected modes and additional hard
  links remain errors.
- The SafeFs regressions are `migration_publish_recovers_exact_checkpointed_inode_at_final_mode`
  and `migration_publish_rejects_a_replaced_checkpointed_stage`.
- A durable final queue-acknowledgement checkpoint authorizes source staging control and empty
  directory cleanup. Cleanup can resume after restart, preserves unknown objects, and retains
  both registrations until its manifest no longer needs recovery. A queue acknowledgement
  receipt bridges a crash before that engine checkpoint without treating task absence as proof.
- `final_ack_cleanup_replays_owner_and_directory_unlinks_after_restart` covers cleanup intents,
  source owner/directory removal and replay. `replaced_task_control_and_unplanned_child_are_preserved`
  checks replacement/unknown-entry preservation, and
  `legacy_completed_handoff_without_final_ack_keeps_source_controls` checks missing ACK proof.
  Stop intent survives attention and cleanup-only retry; queued task writers remain blocked
  until the exact acknowledgement receipt is consumed after cleanup.
- Terminal recovery has an explicit authenticated finalization control after a database epoch
  change. Queue token/receipt proof supplements the engine's saved checkpoints, including an
  installed target whose engine acknowledgement was interrupted. Ordinary stale controls do not
  renew terminal authority; stopped work retains its stop intent.
- Terminal finalization runs as a tracked background operation. A held output lease must leave
  cloud command dispatch responsive. Pending default resolution also retains both participating
  library registrations until its durable outcome is known.
  The default-run mock E2Es are
  `completed_default_finalization_requires_new_epoch_authorization_and_blocks_removal` and
  `stopped_target_installed_handoff_reauthorizes_once_and_keeps_dispatch_responsive`.
  After the exact final queue ACK, stopped handoffs advance verified source-payload cleanup
  before controls and directories, without advancing stopped media groups. Replacement and
  unrelated-sibling regressions protect retained data on this terminal-only path.
- Default-update history is rotated out of the 64 KiB registry into a separate bounded exact
  archive. The registry binds its expected count and SHA-256; an archive-only interrupted write
  is recoverable only from exact duplicate outcomes still present inline. Unknown, missing or
  conflicting archive state does not authorize another default update.
- Registration commits the prepared directory identity with its entry and optional default
  update. A replacement detected before commit preserves registry/default/revision, while
  ordinary child-entry changes remain accepted.
- Preview maintenance examines one rotating candidate per source at a time. Expired documents
  are retired, superseded plans are compacted, and confirmed previews are retired after a
  durable job exists. Confirm replay and controls use that validated job; replaced or unreadable
  documents remain preserved with bounded diagnostics.
- The 32-entry cloud migration history prioritizes completed and stopped jobs with pending
  finalization alongside other recoverable work, so newer inert history cannot hide recovery.
- Migration-preview confirmability is reconciled against local expiry and supersession before
  rendering an awaiting-confirmation receipt. Definite expiry or absence becomes stale;
  unreadable or unverified storage remains an attention state without a false stale result.
- Oversized migration previews retain an actionable bounded summary with the exact confirmation
  identity, complete aggregate counts, sampled paths and explicit omission totals. Compact
  summaries confirm the complete selection with its existing choices and disable per-item
  conflict edits; ordinary full previews retain their existing editing flow.

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
