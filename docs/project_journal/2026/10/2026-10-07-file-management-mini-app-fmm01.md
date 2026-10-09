---
id: 20261007-fmm01
title: 文件管理 Mini App 设计与实施计划
status: active
created: 2026-10-07
updated: 2026-10-09
branch: "wip/file-management-cloud"
pr: ""
supersedes: []
superseded_by: ""
---

# 文件管理 Mini App 设计与实施计划

## 摘要

为 Telegram 下载 bot 实现 `/file` 文件管理 Mini App，以整理已有媒体为最高优先级，同时覆盖查找媒体。`/settings` 与新下载位置选择属于第五阶段，详细设计待前四阶段完成。本文保留已确认的用户体验、安全边界和分阶段计划；具体检查结果单独记录。

## 当前状态

- `/file` 与 `/settings` 分别由两个 Telegram 命令打开；它们是独立 Mini App，后台能力可以共用。设置页管理下载根目录、默认分类和未来的登录状态。
- 三个明确场景是整理现有媒体（优先）、查找媒体、创建新下载时选择保存位置。分类对应根目录内的真实文件夹。
- 当前接受的架构大方向是公开托管的静态界面自动加载文件库并提交操作；Telegram webhook 进入 Cloudflare Worker，再进入 D1 持久收件箱。本机 Rust bot 主动领取操作，并由本机通过同一 Telegram bot 发送或编辑进度消息。本机没有公网入站接口。偏好使用 Cloudflare Free；达到额度限制时暂停操作并提示用户。
- Rust 通过固定 Worker API 补领、确认和回写状态；Mini App 验证 Telegram initData。休眠 WebSocket 推送完整请求，固定共享密钥鉴权，本机落盘后才确认接收。状态通过持久递增版本防止旧回写覆盖新快照。
- 第一至第四阶段的本地代码已落在隔离分支：Worker/D1 收件、Rust 本地收件与状态回写、媒体库扫描、`/file` Mini App、预览确认移动及旧内容 metadata patch。第五阶段 `/settings` 与新下载位置选择仍延期。
- Cloudflare 等待卡片重复问题已修复，类型检查、23 项测试及 Wrangler dry-run 已通过。Rust 完整测试为 572 项通过、0 项失败、11 项忽略，包含 mock Telegram/cloud 和文件管理覆盖。
- 未创建生产 Cloudflare 资源或配置运行时密钥，未修改 Telegram webhook，也未部署 bot。实际休眠、配额和 webhook 切换验证尚未发生。
- 实施分支 `wip/file-management-cloud` 基于更新后的 `origin/master`（`b5cc42e13a4f0d3bb939a2f95b15cc4b6ea4ac16`）。本机已安装的 bot 和其他 worktree 保持各自原状；本记录保持 `active`，直到最终本地 gate、后续生产验证及第五阶段范围明确。

## 已确认的产品与交互

### 入口与设置

- Telegram 的 `/file` 命令打开文件管理 Mini App；`/settings` 命令打开独立的设置 Mini App。两者可复用后台服务和数据模型。
- 设置项至少包括下载根目录和默认分类；登录状态属于未来设置范围。
- 新建下载时的目标目录在任务创建时冻结。之后修改下载根目录或默认分类只影响未来任务，不隐式迁移已有文件。历史文件迁移必须单独预览并确认。

### 三种使用场景

1. **整理已有内容（最高优先级）：**扫描已有媒体，利用可靠线索匹配来源和附件，处理歧义，选择真实目标文件夹，预览并批量应用。
2. **查找媒体：**在根目录范围内搜索和浏览媒体、合集、实际副本及其详情。
3. **为新下载选择位置：**在创建下载任务时选择根目录内的真实分类文件夹；使用当时的设置冻结该任务的目标。

### 文件库与详情

- 主视图包含“文件库”和“待整理”。“待整理”是文件库的工作流子集，不是独立媒体来源。
- 支持搜索、浏览分类、展开合集、多选合集成员和部分选择；返回列表时保留搜索词、滚动位置及选择状态。
- 详情显示媒体的实际规格、不同画质及实际副本、相对路径与完整路径，并提供复制路径和导航到文件位置的操作；同时显示已关联附件。
- 分别展示来源可用性、来源匹配置信度、元数据完整性和文件内容完整性。文件存在不代表视频内容完整；来源无法访问时仍可管理本地文件，并把来源标为未知/不可用。

### 云端查询状态与过期展示

- 用户接受 Rust 同步五类查询数据：本机最近联系状态、任务状态、文件库、已生效设置和操作结果。Worker 据此组装查询回复；登录只同步有效性，凭证保留在本机。
- 状态过期后仍允许浏览和提交请求，明确展示最近联系、上次扫描或最后报告时间。尚未生效的设置变更与当前生效值分开展示；没有首次同步时显示等待同步，不误报空文件库。
- 过期不删除数据或待执行请求。Rust 执行时重新检查实际文件；方案发生实质变化时要求再次确认。

### 批量整理与安全执行

整理采用明确的批量步骤：**选择项目 → 选择目标目录 → 预览 → 一次确认 → 执行并查看进度**。

- 预览逐项列出目标路径、拟用名称、将随媒体移动的附件以及冲突。
- 高置信度且唯一的匹配可以自动纳入批次；低置信度或有歧义的匹配集中呈现给用户确认。
- 目标已经是原位置时按无操作处理。存在同名冲突时可跳过或保留两份；不得静默覆盖原目标。
- 执行前重新核对受保护的对象身份、内容和访问策略。只有这些受保护属性或操作方案发生实质变化时才要求生成新预览并再次确认；不得把任意 `stat` 差异视为内容修改。File Provider 物化引起的无害元数据变化本身不算内容修改。
- 操作持久化、可幂等。关闭 Mini App 不取消批次；失败时保留原始内容，重试只处理未完成项目。每批只使用一条 Telegram 消息，通过编辑该消息报告进度和完成结果。

## 文件范围、身份与组织

### 扫描范围和索引

- 所有媒体库内容都假设位于配置的下载根目录内。只对该根目录及子目录进行有界递归扫描；不扫描全盘或根目录以外的位置。
- Bot 发起的文件移动必须留在下载根目录内。若用户用 Finder 把媒体移出根目录，只在库中标记为未找到，不追踪根外新位置。
- 扫描识别标准下载，也要识别 Finder 在根目录内手动移动或改名后的文件。排除私有队列、staging/暂存及其他私有内部目录。
- 下载失败片段长期保留，并集中放在一个私有子目录中；该目录不进入媒体库。
- 索引可以从目录内容、NFO 和任务记录重建，不为每个视频额外创建 JSON。新下载或整理操作结束后更新索引；索引不是权威媒体内容。

### 匹配、元数据与状态

- 标准下载和 Finder 手动移动/改名后的内容，优先根据任务记录、NFO 与稳定来源 ID 自动识别；文件名和标题仅作为弱线索。高置信度且唯一的匹配可自动处理，歧义项集中等待确认。
- 区分来源条目身份、实际媒体版本、物理文件副本、当前位置和合集关系。Bilibili 多 P 以 BV+CID 作为稳定条目身份；合集顺序和 P 序号只用于排序或展示，不作为身份。
- 多画质和实际文件副本分别记录，不自动合并或删除。内容哈希可辅助核对相同性，不能单独证明下载完整。
- NFO 是可随媒体携带、可移植的信息来源；与媒体同目录、同 basename。保留现有 NFO 自定义字段。索引可重建，不为每个视频再生成一个应用 JSON sidecar。
- 字幕、弹幕和封面作为媒体附件组一起移动。只有能可靠匹配时才建立附件关系，不能仅凭同名或相邻目录断言裸附件归属。

### 命名和目录布局

- 命名可选包含合集顺序、P 序号、标题、来源 ID、画质/编码，例如 `03 - P02 - Title [BV...-CID...][1080p-AV1]`。独立单视频不添加顺序号；合集顺序与 P 序号用于展示和可选命名，不定义稳定身份。
- 远端合集顺序变化不自动改名。
- 合集内容放在独立子目录；独立的多 P 投稿使用自己的子目录。合集内部的多 P 文件平铺在合集目录中，不再为该投稿额外套一层子目录，以便播放器连续播放。
- NFO 与视频使用相同 basename 并放在同一目录；关联字幕、弹幕和封面作为附件组随视频一起移动。

## 旧内容整理与线索

- 旧文件可复用本地任务记录、用户提供的 Telegram Desktop 导出、用户转发或提供的历史消息，以及现有媒体文件本身。可为现有媒体补充元数据并整理到统一规范，而无需重新下载完整视频或强制转码。
- Bot 无法读取用户所有私人聊天历史。历史队列记录不是完整媒体库；它只能作为匹配线索，不能替代目录扫描事实。
- 来源链接或服务不可用时，仍允许本地查找和整理，并将来源未知/不可用状态单独显示。元数据是否完整与媒体文件是否完整分开判定。

## 当前架构与本地实现状态

### 已接受的大方向

- 用户已接受公开托管静态 Mini App，使页面能自动加载文件库并自动提交结构化操作；不要求用户手动导入或发送文件快照。
- 更新链路的大方向是 Telegram webhook → Cloudflare Worker → D1 durable inbox；同一个 bot 的本机 Rust 服务主动领取待处理操作，并直接调用 Telegram Bot API 发送或编辑进度消息。本机不开放公网入站端口。
- 已接受使用休眠 WebSocket 通知本机的方向：Rust 主动连接 SQLite-backed Durable Object；Worker 先持久保存需要本机处理的新请求，再调用该对象唤醒并发送通知。Telegram 请求和 Mini App 操作共用该入口；只读云端查询不必唤醒本机通知对象。
- Rust 的状态回写和 Telegram 出站回复不经过通知对象。D1 不会因写入而自动唤醒该对象；状态经固定 Worker API 回写，不向本机运行时分发 Cloudflare 数据库管理 token。连接建立、关闭和应用消息也可能唤醒对象，协议 ping/pong 不打断休眠。断线、漏通知或通知失败时从持久收件箱补领，不删除已保存请求。
- Cloudflare Free 是偏好；超出额度时暂停新操作并向用户提示。
- 上述 Worker、D1 migration、静态 `/file` 页面、Rust API 与本地持久收件均已在分支中实现。它们仍处于本地代码和模拟测试阶段，不代表已经创建或部署生产资源。

### 已确认的单人单机鉴权

- 首期只有一个使用者、一台部署机和一个 Worker，不引入凭证管理界面。
- 本地配置支持 Worker 地址和共享密钥；HTTPS 请求及 WebSocket 握手使用 `Authorization: Bearer`。密钥从私有配置或 `TELEGRAM_VIDEO_DOWNLOADER_CLOUD_SHARED_SECRET` 环境变量读取，不序列化到传给下载 worker 的配置，也不交给 Mini App。生产密钥尚未生成或配置。
- 需要更换密钥时手动更新两边，不引入设备注册、自动签发、到期续期或多机权限管理。本机新增连接配置为 Worker 地址和共享密钥。
- Worker 验证 Mini App 的 Telegram `initData`，并绑定配置的单一 owner。生产 owner 和 bot secret 尚未配置或验证；Cloud 模式要求本机 allowlist 只有一个正私聊 ID，部署时须与 Worker owner 一致。

### 已确认的领取与持久确认边界

- 正常在线时，Worker 先把新请求持久写入 D1 并分配唯一序列号，再通过 WS 直接推送该编号和完整 payload，省去一次领取请求。
- Rust 先把完整 envelope 原子写入私有本地 inbox，落盘成功后逐条 `POST /api/local/ack`；ACK 表示本机已持久接收，不表示操作已完成。WS 没有独立 ACK frame。ACK 失败时 REST backlog 重送，完全相同的 sequence/envelope 由 inbox 去重。
- WS 推送和 HTTPS 补领使用同一请求编号及相同的本地持久化、去重流程。启动、重连和漏通知恢复时补领未确认记录；不能仅按已见最大序号过滤旧序号，以免并发推送乱序时漏掉请求。
- REST ACK 使用固定 Worker API 和本地 Bearer 鉴权；不会批量塞入状态回写，也不会要求 WS 返回 ACK frame。
- 已接收与执行完成是不同状态，实际执行结果另行报告。云端负责请求送达，本地持久队列负责执行恢复。
- 云端确认丢失导致同一请求再次送达时，以同一操作编号关联已有本地任务，不重复创建任务。网络重试沿用原操作编号。
- 用户主动发起重试或恢复时使用新的操作编号，并沿用既有新 generation 消息机制；这些明确的新操作不能被旧请求的去重规则吞掉。
- 本期使用单机持久接收，不引入多机领取租约。HTTPS 接收确认与操作完成分离；本地递增 `state_version` 控制快照顺序，毫秒时间戳用于 Worker 新鲜度，扫描时间使用秒。已连接的 WS 不因空闲而主动断开；HTTP fallback 低频读取 D1，不调用通知对象。实际云端休眠与生产切换仍需部署验证。

### 仍待验证的边界

- 本地代码已实现启动/重连 REST 补领、空闲保持 WS、收件后 HTTPS ACK、递增版本状态回写、授权的单 owner 配置校验和 Mini App `initData` 验证；真实 Cloudflare hibernation、空闲计费和配额行为仍待部署实测。
- 本地 mock 覆盖 Worker/Telegram 边界，但不能证明生产 bot owner ID、密钥、现存待处理更新和 webhook 状态已正确配置。生产切换前要核对 owner 对齐、保留旧 update、避免 webhook 与 `getUpdates` 同时消费。
- 附件关联和来源线索仍有歧义场景；旧 NFO 的跨媒体库互操作需要实测。已导入线索不会自动覆盖文件 metadata，必须手动映射、预览并确认。
- `/settings`、下载根目录/分类设置和新任务目标冻结属于阶段 5，仍未实现。

### 旧候选和已完成的文档调研

- 最早的方案希望完全无公网入站，并考虑静态页面手动导入文件库快照、以 `sendData` 返回选择。该方案已被后续“公开托管静态界面、自动加载数据和自动提交操作”的方向替代，不再是当前必须满足的用户体验要求。
- Telegram Serverless Mini App 仅完成公开文档调研，没有开通功能、修改 webhook、运行 CLI 或部署页面。它是历史调研结果，不是当前已选定的托管方案。
- 既有调研指出官方文档支持部署静态前端，但关于零 handler/endpoint 项目以及与当前 `getUpdates` 长轮询的兼容性未实测。不得根据文档推断已有 bot 的 webhook 状态会安全兼容。
- 已讨论的 Cloudflare Tunnel/本机公网管理 API 不是当前大方向；本机继续不接收公网入站请求。

## 实施计划

第一至第四阶段已有对应本地实现和测试覆盖。第五阶段仅保留既有范围，仍延期。生产资源、凭据、配额、休眠与 webhook 切换仍需协调并实测。

最初代码核对基于 canonical checkout 的 `e108262dfb11b5a43ff135bb0b029beb7d8b12a4`。实际实施已从更新后的 `origin/master`（`b5cc42e13a4f0d3bb939a2f95b15cc4b6ea4ac16`）建立 `wip/file-management-cloud`，复用当前任务的隔离 worktree；canonical checkout 和已安装的 bot 保持原状。

### 第一阶段：云端接收与现有 bot 接通

- 已实现 Worker/D1 收件、HTTPS 领取/ACK/state API、Bearer 鉴权、WS 通知、Rust 私有 durable inbox、准确 sequence/envelope 去重、乱序收件和重启恢复；现有 Telegram `getUpdates` 路径在 cloud 配置缺省时继续工作。
- WS 和 REST 共享先落盘再逐条 ACK 的边界；ACK 丢失时 D1 重送由 inbox 去重。WebSocket 长时间空闲保持连接，只有真实关闭或错误才重连；REST fallback 读取持久 backlog。
- 本地 mock 覆盖 Cloud REST 落盘/ACK/重启、Telegram `/queue` 分派、重复与乱序请求、云端 ACK 失败重送；新增了空闲超过 75 秒后 WS 收到请求的虚拟时间回归，已纳入通过的完整 Rust 测试。mock 不能替代生产 hibernation 或 webhook 切换验证。
- 生产切换仍需处理旧待收 update，并确保同一 bot 不同时由生产 webhook 与 `getUpdates` 消费；本轮没有执行这些操作。

### 第二阶段：本地媒体库与可重建索引

- 已实现 root-bound 的可重建 library、NFO 读取/保留未知字段、完成任务的逐 entry 稳定 ID hints、附件与合集线索及有界扫描。扫描仅作用于 configured video root，跳过私有内部目录、symlink 和 special file；仅完整遍历的目录可证明旧条目缺失。
- `ffprobe` 使用已打开的 no-follow descriptor 作为 stdin，并在 File Provider 协调结束后执行；耗时与输出大小均受限。初始 scan 或后续 refresh 失败时保留旧 snapshot 并报告脱敏 warning。
- 启动、显式 scan、发布下载完成后 debounce 触发 scan；五分钟 heartbeat 仅回传缓存 snapshot，不扫描。扫描与 File Provider 场景的本地测试已通过。

### 第三阶段：首个完整的 `/file` 整理流程

- `/file` Mini App、library snapshot 查询、搜索/选择、移动 preview 和 `file_confirm` 已实现。每个受选文件与目标均 root-bound；执行时重验 preview identity、SHA-256 和访问策略，按每文件 journal 持久恢复，不把多文件移动当作原子事务。
- 目标冲突支持跳过或保留两份，不静默覆盖。每个确认批次尝试创建一条 Telegram status 消息并编辑同一消息；Telegram `sendMessage` 没有 idempotency key，若接受与本地保存 message ID 之间进程退出，程序避免重复发送并报告关联不确定，Mini App 状态为操作结果来源。
- 关闭 Mini App 不会取消已收件的批次。File Provider 暂时不可访问使用持久 backoff；永久或 stale preview 需重新预览。错误与 restart recovery 场景已纳入通过的本地测试。
- 校验分别保护对象身份、内容稳定性和 POSIX 访问策略。创建时间单独变化时，仅在同进程仍持有原文件 descriptor、当前路径仍指向同一对象且类型、体积、SHA-256、owner/group/mode 全部匹配时允许继续；重启或缓存淘汰后没有该证明则要求重新预览。不把 mtime、ctime 或 link count 的任意差异当作内容变更。

### 第四阶段：旧内容补齐与迁移

- 已实现 legacy text 导入报告、hint 和候选项。候选仅作为线索，不会自动改写媒体 metadata。用户选择 hint 与 library item 后，UI 提交 `metadata_patches`，Rust 生成含 NFO 变更摘要的 preview，再由 `file_confirm` 执行；无可靠候选时允许用户手动选择 item，但仍要 preview 与确认。
- 确认批次可同时移动和补 metadata；patch 仅对可核验字段生成 NFO 更新，逐字段显示原值与新值及实际加入的来源 ID，保留未知 XML 字段和普通 POSIX 权限。组织路径支持合集目录与多 P 排序信息。
- 历史 Telegram 导出格式、裸附件可靠匹配和其他媒体库 NFO 互操作仍有覆盖边界；这些不是“自动导入/自动覆盖”功能。本地 metadata preview/确认测试已通过，其他媒体库互操作仍需实测。

### 第五阶段：`/settings` 与新下载位置选择

- 独立设置 Mini App 支持下载根目录及默认分类，明确区分已生效值和待应用请求；设置变化不隐式移动历史文件。
- 新下载可选择根内分类目录，并在任务创建时冻结目标；已有任务恢复仍使用其原始目标。登录状态整合保留为后续扩展，不要求首期导入云端凭证。
- 验收：设置重复提交、本机离线、任务创建与设置变更交错、任务恢复和无效目标目录；历史任务和文件不会随默认设置变化而悄然迁移。

### 测试与交付约定

- 已新增 Rust 单元与 mock Telegram/cloud 测试、Cloudflare Worker 测试、TypeScript 检查及 CI workflow。Node.js 25.8.2、Wrangler 4.149.0 下，`npm run typecheck`、`npm test`（23/23）与 `npm run build`（仅 dry-run）通过。
- Rust 1.95.0 下，`cargo fmt --all -- --check`、`cargo clippy --all-targets --locked --offline -- -D warnings`、`cargo build --locked --offline` 均通过；`cargo test --all-targets --locked --offline --quiet` 完整结果为 572 passed、0 failed、11 ignored。忽略项不计为已执行通过，本地构建没有安装或重启 bot。
- Rust 通过的覆盖包含本地 Cloud REST 落盘先于 ACK、ACK 失败后重送、重启恢复、乱序与重复序列、`/queue` 分派、WebSocket 空闲后收件、队列发布触发 scan、Telegram 单条批次 receipt 和执行失败编辑。
- 实现与修复由 GPT-6 Luna Max 子代理承担，GPT-6.1 coordinator 统一执行最终本地 gate。新增 Cloudflare CI 与现有完整 Rust CI 一起覆盖本次功能；尚未推送，因此没有远端 CI 结果。
- 文件操作使用合成目录，并区分对象身份、内容稳定性和访问策略；实际部署还要单独验证云端配额、owner/鉴权、休眠连接及 webhook 切换。mock 结果不能替代生产验证。

## 待决事项

- 生产接通后的端到端验证与远端 CI 结果；本地 mock 通过不代表已接通生产 bot。
- 附件文件的可移植关联清单，以及历史裸字幕/封面的可靠归属策略；NFO 扩展仍需其他媒体库互操作验证。
- 生产部署前验证两边 owner ID 对齐、密钥配置、Cloudflare 配额和真实 hibernation 行为，并制定保留旧 Telegram updates 的 webhook 切换方案。本轮没有创建生产资源或切换 webhook。
- 阶段 5 `/settings`、默认分类和新下载目标目录冻结仍未实现，待前四阶段最终 gate 后再规划。

## 后续事项

- 保持当前生产部署，协调其他 worktree 后再安排云端接通和真实端到端验证。
- 在最终本地 gate 通过后，单独准备生产配置和部署验证；需显式核对 owner、secret、配额、hibernation 及 Telegram webhook 切换条件。
- 继续完善附件和旧 NFO 的互操作验证；用户导入的 Telegram Desktop 文本仍只作为线索，必须手动映射、预览与确认。
- 阶段 5 的 `/settings` 和新下载位置选择待本轮阶段 1–4 完成后再启动。
- 弹幕 append-only 更新仍是独立待办，按 `docs/PROJECT_TODO.md` 跟踪，不属于本文件管理设计的实现范围。

## 依据

### 当前代码线索

- `src/cloud.rs`、`src/main.rs`、`src/cloud_bot_tests.rs`：固定 Worker API、Cloud inbox、WS/REST ingress、状态回写、Telegram 分派与本地 mock 覆盖。
- `cloud/src/` 与 `cloud/public/file/`：Worker/D1/通知对象 API 和静态文件管理 Mini App。
- `src/library.rs`、`src/file_manager.rs`：snapshot/hint/preview 类型、root-bound scan、NFO patch、move journal 和恢复。
- `src/queue.rs`：完成下载后的持久发布记录与扫描通知；下载任务 ID 去重仍独立于 cloud inbox 的请求 sequence。
- `src/downloader.rs`、`src/safe_fs.rs`：现有来源 identity、NFO 和 no-replace/root-bound 文件操作线索。
- `src/config.rs`、`config.example.toml`：可选 Cloud 设置；本地运行时密钥配置尚未应用。
- `BBDown-rust` 依赖源码中的 `crates/bbdown/src/client.rs:2507-2523`、`crates/bbdown/src/models.rs:149-165`：Bilibili 多 P 条目共享 BV、各自带 CID；条目 ID 可区分多 P。这些路径属于依赖仓库，不是本仓库子目录。
- `src/downloader.rs:5294-5304`、`src/downloader.rs:12956-12997`、`src/downloader.rs:13177-13212`：sidecar 类型及现有路径映射；历史裸字幕/封面不能总是可靠归属。

### 外部参考（待按方案核实）

- [Telegram Bot Web Apps](https://core.telegram.org/bots/webapps)
- [Telegram Serverless](https://core.telegram.org/bots/serverless)
- [Telegram 更新接收方式](https://core.telegram.org/bots/api#getting-updates)
- [Telegram `messages.getHistory`](https://core.telegram.org/method/messages.getHistory)
- [Telegram Desktop: Export and More](https://telegram.org/blog/export-and-more)
- [Kodi Movie NFO files](https://kodi.wiki/view/NFO_files/Movies)
- [Cloudflare Tunnel](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/)
- [Cloudflare Quick Tunnels](https://developers.cloudflare.com/tunnel/get-started/quick-tunnels/)
- [Cloudflare Workers pricing](https://developers.cloudflare.com/workers/platform/pricing/)
- [Cloudflare Durable Objects pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/)
- [Cloudflare WebSocket hibernation](https://developers.cloudflare.com/durable-objects/best-practices/websockets/)
