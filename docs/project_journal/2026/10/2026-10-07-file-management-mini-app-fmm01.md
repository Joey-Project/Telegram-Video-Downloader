---
id: 20261007-fmm01
title: 文件管理 Mini App 设计与实施计划
status: active
created: 2026-10-07
updated: 2026-10-20
branch: "wip/file-management-cloud"
pr: "https://github.com/Joey-Project/Telegram-Video-Downloader/pull/32"
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
- Cloudflare 等待卡片重复问题已修复。2026-10-19 的类型检查、75 项 Cloud 测试及 Wrangler dry-run 通过；Rust 1.99.1 的格式、编译、Clippy、调试构建及完整测试通过，完整测试为 1020 项通过、0 项失败、16 项忽略，包含 mock Telegram/cloud、文件管理、状态版本恢复、单集发布记录、来源匹配和有界状态清理，以及 YouTube legacy 提取、纯标题导入、目标目录深度、确认恢复提示、导入来源恢复、队列快照、hint 唯一映射、长附件名改名、零条目上传缩减和预览 anchor 可用性覆盖。
- 未创建生产 Cloudflare 资源或配置运行时密钥，未修改 Telegram webhook，也未部署 bot。实际休眠、配额和 webhook 切换验证尚未发生。
- 实施分支 `wip/file-management-cloud` 初始基于 `b5cc42e13a4f0d3bb939a2f95b15cc4b6ea4ac16`，后续整合截至 `afbf09300bf5ac7c0dbdc11ac5d1e10f934336fb` 的 master 更新，包含 ASS 弹幕显示默认值、恢复工作流收尾、Cargo 输出目录之外的签名服务安装流程及切换记录，并补齐 Cloud 模式的服务就绪兼容。本分支未安装或重启 bot，也未修改其他 worktree；本记录保持 `active`，后续生产验证及第五阶段范围仍需推进。
- Python 3.14.5 的 101 项服务单测、隔离 shell 集成测试与 shell 语法检查均通过，覆盖私有 shared secret 传递、来源切换和回滚；此前 shell 修改也已通过 ShellCheck。测试使用临时 HOME 和模拟系统命令，不安装或重启实际服务。CI 已纳入 Python 与 shell 服务测试。

### 2026-10-19 Isolated integration validation

- Isolated real Cloudflare validation covered Worker/D1 authorization, persist-before-notify, request deduplication, ACK replay, disconnected backlog recovery, and versioned snapshots. The reviewed Rust client also recovered from a stale local state version. A WebSocket received a notification after 20 seconds idle; this does not directly observe the Durable Object hibernation lifecycle. Static HTML, JavaScript, and CSS passed HTTP/MIME checks. Production webhook cutover, quota observation, and Telegram-client browser E2E remain rollout work.
- Real integration exposed a manual metadata guard mismatch: dispatch intentionally skips import lookup for patches without `hint_index`, but validation still required an import. Manual patches now use current-item and preview-confirm validation independently; explicit hint mappings retain completed-import requirements.
- Review fixes reject supported HFS-ignorable private-directory aliases before enqueueing, compact oversized legacy import reports against the complete persisted receipt while retaining a usable hint prefix and stable candidate indices, and open one searchable manual target picker with at most 100 present-media options.

### 2026-10-20 Recovery boundaries

- The local inbox keeps exclusive ownership while any inbox clone remains alive and explicitly unlocks after the final clone is dropped. This prevents a child holding an inherited descriptor from prolonging ownership. A macOS probe reproduced descriptor inheritance and explicit-unlock behavior; the original CI failure's specific child process was not observed. The affected startup recovery test also passed alone and in 15 bounded repetitions.
- Library and Needs Organizing views paginate actual media rows before rendering, preserve cross-page selections, and filter the full uploaded library. Folder suggestions are limited to 100 matches per input while typed paths remain available. Legacy text parsing recognizes LF, CRLF, and bare CR separators. Snapshot restoration reserves operation-detail space for the newest usable completed import before other results.
- Collection metadata patches preserve structured Kodi `<set>` children and attributes while updating the nested name; ambiguous structures fail before writes. A metadata-only preview/confirmation/rescan regression covers the retained fields and updated title.
- Legacy mappings use the newest usable completed import, render at most 50 candidate/manual rows per page, and retain selections and preview patches across pages. Every hint supports a manual alternative, even when an automatic candidate is available.
- Attachment processing stops incomplete item construction when the scan budget expires, including after the final sidecar read. Prior indexed metadata stays unavailable for management without losing its fields or queue provenance; new partial items are not published or indexed.
- The macOS installer removes a persisted Cloud shared-secret environment override when the configuration provides the secret and the installer has no non-empty replacement environment value. Explicit environment replacements still take precedence, environment-only upgrades retain their saved value, and unrelated environment entries are preserved.
- Local validation passed with 1,046 Rust tests, 0 failures, and 16 ignored tests; 86 Cloud tests passed. Formatting, all-target compilation, strict Clippy, the debug build, Cloud typechecking, and the Wrangler dry-run also passed. HTTP retry regressions compare actual request timestamps instead of short negative timing windows, preserving the configured 100 ms retry and 500 ms exhaustion-backoff assertions under concurrent test scheduling.
- State uploads keep their pending content and version after HTTP 400, refresh the timestamp on each attempt, and use backoff so correcting clock skew can recover without another library change.
- Cloud previews check the complete prospective inbox receipt before saving descriptor anchors or a manifest. Oversized selections receive an actionable split request; smaller previews retain the normal confirmation flow.
- The local inbox is scoped to an authenticated D1 epoch. A replacement database gets a separate local namespace; legacy and retired records keep their original paths and inode bindings. A same-epoch sequence collision conditionally rotates the cloud epoch using the current server request identity, then stops the runtime for a fresh start. ACK transactions compare the complete server request identity as well as the epoch, including when a restore occurs between persistence and ACK. Stale ACK and state writes are rejected transactionally. Retired records conservatively protect existing private preview manifests from cleanup.
- Active task summaries select candidates from the committed queue index before reading task records, so retained history does not trigger repeated record reads under the global queue operation lock. Startup reconciliation repairs indexed active records from their durable revision and timestamps, then persists terminal move intents before renaming completed or cancelled records into history. Ordinary queries keep their bounded record reads and existing File Provider and identity checks.
- Cloud task summaries retain task identities, statuses, and counts while bounding display labels and redacted errors to 1 KiB and 2 KiB of UTF-8 text. Actual JSON encoding is limited to 8 KiB per summary and 1 MiB per task array, including escaping overhead; truncated text is marked and the complete queue records remain unchanged.
- Confirmation receipts persist an immutable local receipt timestamp. Preview expiry and confirmation admission use local seconds, independently of the Worker's request clock; processing and restart recovery do not reset receipt time. Legacy active confirmations without a local receipt timestamp conservatively retain their recovery and preview protection.
- Legacy title hints normalize Unicode whitespace to single spaces before candidate matching and enforce the same control-character and length limits as metadata patches. Tabs in imported titles therefore cannot create mapping actions that the Worker permanently rejects.

### 2026-10-18 审查修复

- Mini App 在当前 owner 身份验证成功后才显示私有文件库；授权失败隐藏私有内容并暂停发送。永久拒绝的动作不再阻塞后续队列，网络、限流和服务端错误沿用原请求 ID 重试。
- 失败预览保留在操作历史，不可用媒体不可选，手动关联单个元数据字段可以进入确认流程，移动预览按实际元数据变化显示标题。
- Telegram 更新取得执行许可后才标记处理中；本地收件保留持久去重记录，待处理及状态摘要使用独立索引。任务状态与进度触发合并后的状态同步，单集发布仍单独触发文件库扫描。
- 超大状态上传和生成结果有独立失败恢复；永久失效的确认会关闭旧预览并要求重新预览。活动任务摘要覆盖多页，元数据字段限制与 Worker 对齐。
- 文件冲突跳过不再计为成功；NFO 完整度按字段判断，YouTube 来源及合集 ID/类型可以往返保存，同一物理对象移动后的队列元数据可以跨重启保留。旧记录关联使用同一次带队列线索的扫描；已确认缺失的历史索引有数量及体积上限。
- 后续审查补充了启动和 WS 通知前的有序权威补领、owner 未配置时拒绝确认 webhook、暂态部分失败后保留原预览、状态历史的编码体积预算、认证恢复重试、终态等待卡片过滤，以及自闭合 NFO 元素的安全连续修改。过大的历史结果明确标记截断；最新导入报告优先保留供手动关联，不完整预览禁止确认。
- HTTP 模拟覆盖 503 同版本重试、认证恢复、操作历史压缩、低于本地软限制仍反复 413 时的真缩减、裁剪成功后的心跳续期及后续状态更新，并覆盖无法继续缩减时停止分配版本。暂态确认通过测试专用 provider 注入真实 File Provider access error 贯穿 dispatcher，验证部分移动后沿用原预览继续完成；独立文件管理测试还验证了重开 manager 后恢复。生产休眠、真实配额与 webhook 切换仍留待部署验证。
- 状态计数器丢失或迁移后，已鉴权的 Worker 在冲突响应中提供已保存版本，本机持久保存更高版本后重试原快照；缺失或无效提示继续退避恢复，计数耗尽不永久屏蔽相同内容。Mini App 的可见项、合集和单项选择共同遵守每批 100 项上限，提交前再次检查并提示分批操作。
- 大文件库快照使用独立的有界复杂度预算，节点或深度超限以 413 进入已有缩减重试。扫描按目录及 stem 预建附件索引，最长匹配优先，歧义附件保留原位；可读取的坏索引在身份、内容及访问策略复验后隔离并重建，保留数量与读取体积均有上限。预览 descriptor anchors 按闲置时间和最近使用情况淘汰，终态释放；目标预留遵循支持的卷命名规则，提前处理大小写及规范化别名冲突。
- 第五轮审查修复收紧了媒体元数据边界：旧索引缺失来源标记时不猜测队列数据；已记录的队列字段仅在文件对象身份匹配后继承。当前 sidecar 内容优先，修改或删除 sidecar 不会复活旧的 sidecar 字段。JSON 只读取明确的媒体对象及已知封套字段，不递归采集章节或播放列表条目；NFO 支持 CDATA，BV 前缀大小写不敏感但保留 ID 其余部分大小写。
- 队列提示先按现存精确路径独占绑定副本；仅当扫描确认该路径不存在且来源 ID 唯一时回退，别名、其他现存项、不可用路径或不完整扫描均不视为缺失。附件按目录与 stem 建索引，遵守文件系统大小写和 Unicode 规范化语义；最长媒体 stem 优先，歧义附件留在原位，移动时保留语言后缀。
- 清理策略为 D1 已确认请求的去重键保留 7 天，无关联批次的过期预览和终态移动记录也按 7 天期限回收；未终结或可重试的批次，包括旧记录中终态未知的批次，继续保留以便恢复。此清理不触碰下载片段或暂存恢复数据。
- 本地 Cloud inbox 的已完成、失败、过期及结果不确定记录同样保留 7 天；待处理、处理中、可重试及待确认记录继续保留。每轮维护最多检查 32 个目录名、读取 8 MiB，请求文件移除前复验对象、内容、访问策略与终态期限，并同步清除内存索引。
- 启动先只读建立收件索引，再分轮恢复处理中记录，完成前不对外暴露部分待处理或历史结果；目录条目集合变化会触发重新枚举。对应回归同时覆盖低序号新请求、恢复顺序、读取预算及真实过期记录，目录时间只作为重扫提示，不作为内容被篡改的证据。
- NFO 支持合法十进制及十六进制字符引用，单次解码且保持 CDATA 字面内容。预览按目标文件系统规则排除跨媒体共享的 NFO 目标；替换现有 NFO 前持久保存新对象 checkpoint，使原子替换已完成但最终状态未写入的批次可在重启后安全恢复。
- 旧内容导入在调用 `File.text()` 前先检查文件体积，读取后仍检查 UTF-8 编码体积，避免超大本地导出文件占满 Mini App 内存。
- NFO 元素读取、替换及根节点插入共用真实 XML 标签范围，跳过注释、CDATA、处理指令、声明及带引号属性内的伪标签，原有注释及未知字段保持原样。旧 JSON 文本收集改为单次累计字节预算并跳过空字符串，避免反复扫描已收集字符串。
- 导入标题、metadata patch 标题及合集标题在 Worker 和 Rust 各层统一为 trim 后最多 500 个 Unicode 码点，中文及补充平面 emoji 使用相同计数；Worker 在入队前拒绝孤立 surrogate。长度回归覆盖 500/501 边界，避免云端已接受后才在本机因计量单位不同而失败。
- 自闭合 NFO 标签展开时保留原始 opening name 的大小写，避免兼容读取后生成不匹配的 closing tag。旧消息的来源 ID 及标题自动候选均仅匹配当前 `Present` 媒体；Mini App 的历史候选也按当前文件库复验，不可用的旧候选保留为可手动关联的提示。
- 附件所在目录未完整枚举时，其中媒体暂不可移动，避免只移动视频却留下 NFO、字幕或封面；完整且无关的目录继续可管理。扫描超时及索引恢复不把未观察完整的路径误判为缺失。
- 扫描条目携带同一次 descriptor 读取的 fingerprint 用于稳定 ID 与旧索引关联，取消第二轮路径重开及其超时后留下的 provisional 条目；旧记录按路径及对象身份预建查找索引，未构建完媒体详情的已观察 alias 也参与歧义计数。此快照身份不替代预览与执行时的重新验证。
- 媒体探测排除内嵌封面后优先选择真实视频流，没有视频时读取音频编码且不填视频分辨率。合集顺序在 Worker 入队前校验为 `0..=u32::MAX`，避免云端接受后本机因整数范围不同而无法执行。
- 不完整扫描无法确认改名时，暂时不可用的新路径不写成会遮蔽旧 ID 的索引记录；相关旧记录保持不可用而非缺失，后续完整扫描恢复稳定 ID 及队列元数据。持久化的现存项省略可重建附件列表，快照保留完整附件；最小存活索引仍超 16 MiB 时保留上一份有效索引并返回带持久化警告的新快照，后续扫描可继续重试，不删除媒体或已有队列来源信息。
- 精确路径匹配不再遍历同一对象的全部历史别名；部分扫描按对象缓存尚未消费的候选，延期保护登记也只做一次。确定性访问次数回归用于限制大量硬链接时的候选查找工作，避免按路径反复遍历同一候选列表。
- 文件已枚举但暂时无法打开时，安全匹配的旧记录保留稳定 ID 并标为不可用，不发布重复的临时记录或误报缺失；不同替换对象不继承旧队列元数据。队列提示路径按每个组件父目录的实际文件系统命名规则比较，包含受支持的大小写、Unicode 规范化及 HFS 忽略字符行为。
- 初始扫描、下载发布扫描及管理操作共用本机扫描与发布锁，从阻塞扫描开始前持有到快照和警告发布结束，使较早扫描的异步恢复不会覆盖后续结果；失败警告也遵守相同顺序。
- 预览按七天窗口停止接受新确认，本机在过期前已持久接收且仍待处理、执行中或可重试的确认保留关联预览收件记录及实际私有预览文件供恢复；扫描与执行中的清理都在删除前复查活动确认，收件恢复未完成或查询失败时保留。过期预览在状态视图中显示失效，放弃确认的记录仍可清理。确认按 preview ID 和已落盘的确认序号查询；有效期比较统一使用不可变的本机秒级收件时间，不使用 Worker 请求时间或重试更新时间，缺少该时间的旧活动确认保守保留。
- 刷新失败时保留旧条目、revision 及原扫描时间，并立即同步脱敏警告；成功扫描后同步清除警告。上传缩减同时约束警告文本及分类目录，即使只有零或一个视频也可压缩，缺省分类以警告说明，不把截短路径作为可选目录。
- Mini App 静默轮询恢复身份校验后也会补发已保存请求。跨设备移动在预览阶段按每个媒体及附件检查源描述符与目标已存在父目录的设备，混合批中任一不支持的跨卷改名会阻止生成确认预览；跳过及原地不动项保持原语义。
- 云端以同一 D1 事务读取快照元信息及内容分片，返回前复验存储哈希，避免并发发布时混合旧版本与新内容；回归覆盖分片数量相同及变化时的状态和文件库读取。
- 合集目录按清理后的最终名称及目标文件系统规则检测碰撞，必要时按合集身份添加稳定区分后缀；顺序和分 P 不拆分同一合集。扫描按确定性优先级选择权威 NFO，预览及移动后修改都沿用其确切路径；已有 NFO 不可读或消失时拒绝修改，避免另建大小写或后缀不同的文件而令重扫仍显示旧值。

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
- 上述 Worker、D1 migration、静态 `/file` 页面、Rust API 与本地持久收件均已实现，并接受本地、mock 及隔离真实 Cloudflare 验证。隔离测试不代表已经创建或部署生产资源。

### 已确认的单人单机鉴权

- 首期只有一个使用者、一台部署机和一个 Worker，不引入凭证管理界面。
- 本地配置支持 Worker 地址和共享密钥；HTTPS 请求及 WebSocket 握手使用 `Authorization: Bearer`。密钥从私有配置或 `TELEGRAM_VIDEO_DOWNLOADER_CLOUD_SHARED_SECRET` 环境变量读取，不序列化到传给下载 worker 的配置，也不交给 Mini App。生产密钥尚未生成或配置。
- 需要更换密钥时手动更新两边，不引入设备注册、自动签发、到期续期或多机权限管理。本机新增连接配置为 Worker 地址和共享密钥。
- Worker 验证 Mini App 的 Telegram `initData`，并绑定配置的单一 owner。生产 owner 和 bot secret 尚未配置或验证；Cloud 模式要求本机 allowlist 只有一个正私聊 ID，部署时须与 Worker owner 一致。

### 已确认的领取与持久确认边界

- 正常在线时，Worker 先把新请求持久写入 D1 并分配唯一序列号，再通过 WS 推送该编号和完整 payload。Rust 在执行通知对应请求前仍先核对 D1 的权威有序 backlog，避免并发通知乱序造成命令反向执行；正确性优先于原先省去一次读取的优化。
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
- macOS 服务就绪检查按同一配置选择 Telegram 或 Cloud 收件模式，保留同 PID、安装二进制、工作目录和新增日志校验。Cloud 仅在完整 REST 对账、持久化与 ACK 成功后报告就绪，stderr 标记不受 `RUST_LOG=off` 影响；不伪造 Telegram 轮询，也不要求 WS 必须可用。
- 初始 inbox 恢复以有界页推进，新本地写入等待恢复完成；新请求仍保留在 D1 且不 ACK，接收路径自身继续恢复，不因持续新消息清空进度。外部目录条目变化仍要求完整性重扫，身份与访问策略独立核验。
- 运行中的 dispatcher 保留已完成操作的 outcome，终态 receipt 或关联 preview 写入暂时失败时只重试持久化；不重新调用 Telegram 或执行文件操作。跨进程退出仍采用原有 Telegram Uncertain 和文件 journal 恢复边界。
- 本地 mock 覆盖 Cloud REST 落盘/ACK/重启、Telegram `/queue` 分派、重复与乱序请求、云端 ACK 失败重送；新增了空闲超过 75 秒后 WS 收到请求的虚拟时间回归，已纳入通过的完整 Rust 测试。mock 不能替代生产 hibernation 或 webhook 切换验证。
- 生产切换仍需处理旧待收 update，并确保同一 bot 不同时由生产 webhook 与 `getUpdates` 消费；本轮没有执行这些操作。

### 第二阶段：本地媒体库与可重建索引

- 已实现 root-bound 的可重建 library、NFO 读取/保留未知字段、完成任务的逐 entry 稳定 ID hints、附件与合集线索及有界扫描。扫描仅作用于 configured video root，跳过私有内部目录、symlink 和 special file；仅完整遍历的目录可证明旧条目缺失。
- `ffprobe` 使用已打开的 no-follow descriptor 作为 stdin，并在 File Provider 协调结束后执行；耗时与输出大小均受限。初始 scan 或后续 refresh 失败时保留旧 snapshot 并报告脱敏 warning。
- 启动、显式 scan、发布下载完成后 debounce 触发 scan；五分钟 heartbeat 仅回传缓存 snapshot，不扫描。扫描与 File Provider 场景的本地测试已通过。

### 第三阶段：首个完整的 `/file` 整理流程

- `/file` Mini App、library snapshot 查询、搜索/选择、移动 preview 和 `file_confirm` 已实现。每个受选文件与目标均 root-bound；执行时重验 preview identity、SHA-256 和访问策略，按每文件 journal 持久恢复，不把多文件移动当作原子事务。
- 同一 preview 只做一次锁内 fresh scan，legacy mapping 校验、preview 构造和发布使用同一 snapshot。文件批次执行结果与之后的 refresh 分离；refresh 失败保留已完成结果、原缓存 snapshot 和独立 warning，不重新执行已完成的文件操作。
- 目标冲突支持跳过或保留两份，不静默覆盖。每个确认批次尝试创建一条 Telegram status 消息并编辑同一消息；Telegram `sendMessage` 没有 idempotency key，若接受与本地保存 message ID 之间进程退出，程序避免重复发送并报告关联不确定，Mini App 状态为操作结果来源。
- 关闭 Mini App 不会取消已收件的批次。File Provider 暂时不可访问使用持久 backoff；仅此类可重试批次的 Telegram 结果提示复用原确认，永久或 stale preview 明确提示重新预览。错误与 restart recovery 场景已纳入通过的本地测试。
- 云端在入队前逐段拦截内部私有目录前缀及其大小写变体，避免请求已被接收后才被本地文件系统语义拒绝；本地仍执行最终路径校验。
- 校验分别保护对象身份、内容稳定性和 POSIX 访问策略。创建时间单独变化时，仅在同进程仍持有原文件 descriptor、当前路径仍指向同一对象且类型、体积、SHA-256、owner/group/mode 全部匹配时允许继续；重启或缓存淘汰后没有该证明则要求重新预览。不把 mtime、ctime 或 link count 的任意差异当作内容变更。
- 缺少 creation time 的预览必须保有原始 live descriptors；anchor 过期、被淘汰或因重启丢失后，云端不再显示为可确认。只读可用性检查不续期；具备完整持久 token 的预览保留原有跨重启校验流程。

### 第四阶段：旧内容补齐与迁移

- 已实现 legacy text 导入报告、hint 和候选项。候选仅作为线索，不会自动改写媒体 metadata。用户选择 hint 与 library item 后，UI 提交 `metadata_patches`，Rust 生成含 NFO 变更摘要的 preview，再由 `file_confirm` 执行；无可靠候选时允许用户手动选择 item，但仍要 preview 与确认。
- 同一条 hint 在一次映射中最多选择一个视频；界面阻止重复选择，Worker 和 Rust 同时拒绝重复的显式 `hint_index`。没有 hint 索引的独立手动 metadata 编辑保留原有语义。
- 旧文本允许普通换行、回车及制表符，仍按解码后的 256 KiB UTF-8 上限校验并拒绝其他不安全控制字符或无效 Unicode；动作 JSON 的独立体积上限覆盖合法转义后的文本。
- legacy 候选按当前 snapshot 一次建立来源 ID 和规范化标题索引，保留来源 ID 优先、文件顺序和歧义确认语义。匹配工作量、候选条数和编码体积均有上限；截断会明确告知，保留的 hints 仍可手动关联。
- 所有文本块合计保留最多 10,000 个去重后的 hints，超限明确显示截断 warning；前后端都接受 0–9,999 的 hint 索引，合集 ID 都按 128 个 UTF-8 bytes、kind 按 64 个 UTF-8 bytes 校验。
- 已完成的 legacy 导入来源使用独立索引保留最新 100 份，不会因为新 preview 挤出管理视图的 100 条记录而立即失效；重开收件箱会重建索引，仍遵循原有七天终态过期策略。
- 云端任务摘要在队列操作锁内一次读取和选择最多 100 条活跃任务，保留允许聊天范围及稳定排序，进度更新不会在分页之间造成重复或遗漏。
- 纯标题旧记录也会生成 hints，沿用标题候选和显式手工关联；空行及没有受支持来源 ID 的纯 HTTP(S) URL 不生成标题线索。
- 目标目录在浏览器、Worker 和 Rust 都按 1,024 个 UTF-8 bytes 校验；确认页列出媒体及附件各自的源路径与目标路径。组织后最终父目录最多 48 层，超出扫描范围的目标在写入 preview 或创建目录前拒绝。
- 目标路径每个分量另有 255 个 UTF-8 bytes 上限，超长名称在产生可确认 preview 前拒绝。单个媒体条目过大时，上传副本可以省略全部 items 并报告遗漏数量和 incomplete 状态；本地完整索引保留，成功缩减后仍可继续心跳及更新状态。
- 整组改名按最长附件 qualifier 和扩展名为共同媒体 stem 预留空间，保证每个分量最多 255 个 UTF-8 bytes；保留两份时同样计入冲突编号。仅在 Unicode 字符边界缩短 stem，保留附件后缀；无法表示的名称在确认前明确拒绝。
- legacy text 复用现有 YouTube URL 识别规则，校验 host 和十一字符 video ID 后生成 `youtube:<id>` 来源线索，不把任意裸字符串当作视频 ID。
- 确认批次可同时移动和补 metadata；patch 仅对可核验字段生成 NFO 更新，逐字段显示原值与新值及实际加入的来源 ID，保留未知 XML 字段和普通 POSIX 权限。组织路径支持合集目录与多 P 排序信息。
- 历史 Telegram 导出格式、裸附件可靠匹配和其他媒体库 NFO 互操作仍有覆盖边界；这些不是“自动导入/自动覆盖”功能。本地 metadata preview/确认测试已通过，其他媒体库互操作仍需实测。

### 第五阶段：`/settings` 与新下载位置选择

- 独立设置 Mini App 支持下载根目录及默认分类，明确区分已生效值和待应用请求；设置变化不隐式移动历史文件。
- 新下载可选择根内分类目录，并在任务创建时冻结目标；已有任务恢复仍使用其原始目标。登录状态整合保留为后续扩展，不要求首期导入云端凭证。
- 验收：设置重复提交、本机离线、任务创建与设置变更交错、任务恢复和无效目标目录；历史任务和文件不会随默认设置变化而悄然迁移。

### 测试与交付约定

- 2026-10-16 整合 master 的 MP4 发布验证、独立分集规划与精确跳过、逐集结果/恢复/重试、磁盘不足暂停和压缩弹幕解码修复，保留 `bbdown-core` 最新固定版本。
- 稳定媒体 ID 解析使用固定长度数组切片 `as_chunks::<2>()`，保留原先偶数长度校验与解析行为，满足 CI 的 `chunks_exact_to_as_chunks` Clippy 规则。
- 媒体库按单集 `Verified` 发布记录的稳定 ID、路径和摘要生成来源线索，不把失败或待下载的全计划条目算作已发布；保留原始集索引和旧式整任务完成记录。新记录成功持久化后才通知扫描，普通进度不通知；任务的部分完成、恢复和历史状态语义保持独立。

- 已新增 Rust 单元与 mock Telegram/cloud 测试、Cloudflare Worker 测试、TypeScript 检查及 CI workflow。Node.js 25.8.2、Wrangler 4.149.0 下，`npm run typecheck`、`npm test`（23/23）与 `npm run build`（仅 dry-run）通过。
- Rust 1.95.0 下，`cargo fmt --all -- --check`、`cargo check --all-targets --locked --offline`、`cargo clippy --all-targets --locked --offline -- -D warnings`、`cargo build --locked --offline` 均通过；`cargo test --all-targets --locked --offline --quiet` 完整结果为 798 passed、0 failed、15 ignored。编译命令显式使用当前 worktree 的 target 目录，忽略项不计为已执行通过，本地 debug 构建没有安装或重启 bot。
- Rust 通过的覆盖包含本地 Cloud REST 落盘先于 ACK、ACK 失败后重送、重启恢复、乱序与重复序列、`/queue` 分派、WebSocket 空闲后收件、队列发布触发 scan、Telegram 单条批次 receipt 和执行失败编辑。
- 实现、修复与验证由 GPT-6 Luna Max 子代理承担，GPT-6.1 coordinator 统一协调交付。新增 Cloudflare CI 与现有完整 Rust CI 一起覆盖本次功能；远端结果以 PR 当前提交的 checks 和 Codex review 为准，不能沿用旧提交结果。
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
