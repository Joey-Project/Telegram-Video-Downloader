---
id: 20261002-qtd01
title: 队列任务详情与按钮区分
status: completed
created: 2026-10-02
updated: 2026-10-02
branch: codex/queue-task-details
pr: https://github.com/Joey-Project/Telegram-Video-Downloader/pull/24
supersedes: []
superseded_by:
---

# 队列任务详情与按钮区分

## 摘要
- `/queue` 每项以同一编号关联正文与按钮，正文显示状态、标题、分辨率、预估体积、媒体进度、任务 ID 和 URL。
- Retry、Resume、Confirm、Cancel 按钮显示动作、编号、短标题、画质与大小；原任务 ID、generation 和 callback action 保持不变。
- 标题优先采用对应计划快照；确认中的待确认计划会标为更新计划，只有 proposed plan 时会标注 Proposed。缺少快照或标题时使用任务类型回退，画质和体积显示 unknown。
- 合集显示合集标题与条目数；画质列出不同分辨率，并把缺少或无效的已选视频分辨率计为 unknown。大小按所选视频与音频流 subject 去重，精确值优先；不确定的总和显示为下界或 unknown。

## 验证范围
- 新增队列展示单测，覆盖混合合集分辨率与 `0x0`、exact/approximate/partial/missing/conflicting/overflow 大小、计划回退优先级，以及各状态按钮到原 callback 的映射。
- 扩展持久队列 mock Telegram E2E：重启后从已保存计划显示标题、分辨率和大小，并核对 Resume/Cancel callback 仍关联原任务 generation。
- 扩展十条长 Emoji 标题与 URL 页面回归，断言文本和每个按钮标签保持 Telegram 长度预算且全部任务操作仍保留。
- 未读取实时队列、配置、凭据或真实媒体，也未访问 Telegram。

## 检查记录
- `cargo fmt --all -- --check`、`cargo build --locked --offline` 与 `cargo clippy --all-targets --locked --offline -- -D warnings` 均通过。
- 五项新增单测与三项指定回归/E2E 共 8 passed、0 failed；包含 `persistent_queue_restart_and_telegram_interactions_e2e` 的实际 mock Telegram 文本、键盘和 callback 核对。
- `cargo test --all-targets --locked --offline --quiet` 通过：548 tests，537 passed、0 failed、11 ignored，47.85 秒。主套件启动的跨进程子测试已包含在该统计中。
- 使用 Cargo/Rust 1.95.0；测试在沙盒外运行本地 mock 与 fixture。构建、Clippy 和全量测试均针对同一最终源文件内容，验证前后源文件哈希一致。
- 项目日志校验与 `git diff --check` 通过。独立内部检查提出的 Failed 按钮测试期望已修正，保留原有仅 Retry 的行为；未运行 named single/double/triple review。
