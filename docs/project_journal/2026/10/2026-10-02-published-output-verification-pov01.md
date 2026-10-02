---
id: 20261002-pov01
title: 发布媒体路径校验
status: completed
created: 2026-10-02
updated: 2026-10-02
branch: codex/fix-published-output-verification
pr: https://github.com/Joey-Project/Telegram-Video-Downloader/pull/23
supersedes: []
superseded_by:
---

# 发布媒体路径校验

## 摘要
- staged 视频下载完成后，报告重建曾把 worker 报告中的活动 staging 源媒体路径当作最终输出路径保留；staging 位于下载根目录之下，因而错误路径也满足根目录前缀检查。
- 发布步骤本来已返回成功 move 的最终 destination。现在同时保留实际 move source→destination 记录；每个 worker 报告里的 staging 媒体源必须映射到成功发布的目标，否则任务失败并保留 staging。既有最终媒体路径通过下载根目录和文件对象身份验证后才合并。
- `AlreadyComplete` 报告拒绝活动 staging 和下载根目录外的媒体路径；媒体下载的空报告失败。清理 staging 前会验证最终输出仍存在且是同一常规文件对象。
- 三条运行记录中的错误目录都位于配置根目录下隐藏 staging，且严格处于各自唯一 staging attempt 的子目录中，与已确认的报告路径泄漏一致。没有额外读取或确认这些 attempt 内实际文件是否已发布到最终目录。

## 当前状态
- 文件验证保护下载根目录边界、常规文件类型和对象身份（device/inode）；内容稳定性由后续 descriptor-bound SHA-256 读取负责。无缺失目录创建、替代路径扫描或未经验证的字符串重写。
- staging 报告路径只通过成功 move destination 映射到最终路径；缺失或无法验证的既有最终路径会阻止报告完成，活动 staging 保留以便恢复。
- 回归覆盖嵌套 staging、同时保留新发布与已有媒体、staging 清理后的实际最终哈希，以及缺失最终 parent 时不创建目录并保留 staging。Telegram mock 集成覆盖这些最终路径进入完成记录、瞬时队列写入失败后的单次通知和完成重试。
- README 现有 SHA-256 与 staging 失败恢复说明无需改变；File Provider coordinated hash 不在本修复范围内。

## 检查记录
- `cargo test media_report`：4 passed，覆盖成功 move 映射、未发布 staging 源拒绝、staging 清理后哈希、缺失路径失败语义，以及 `AlreadyComplete` 的 staging、根外和空媒体保护。
- `cargo test nested_staging_paths_are_excluded_before_published_completion_e2e`：1 passed，覆盖最终媒体哈希进入任务完成记录及 File Provider 瞬时写入失败后的单次 Telegram 通知与重试；测试使用 mock Telegram localhost listener，在沙盒外执行。
- `cargo fmt --all -- --check` 与 `git diff --check` 通过。
- 完整套件首次沙盒外运行 530 passed、2 failed、11 ignored；两项失败共用的 progress task 在 5 秒 fixture timeout 后仍有消息待收尾，而独立精确运行均通过（2.77 秒、3.82 秒）。将该共享等待预算提高到 30 秒，保留有限超时和消息断言。
- 最终格式检查、构建、Clippy（`-D warnings`）、完整 Rust 测试、diff 检查与 journal 校验均通过；完整测试 532 passed、0 failed、11 ignored，耗时 55.50 秒。使用 Cargo/Rust 1.95.0，构建和测试采用 `--locked --offline`；localhost mock 和原生 File Provider 测试在沙盒外运行。
