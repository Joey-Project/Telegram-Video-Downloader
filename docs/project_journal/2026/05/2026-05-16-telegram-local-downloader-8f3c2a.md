---
id: 20260516-8f3c2a
title: Telegram Local Downloader Bot
status: completed
created: 2026-05-16
updated: 2026-06-04
branch:
pr:
supersedes: []
superseded_by:
---

# Telegram Local Downloader Bot

## Summary
- 第一版目标是个人本机长驻 Telegram bot：把支持的链接发给 bot 后，本机自动保存视频或网页 PDF。
- 主服务使用 Rust；PDF 打印 helper 使用 uv 管理的 Python Playwright，以降低 Chrome 自动化和 lazy-loading 滚动处理成本。
- 真实 `config.toml` 会包含 Telegram token，但必须被忽略；仓库只提交 `config.example.toml`。
- 第二轮增强目标是从 Telegram 文本中提取真实 URL，并为视频下载补齐播放器内嵌 metadata 与媒体库 sidecar。

## Confirmed Design
- Bilibili 路由：普通消息里的 `bilibili.com`、其子域名、`b23.tv` 链接调用本机 `BBDown`，工作目录为 `/Users/joey/Movies/Downloads`。
- YouTube 路由：普通消息里的 `youtube.com`、其常见子域名、`youtu.be` 等链接调用本机 `yt-dlp`，输出目录为 `/Users/joey/Movies/Downloads`。
- PDF 路由：图文网页必须显式使用 `/pdf URL`；helper 使用 Chrome 打开页面、逐步下滑到高度稳定后保存 PDF 到 `/Users/joey/Documents/Downloads`。
- 任务调度：全局并发限制由配置控制，默认 `2`；超过限制的任务排队。
- 状态反馈：bot 对每个任务回复已入队、开始处理、成功保存位置或失败摘要。
- 第一版不内置 cookie 管理；BBDown 和 yt-dlp 沿用本机 CLI 既有登录/cookie 配置。

## Implementation Plan
- 搭建 Rust 项目，包含配置加载、Telegram `getUpdates` polling、URL 路由、全局 semaphore 队列和外部命令执行。
- 搭建 uv Python 项目，提供 `scripts/pdf_helper.py` 和最小单元测试；依赖 Playwright 与 ruff。
- 提供 `config.example.toml`、`.gitignore` 和 README，明确 token、本机路径、CLI 路径、运行与验证方法。
- 实现 Rust 单元测试覆盖路由、命令分类和配置默认值；Python 单元测试覆盖 PDF 文件名清理等纯函数。

## Validation Plan
- Rust: `cargo fmt --check`
- Rust: `cargo clippy --all-targets -- -D warnings`
- Rust: `cargo test`
- Python: `uv run ruff format --check`
- Python: `uv run ruff check`
- Python: `uv run python -m unittest`

## Current State
- 第二轮增强和 downloader 可观测性 follow-up 已实现。
- Rust 主服务已包含配置加载、Telegram `getUpdates` polling、全文 URL 扫描、消息路由、全局并发限制、外部命令执行和状态回复。
- 外部命令现在流式采集 stdout/stderr、监控输出目录文件增长，并支持总超时与 idle timeout；Telegram 任务会转发节流后的进度消息。
- 新增 `--replay-message` 本地入口，可用真实消息文本重放路由和下载组件，不依赖 Telegram ingress。
- YouTube 下载会预取 yt-dlp metadata，优先人工字幕、fallback 自动字幕，并启用 metadata、封面、字幕、info JSON、description 和 NFO 输出。
- Bilibili 下载继续由 BBDown 负责，显式跳过 AI 字幕，默认追加 `--video-ascending` 以避开当前复现链接在后台模式下的高码率流卡住问题，并对新增视频生成 best-effort NFO。
- PDF 支持 `mp.weixin.qq.com` 自动白名单，`/pdf URL` 继续保留。
- Bilibili `opus` 文章链接现在会规范化为 `https://www.bilibili.com/opus/<id>` 并走 PDF；PDF helper 对这类页面使用静态 HTML 快照渲染，避开页面脚本在 headless Chrome 中主动关闭页面的问题。
- BBDown 登录态现在由 bot 通过 Bilibili Web QR API 管理：私聊 `/bbdown login/status/logout` 可扫码登录、查看账号、清理本机状态；Bilibili 下载会自动把 bot-managed cookie 注入 BBDown。
- 视频下载现在默认先进入隐藏 staging 目录，成功后再移动到最终目录；对可直接提取媒体 ID 的 YouTube/Bilibili URL，若本地已有匹配视频或 sidecar，用户可通过 Telegram inline keyboard 选择覆盖、两者并存或取消。

## Next Steps
- 继续追查 BBDown 下载 stall/合并阶段问题，基于现有失败摘要和进度日志做可复现 debug。
- 完善 Bilibili 专栏/opus PDF archive 行为。
- 如果 YouTube 下载遇到 yt-dlp JS runtime warning 变成实际失败，安装 deno 或 node 并在 yt-dlp 配置里启用。

## Evidence
- 本机已确认存在 `BBDown`、`yt-dlp`、Chrome、Rust/Cargo、clippy、rustfmt、uv、pnpm、ffmpeg。
- 计划来源：用户在 2026-05-16 要求实现 Telegram Local Downloader Bot，并随后要求先按 project-journal 记录设计和计划并提交。
- Journal checkpoint commit: `cbecd1b Document downloader bot plan`.
- 自动化验证通过：`cargo fmt --check`、`cargo clippy --all-targets -- -D warnings`、`cargo test`、`uv run ruff format --check`、`uv run ruff check`、`uv run python -m unittest discover -s tests`。
- 未执行 live Telegram 验收，因为仓库不提交真实 `config.toml` 和 bot token。
- Review gate: helper-backed `codex-readonly` found and fixes were applied for chat allowlist, child process cleanup, PDF output path races, Telegram HTTP timeouts, and bot token leakage in reqwest error logs. Final `codex-readonly` rerun returned `LGTM`.
- 第二轮新增单元覆盖：全文 URL 提取、PDF 白名单、YouTube 字幕选择、metadata 下载命令、Bilibili `--skip-ai` 和 NFO 渲染。
- 第二轮 review gate found a video output race in Bilibili directory-diff NFO generation; all video-output writes are now serialized inside the bot process while PDF work can still run concurrently.
- Follow-up review found URL pollution when CJK punctuation directly follows a URL without whitespace; URL scanning now treats CJK punctuation as a boundary and has a regression test.
- Final review follow-up fixed two edge cases: YouTube NFO generation now skips directory fallback paths, and URL scanning no longer treats ASCII parentheses inside URLs as hard boundaries.
- Additional review follow-up avoids Bilibili directory scans when NFO generation is disabled and handles fullwidth wrapped URLs without whitespace.
- URL scanner now keeps balanced ASCII parentheses inside URLs while stopping at unmatched ASCII closing wrappers.
- URL scheme scanning is now ASCII case-insensitive, matching `HTTP://` and `HTTPS://` variants before normalizing through `url::Url`.
- Quoted URLs followed immediately by captions now stop at ASCII or smart quote boundaries.
- NFO generation is now best-effort: scan/write failures are reported as job details but do not fail an otherwise successful video download.
- URL cleanup no longer strips balanced ASCII closing parentheses from legitimate URLs such as Wikipedia paths.
- 2026-05-24 BBDown root-cause pass: `https://b23.tv/mlTVYet` succeeded in a TTY direct run but stalled in non-TTY background mode with the default AVC stream; adding `--video-ascending` selected the smaller 480P HEVC stream and completed in both direct and replay tests.
- Replay validation passed: `cargo run -- --replay-message .codex-tmp/replay-config.toml https://b23.tv/mlTVYet` completed, emitted file-growth progress, wrote a 7.7 MiB MP4 and same-basename NFO under `.codex-tmp/replay-video`.
- Local environment repair: `uv tool install --force yt-dlp` fixed a broken `yt-dlp` shebang; `yt-dlp --dump-json --skip-download --no-playlist` succeeded for the prior YouTube sample URL, with a remaining JS runtime warning.
- 2026-05-31 Bilibili opus follow-up: route tests cover `m.bilibili.com/opus/<id>` and `www.bilibili.com/opus/<id>` canonicalization, malformed opus URLs remain unsupported, and PDF helper tests cover Bilibili snapshot routing plus cleanup of partial PDFs after print failures.
- Bilibili opus replay validation passed with the user-provided sample URL: `cargo run -- --replay-message .codex-tmp/opus-replay-config.toml 'Bilibili 文章可以看 https://m.bilibili.com/opus/1206098216310800386?...'` wrote a single 4-page PDF under `.codex-tmp/opus-pdf-final`.
- 2026-06-03 BBDown auth follow-up: planned from updated `master`, using direct Bilibili Web QR API instead of `BBDown login` because local BBDown 1.6.3 currently fails before QR generation with `System.Net.CookieContainer`.
- BBDown auth unit coverage includes command routing, chat type detection, QR PNG rendering, cookie extraction, auth state save/load/delete, private file permissions, and BBDown `--cookie` command construction.
- Internal review found and fixed two BBDown auth-path issues: Bilibili can return successful QR-login cookies in poll response `data.url` instead of `Set-Cookie`, and Bilibili auth HTTP calls now have bounded request/connect timeouts.
- Follow-up review found and fixed a login/logout race: `/bbdown logout` now invalidates pending login flows before they can write a later successful QR-login cookie back to disk.
- Final helper-backed `codex-readonly` rerun returned `LGTM` after the auth fixes.
- PR review follow-up fixed two auth hardening issues: BBDown now receives cookies through a protected `--config-file` instead of argv, and login polling sleep is bounded by the configured timeout deadline.
- PR review rerun corrected the BBDown config-file format to BBDown's line-oriented argument syntax and redacts Bilibili QR login URLs from outbound-message logs.
- 2026-06-04 duplicate-video follow-up: added pre-download duplicate prompts for direct YouTube and Bilibili IDs, Telegram callback query handling with per-prompt nonce tokens, default keep-both staging for all video downloads, overwrite backup/rollback semantics, and staging-directory exclusion from duplicate scans.
- Duplicate-video offline review found that Bilibili `--audio-only` downloads could be misclassified as failed after staging; staging now accepts audio primary media for that effective BBDown mode and has regression coverage for audio-only keep-both moves.
- Duplicate-video offline review also found that YouTube IDs were being duplicate-matched case-insensitively; duplicate detection now keeps media IDs case-sensitive while matching provider markers case-insensitively, with a YouTube case regression test.
- Duplicate-video offline review found audio-only duplicates were still missed during pre-download scans; duplicate detection now scans audio primary files when Bilibili effective args include `--audio-only`, with `.m4a` duplicate coverage.
- Duplicate-video offline review found overwrite could remove extra duplicate hits not replaced by staged media; overwrite now only backs up existing paths mapped to staged primary media, with coverage that unmapped duplicates remain.
- Duplicate-video offline review found staged Bilibili could still pass a relative explicit `--config-file` when `downloads.video_dir` itself is relative; staging now writes an absolute config path and has regression coverage for that shape.
