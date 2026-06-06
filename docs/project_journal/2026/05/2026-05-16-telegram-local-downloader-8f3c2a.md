---
id: 20260516-8f3c2a
title: Telegram Local Downloader Bot
status: completed
created: 2026-05-16
updated: 2026-06-06
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
- BBDown 下载期间的进度消息现在默认每 5 秒刷新一次，即使命令没有继续输出，也会显示阶段、已完成/待完成阶段、文件写入速度、持续时间和最近文件变化。
- 新增 `--replay-message` 本地入口，可用真实消息文本重放路由和下载组件，不依赖 Telegram ingress。
- YouTube 下载会预取 yt-dlp metadata，优先人工字幕、fallback 自动字幕，并启用 metadata、封面、字幕、info JSON、description 和 NFO 输出。
- Bilibili 下载继续由 BBDown 负责，显式跳过 AI 字幕，默认使用 `--video-ascending --skip-mux`，并在没有显式多线程设置时由下载命令追加 `--multi-thread false`，以避开当前复现链接在后台模式下的高码率流和多线程分片卡住问题，并对新增视频生成 best-effort NFO。
- Bilibili 视频默认通过 BBDown 保留 XML/ASS 弹幕 sidecar；这些 sidecar 会跟随 staging、覆盖和两者并存流程移动。
- PDF 支持 `mp.weixin.qq.com` 自动白名单，`/pdf URL` 继续保留。
- Bilibili `opus` 文章链接现在会规范化为 `https://www.bilibili.com/opus/<id>` 并走 PDF；PDF helper 对这类页面使用静态 HTML 快照渲染，避开页面脚本在 headless Chrome 中主动关闭页面的问题。
- BBDown 登录态现在由 bot 通过 Bilibili Web QR API 管理：私聊 `/bbdown login/status/logout` 可扫码登录、查看账号、清理本机状态；Bilibili 下载会自动把 bot-managed cookie 注入 BBDown。
- 视频下载现在默认先进入隐藏 staging 目录，成功后再移动到最终目录；对可直接提取媒体 ID 的 YouTube/Bilibili URL，若本地已有匹配视频或 sidecar，用户可通过 Telegram inline keyboard 选择覆盖、两者并存或取消。

## Next Steps
- 调研 Bilibili 弹幕预渲染：以 ASS 弹幕为中间格式，评估生成播放器可加载的 PGO/PGS 等图形字幕 sidecar。
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
- Duplicate-video offline review found staging early-return paths could leave job directories behind; staged video downloads now use a cleanup guard that removes the job staging directory on any exit path.
- Independent PR review found overwrite sidecar backup could remove sidecars belonging to sibling primary media such as `Show.part2.mkv`; artifact collection now excludes sidecars that match another primary media stem and has regression coverage for `part2.nfo`.
- Follow-up PR reviews found staged sidecar mapping had the same dot-prefix ambiguity for outputs like `Movie.mkv` plus `Movie.part2.mkv`; staged sidecars now choose the longest matching primary stem and have regression coverage for this keep-both case.
- Follow-up offline review found the Telegram polling loop still awaited duplicate scans; URL jobs now dispatch duplicate checks and subsequent prompt/queue handling in background tasks so callbacks and other updates are not blocked by filesystem scans.
- Follow-up independent review found duplicate scans could fan out without bounds after being dispatched; duplicate scans now use a bounded semaphore sized to `bot.concurrency` while downloads keep their existing semaphore.
- Follow-up offline review found duplicate detection treated `.description` free text as identity metadata; duplicate matching now uses structured `.nfo` unique IDs and `.info.json` identity fields only, with coverage for info JSON matches and ignored description references.
- Follow-up reviews found two duplicate-prompt/sidecar edge cases: pending duplicate choices are now registered before sending the inline keyboard, and overwrite sidecar ownership now uses longest primary stem matching so `Movie.part2.nfo` is replaced with `Movie.part2.mkv` instead of being excluded by `Movie.mkv`.
- Final triple-review follow-up found duplicate matching could still treat bare filename suffixes and `.info.json` free text as identities, and pending prompt capping could evict a just-sent token on timestamp ties; duplicate matching now requires explicit filename ID markers or typed sidecar parsing, and pending caps preserve the newly issued token.
- Final independent review found recursive staging directories with same-stem videos could cross-attach sidecars; staged sidecar matching now requires the sidecar and primary media to share the same staging parent directory, with regression coverage for `a/Movie.mkv` and `b/Movie.mkv`.
- 2026-06-04 Bilibili opus archive polish: snapshot rendering now injects Bilibili opus print CSS to hide navigation, TOC, share/feedback controls, and page backgrounds while preserving author, title, content, images, and copyright information. Real sample validation wrote a 1.7 MiB PDF with 5 page markers under `.codex-tmp/opus-archive-check`.
- 2026-06-05 BBDown stall triage: replaying the user-provided title plus `BV12TRrBcEP8` URL with the prior default args stalled inside BBDown after `开始下载P1视频...`; staging files stopped at 12.4 MiB and ffmpeg never started. Adding `--multi-thread false` to the same replay completed BBDown video and audio downloads, ffmpeg mux, staging move, and MP4 output under `.codex-tmp/bbdown-debug/video`.
- 2026-06-05 BBDown single-thread PR follow-up: explicit `--config-file` paths now fail early when missing while the implicit download-directory `BBDown.config` remains optional, preventing a typo from silently dropping user BBDown defaults.
- 2026-06-05 Independent review follow-up: Bilibili effective-arg boolean detection now respects explicit `false` values from space, equals, or colon forms so bot post-processing stays aligned with BBDown config semantics.
- 2026-06-05 Independent PR review follow-up: no-auth Bilibili downloads now pass the download-directory `BBDown.config` with `--config-file` when that implicit config is used for effective-arg detection, keeping the bot's single-thread fallback decision aligned with the actual BBDown command.
- 2026-06-05 BBDown mirror check: local BBDown already supports `--download-danmaku`, `--download-danmaku-formats`, `--danmaku-only`, XML download from `comment.bilibili.com/{cid}.xml`, and ASS writing via `DanmakuUtil.SaveAsAssAsync`; future work can preserve XML/ASS sidecars first, then evaluate pre-rendered graphics subtitle sidecars for target players.
- 2026-06-05 Bilibili danmaku sidecar follow-up: added `[bilibili.danmaku]` config, default BBDown `--download-danmaku` argument, XML sidecar ownership for overwrite, and command/config coverage for enabled and disabled danmaku modes.
- Danmaku replay validation passed: `cargo run -- --replay-message .codex-tmp/danmaku-replay/config.toml https://b23.tv/mlTVYet` completed and produced same-basename `.mp4`, `.nfo`, `.xml`, and `.ass` files under `.codex-tmp/danmaku-replay/video`; local BBDown 1.6.3 does not expose `--download-danmaku-formats`, so the bot relies on BBDown's default XML/ASS output for now.
- Offline review found a possible mux-sidecar mismatch if BBDown places danmaku next to raw stream files; post-mux cleanup now moves raw-stream `.xml/.ass` sidecars to the final MP4 basename when needed.
- Follow-up review found stale sidecar collisions could keep an old `Title.xml` beside a new `Title.mp4`; Bilibili post-mux cleanup now removes or replaces stale root `.xml/.ass` sidecars while keeping current same-run root sidecars on the final basename.
- Follow-up review also found staged keep-both could choose a primary media basename already occupied by a stale sidecar; staged primary destination selection now avoids existing same-stem sidecars before assigning sidecar destinations.
- Final review found two edge cases: bot-managed cookie downloads could let a temporary BBDown config override the danmaku CLI flag, and staged keep-both only checked simple sidecar extensions; danmaku args now come after the managed config path, and staged primary selection detects compound sidecars such as `.info.json`.
- Danmaku E2E replay validation passed on 2026-06-05 with the prior title-prefixed Bilibili sample URL `BV12TRrBcEP8`: replay stripped non-URL text, downloaded through BBDown, muxed with ffmpeg, and produced same-basename `.mp4`, `.nfo`, `.xml`, and `.ass` files under `.codex-tmp/danmaku-e2e/video`.
- GitHub Codex review found root/custom BBDown danmaku sidecars could be left behind when mux output names diverged; post-mux cleanup now searches current-download `.xml/.ass` candidates and the second E2E replay produced same-basename `(2).mp4`, `(2).nfo`, `(2).xml`, and `(2).ass`.
- Final danmaku-sidecar review follow-up covered no-replacement, stale raw-stream, multi-part raw-stream, and disabled-config edge cases: cleanup now preserves existing root sidecars when no current replacement exists, ignores old raw sidecars, prefers the current raw stream sidecar among sibling candidates, and appends `--download-danmaku false` when the bot disables danmaku.
- Offline frozen review found current root sidecars could cause cleanup to delete sibling raw sidecars before later mux outputs used them; current-root cleanup now only removes the direct raw duplicate for the source video being muxed.
- A later offline frozen review found fallback selection could reassign a current root sidecar already bound to another same-run mux output; sidecar candidates now exclude non-direct sidecars that already have same-stem current primary media.
- PR9 current-head replay validation passed on 2026-06-05 with the title-prefixed `BV12TRrBcEP8` sample: the first replay produced `.mp4/.nfo/.xml/.ass`, and the second replay produced keep-both `(2).mp4/.nfo/.xml/.ass` under `.codex-tmp/danmaku-e2e-current/video` with no staging or `.json` residue.
- GitHub Codex review found newer BBDown defaults may emit JSON danmaku sidecars; local BBDown rejected `--download-danmaku-formats`, so the bot keeps XML/ASS compatibility and now removes same-run `.json` danmaku sidecars during post-mux cleanup.
- 2026-06-06 progress display follow-up: `bot.progress_update_seconds` default changed to 5 seconds; file activity polling now emits fixed-interval snapshots while only real stdout/stderr or file changes refresh idle timeout. Validation passed: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test`, `uv run ruff format --check`, `uv run ruff check`, and `uv run python -m unittest discover -s tests`.
