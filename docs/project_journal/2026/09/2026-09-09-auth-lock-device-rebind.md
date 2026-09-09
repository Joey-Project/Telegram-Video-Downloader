---
id: 20260909-auth-lock-device-rebind
title: BBDown Auth Lock Device Rebind
status: completed
created: 2026-09-09
updated: 2026-09-09
branch: wip/auth-lock-device-rebind
pr:
supersedes: []
superseded_by:
---

# BBDown Auth Lock Device Rebind

## Summary
- Repair BBDown authentication startup after a macOS APFS remount changes `st_dev` while the persisted lock inode and verified two-link lock pair remain stable.
- Repair the video output control record under the same device-only remount condition, so startup recovery can reach Telegram polling.
- Preserve fail-closed behavior for lock inode changes, replaced aliases, unsafe permissions, and ownership-record replacement.

## Current State
- A live LaunchAgent entered a restart loop because both its persisted auth-owner record and video-control record retained pre-remount device values while their verified inode bindings remained stable.
- The release now atomically rebinds only device-only record changes after validating the corresponding private object and root/control bindings. It was deployed against the affected local records; the bot returned to a stable `running` state and resumed a queued Bilibili job.

## Next Steps
- No follow-up is required for this incident.

## Evidence
- Regression coverage accepts a device-only mismatch for both BBDown auth and the video control directory, while separate tests reject mismatched auth-lock and video-control inodes.
- `cargo fmt --all --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test --all-targets --quiet` passed (`420 passed, 10 ignored`). The first full test attempt had one unrelated low-file-descriptor mux timeout; its focused rerun and the subsequent full run passed.
- `cargo build --release`, `uv run ruff format --check .`, `uv run ruff check .`, `uv run python -m unittest discover -s tests`, project journal validation, and `git diff --check` passed.
- The deployed LaunchAgent remained running past its minimum runtime with one main process and one Bilibili worker. Both owner records were rewritten with current device values while retaining their original inode bindings.
