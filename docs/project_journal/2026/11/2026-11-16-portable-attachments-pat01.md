---
id: 20261116-pat01
title: Portable attachment manifests and NFO interoperability
status: completed
created: 2026-11-16
updated: 2026-11-16
branch: "codex/portable-attachments"
pr: "https://github.com/Joey-Project/Telegram-Video-Downloader/pull/78"
supersedes: []
superseded_by: ""
---

# Portable attachment manifests and NFO interoperability

## Summary

- Added bounded, local NFO attachment declarations, conservative legacy sidecar association, download NFO output, and durable organize/migration reference handling.

## Current State

- NFOs use repeated `<tvd_attachment role="cover|subtitle|danmaku">basename</tvd_attachment>` elements. Declarations accept only safe same-directory regular files with role-compatible extensions; traversal, URI, symlink, primary media, NFO, queue protocol, multi-owner and over-limit claims remain unattached or unsupported.
- Legacy bare covers, language-tagged subtitles and danmaku attach only in a complete single-primary leaf directory. Scans remain read-only. Confirmed organize and rename operations update local references while preserving unknown NFO fields and unambiguous source IDs; migration keeps stable references and safely skips KeepBoth renames that would stale them.
- New Bilibili and YouTube NFOs include local attachment references when NFO output is enabled and a new NFO can be created. Local `thumb`, `localfilename` and missing `tvshowtitle` support library interoperability; remote thumbnails are not fetched.
- Compact move previews permit bounded automatic NFO updates while metadata-only previews retain exact requested counts. Completed NFO replacements update the matching moved-file identity; recovery binds the durable replacement checkpoint, target path, content digest and access policy before accepting its new identity.
- Danmaku refresh supports existing custom-named sidecars only through captured NFO declarations and unique catalog ownership. Newly generated canonical sidecars keep the current-directory ownership checks.
- README documents the schema and safety/compatibility rules. The completed attachment-interoperability entry was removed from `docs/PROJECT_TODO.md`.

## Next Steps

- None within this feature slice; implementation and isolated local validation are complete.

## Evidence

- Integrated regression filters pass 18 unique cases, including all eight full-CI failures, bounded compact receipts, the moved-NFO checkpoint crash window, replacement rejection, ownership/churn and custom-NFO danmaku refresh (`attachments-core-regressions-1794841864556322000.log`, `attachments-mock-e2es-1794841900972305000.log`, `attachments-custom-nfo-e2e-1794841954213397000.log`).
- The complete workspace on integrated implementation `cceec99b6c918eb1ac055dc5076740b60c5d03dc` passed `cargo test --locked --workspace --all-targets -- --test-threads=2`: bot 1,680 passed with 17 opt-in tests ignored; converter 27 unit and 8 native E2E cases passed (`attachments-full-workspace-1794842319520732000.log`). Tests use mocks, loopback services and temporary media; they perform no production organization or Music imports.
- Strict workspace/all-target Clippy passed on the integrated code with `-D warnings` (`attachments-integrated-final-clippy-1794842864344750000.log`); `cargo fmt --all -- --check`, diff checks and the project journal validator passed.
- `git diff --check` passed after the final documentation edits.
