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
- Independent attachment-profile exact-binary filters: `round6` 26/26 (`attachment-round6-current-1794837417640563000.log`); captured-NFO custom XML refresh E2E 1/1 (`attachment-crossfeature-custom-nfo-e2e-1794837158259235000.log`); Bilibili NFO 1/1 (`attachment-bilibili-nfo-current-1794837455920216000.log`); YouTube NFO 1/1 (`attachment-youtube-nfo-current-1794837457795004000.log`); mock collection publication 1/1 (`attachment-collection-publish-current-1794837459532256000.log`); stable migration 1/1 (`attachment-migration-stable-current-1794837461821775000.log`); KeepBoth stale-reference skip 1/1 (`attachment-migration-keepboth-current-1794837464234273000.log`); NFO checkpoint restart recovery 1/1 (`attachment-nfo-checkpoint-current-1794837466229544000.log`). Total: 33 passed, 0 failed across the eight filters; binary `telegram_video_downloader-c427f64a88151bc6`.
- Independent attachment-profile strict workspace/all-target Clippy passed with `-D warnings` (`attachment-strict-clippy-final-1794837476058712000.log`); `cargo fmt --all -- --check` passed (`attachment-rustfmt-check-1794837583812453000.log`).
- `git diff --check` passed after the final documentation edits.
