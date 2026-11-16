---
id: 20261116-pat01
title: Portable attachment manifests and NFO interoperability
status: completed
created: 2026-11-16
updated: 2026-11-16
branch: "codex/portable-attachments"
pr: ""
supersedes: []
superseded_by: ""
---

# Portable attachment manifests and NFO interoperability

## Summary

- Added bounded, local NFO attachment declarations, conservative legacy sidecar association, download NFO output, and durable organize/migration reference handling.

## Current State

- NFOs use repeated `<tvd_attachment role="cover|subtitle|danmaku">basename</tvd_attachment>` elements. Declarations accept only safe same-directory regular files with role-compatible extensions; traversal, URI, symlink, primary media, NFO, queue protocol, multi-owner and over-limit claims remain unattached or unsupported.
- Legacy bare covers, language-tagged subtitles and danmaku attach only in a complete single-primary leaf directory. Scans remain read-only. Confirmed organize and rename operations update local references while preserving unknown NFO fields and clear source IDs; migration keeps stable references and safely skips KeepBoth renames that would stale them.
- New Bilibili and YouTube NFOs include local attachment references when NFO output is enabled and a new NFO can be created. Local `thumb`, `localfilename` and missing `tvshowtitle` support library interoperability; remote thumbnails are not fetched.
- README documents the schema and safety/compatibility rules. The completed attachment-interoperability entry was removed from `docs/PROJECT_TODO.md`.

## Next Steps

- None within this feature slice; implementation and isolated local validation are complete.

## Evidence

- Independent attachment-profile `round6` filter: 26 passed, 0 failed; log `attachments-isolated-tests-requeued-1794832060969678000.log`.
- Independent exact-binary filters: Bilibili manifest 1/1 (`attachments-isolated-bilibili-nfo-1794832670724271000.log`); YouTube NFO 1/1 (`attachments-isolated-youtube-nfo-1794832675551254000.log`); mock collection publication 1/1 (`attachments-isolated-collection-e2e-1794832682763685000.log`); stable migration references 1/1 (`attachments-isolated-migration-stable-1794832688039793000.log`); KeepBoth stale-reference skip 1/1 (`attachments-isolated-migration-keepboth-1794832693665116000.log`); NFO checkpoint restart recovery 1/1 (`attachments-isolated-nfo-recovery-1794832698912215000.log`).
- Independent attachment-profile strict workspace/all-target Clippy passed with `-D warnings`; log `attachments-isolated-clippy-1794832713446265000.log`.
- `git diff --check` passed after the final documentation edits.
