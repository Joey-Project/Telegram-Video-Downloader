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
- Equivalent filename/role declarations across NFO sources of one media owner form one logical claim; every source NFO remains attached for reference updates. Role conflicts and cross-owner claims remain ambiguous. Declaration reads share the scan deadline, and timeout reconciliation preserves prior rows as unavailable within incomplete scopes until a complete rescan verifies them.
- Conflicts revoke tentative stem/legacy ownership before valid declarations are applied, including disagreement between a sole explicit claimant and an inferred owner. A contested path cannot be reclaimed by hash-map iteration order.
- KeepBoth preserves ordinary custom attachment spelling. Near-limit names reserve space for the extension, serial suffix and stable basename discriminator, then truncate at a UTF-8 boundary. Distinct trimmed tails retain distinct targets, and confirmed moves update NFO declarations and thumbnail references to those targets.
- Legacy bare covers, language-tagged subtitles and danmaku attach only in a complete single-primary leaf directory. Scans remain read-only. Confirmed organize and rename operations update local references while preserving unknown NFO fields and unambiguous source IDs; migration keeps stable references and safely skips KeepBoth renames that would stale them.
- New Bilibili and YouTube NFOs include local attachment references when NFO output is enabled and a new NFO can be created. Local `thumb`, `localfilename` and missing `tvshowtitle` support library interoperability; remote thumbnails are not fetched.
- Bilibili collection publication generates NFOs after flattening and rewrites existing SDK NFOs with each video's own attachment mapping. Final KeepBoth publication uses private rewritten copies, leaving source metadata unchanged; a saved publication manifest retains ownership of its exact rewritten copy during recovery.
- YouTube uses the same final NFO publication mapping, including conventional unqualified sidecars only when their staged parent has one primary. Collision-resolved media, cover and subtitle basenames are reflected in the NFO without modifying the retry source.
- Confirmed moves update every bounded NFO declaration source using the source filesystem's filename comparison rules. Generated NFO destinations participate in conflict planning before media moves. Metadata previews and writes share the same source-ID acceptance rules: empty placeholders can be filled while known nonempty IDs remain protected.
- Declared custom artwork and subtitle basenames survive stable library migration. The private library directory does not suppress otherwise valid legacy leaf association, and only recognized bare TXT danmaku names receive a persistable attachment role.
- KeepBoth migration inspects all bounded attached NFO sources before permitting a rename; any reference set that would become invalid skips the whole group. Danmaku declaration resolution uses the actual scanned path with rooted filesystem-name semantics. Existing custom XML/ASS without captured authoritative declaration proof is skipped without fetching or creating a separate canonical output.
- Migration relying on undeclared legacy ownership verifies the projected target layout remains a single-primary leaf, including existing and incoming contents. Verified NFO-declared ownership remains valid in shared directories. Atomic replacement uses bounded target-hash temporary basenames; recovery accepts both strict current and legacy checkpoint names while retaining parent, identity, digest and access-policy validation.
- Compact move previews permit bounded automatic NFO updates while metadata-only previews retain exact requested counts. Completed NFO replacements update the matching moved-file identity; recovery binds the durable replacement checkpoint, target path, content digest and access policy before accepting its new identity.
- Danmaku refresh supports existing custom-named sidecars only through captured NFO declarations and unique catalog ownership. Newly generated canonical sidecars keep the current-directory ownership checks.
- README documents the schema and safety/compatibility rules. The completed attachment-interoperability entry was removed from `docs/PROJECT_TODO.md`.

## Next Steps

- None within this feature slice; implementation and isolated local validation are complete.

## Evidence

- The integrated workspace at `588042a8682537b066861b940d4b87bac3238a03` passed `cargo test --locked --workspace --all-targets -- --test-threads=2`: bot 1,706 passed with 17 opt-in tests ignored; converter 27 unit and 8 native E2E cases passed (`attachment-pr78-final-workspace-readiness-1794861966527595000.log`). Tests use mocks, loopback services and temporary media; they perform no production organization or Music imports.
- Seven exact regressions cover projected legacy migration layouts, explicit ownership in shared destinations, maximum-length ASCII/CJK NFO names, replacement checkpoint recovery and inferred/declared ownership conflicts. The complete workspace also covers Unicode/case aliases, multiple declaration sources, generated-NFO conflicts, empty source-ID placeholders, final KeepBoth publication, custom-NFO danmaku refresh and manifest-bound recovery.
- The torrent Mini App fixture waits for asynchronous inventory readiness before asserting revision-bound preview/confirmation and replay behavior. The 7-case module and three consecutive runs of the formerly intermittent case passed before the full workspace run (`attachment-pr78-torrent-module-filter-1794861915463582000.log`, `attachment-pr78-torrent-review-replay-3-1794861952680070000.log`).
- The subsequent custom-name KeepBoth repair passed both `review3f9_` regressions and all 193 `file_manager` tests from one fresh binary (SHA-256 `a4410f3d0b774376480e6acc8c81ab7fe90c738aa760be8aa54fc60f67e5ac2f`). They exercise exact-limit ASCII/CJK/emoji basenames, a media-only conflict and two custom names differing only in their trimmed tails, through preview, confirmation, NFO updates and rescan ownership (`pr78-review3f9-tests-1794869475594083000.log`, `pr78-file-manager-review3f9-1794869564277223000.log`).
- Strict workspace/all-target Clippy passed with `-D warnings` after that repair (`pr78-attachment-strict-clippy-1794869613553549000.log`), along with `cargo fmt --all -- --check` (`pr78-attachment-fmt-1794869650868097000.log`) and project-journal validation.
- `git diff --check` passed after the final documentation edits.
