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
- Legacy bare covers, language-tagged subtitles and danmaku attach only in a complete single-primary leaf directory. Scans remain read-only. Confirmed organize and rename operations update local references while preserving unknown NFO fields and unambiguous source IDs; migration keeps stable references and safely skips KeepBoth renames that would stale them.
- New Bilibili and YouTube NFOs include local attachment references when NFO output is enabled and a new NFO can be created. Local `thumb`, `localfilename` and missing `tvshowtitle` support library interoperability; remote thumbnails are not fetched.
- Bilibili collection publication generates NFOs after flattening and rewrites existing SDK NFOs with each video's own attachment mapping. Final KeepBoth publication uses private rewritten copies, leaving source metadata unchanged; a saved publication manifest retains ownership of its exact rewritten copy during recovery.
- YouTube uses the same final NFO publication mapping, including conventional unqualified sidecars only when their staged parent has one primary. Collision-resolved media, cover and subtitle basenames are reflected in the NFO without modifying the retry source.
- Confirmed moves update every bounded NFO declaration source using the source filesystem's filename comparison rules. Generated NFO destinations participate in conflict planning before media moves. Metadata previews and writes share the same source-ID acceptance rules: empty placeholders can be filled while known nonempty IDs remain protected.
- Declared custom artwork and subtitle basenames survive stable library migration. The private library directory does not suppress otherwise valid legacy leaf association, and only recognized bare TXT danmaku names receive a persistable attachment role.
- KeepBoth migration inspects all bounded attached NFO sources before permitting a rename; any reference set that would become invalid skips the whole group. Danmaku declaration resolution uses the actual scanned path with rooted filesystem-name semantics. Existing custom XML/ASS without captured authoritative declaration proof is skipped without fetching or creating a separate canonical output.
- Compact move previews permit bounded automatic NFO updates while metadata-only previews retain exact requested counts. Completed NFO replacements update the matching moved-file identity; recovery binds the durable replacement checkpoint, target path, content digest and access policy before accepting its new identity.
- Danmaku refresh supports existing custom-named sidecars only through captured NFO declarations and unique catalog ownership. Newly generated canonical sidecars keep the current-directory ownership checks.
- README documents the schema and safety/compatibility rules. The completed attachment-interoperability entry was removed from `docs/PROJECT_TODO.md`.

## Next Steps

- None within this feature slice; implementation and isolated local validation are complete.

## Evidence

- The complete integrated workspace on `d74a52ccd8dea52f3be1bae015512501759925c2` passed `cargo test --locked --workspace --all-targets -- --test-threads=2`: bot 1,691 passed with 17 opt-in tests ignored; converter 27 unit and 8 native E2E cases passed (`attachments-final-integrated-workspace-1794852108922262000.log`). Tests use mocks, loopback services and temporary media; they perform no production organization or Music imports.
- Targeted review regressions cover custom Unicode migration names, filesystem case/normalization aliases, multiple NFO declaration sources, generated-NFO conflicts, empty source-ID placeholders, recognized legacy TXT roles, SDK flattening, final KeepBoth references and manifest-bound recovery (`attachments-p2-final-exact-1794849627011246000.log`, `attachments-eight-findings-exact-1794850809534223000.log`). The complete integrated run includes those cases, compact receipts, ownership/replacement rejection, custom-NFO danmaku refresh and the synchronized settings lifecycle E2E.
- The ownership/deadline/YouTube publication implementation at `800a2cd` passed all six `review78_` regressions and the exact generated-YouTube-NFO KeepBoth E2E. The same fresh binary passed the affected filters: `file_manager` 187, collection/publication E2Es 10, `danmaku_refresh` 28 and `metadata_recovery_` 25 (`attachment-pr78-fresh-review-tests-1794854454483182000.log`, `attachment-pr78-youtube-keepboth-1794854674237346000.log`).
- The subsequent all-source migration guard and rooted danmaku-name resolution passed four exact regressions, then `file_manager` 187, `migration_fault_tests` 44, `danmaku_refresh` 30 and `metadata_recovery_` 25 from one fresh binary. Coverage includes secondary-NFO KeepBoth skip/stable migration, an actual case/NFC/NFD alias merge and conservative no-fetch/no-write handling for an unproven custom XML (`attachment-pr78-fresh-compile-no-run-1794856503833418000.log`, `attachment-pr78-exact-danmaku-alias-1794856728816258000.log`, `attachment-pr78-broad-migration-fault-tests-1794856814646229000.log`).
- Strict workspace/all-target Clippy passed after the final repairs with `-D warnings` (`attachment-pr78-strict-workspace-clippy-1794856891089095000.log`); `cargo fmt --all -- --check` passed (`attachment-pr78-rustfmt-check-1794856945870697000.log`), along with diff checks and the project journal validator.
- `git diff --check` passed after the final documentation edits.
