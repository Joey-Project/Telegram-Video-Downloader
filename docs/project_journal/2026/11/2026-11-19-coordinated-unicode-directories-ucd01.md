---
id: 20261119-ucd01
title: Accept coordinated directory aliases with bound identity checks
status: completed
created: 2026-11-19
updated: 2026-11-19
branch: "codex/fix-coordinated-unicode-directories"
pr: "https://github.com/Joey-Project/Telegram-Video-Downloader/pull/85"
supersedes: []
superseded_by: ""
---

# Accept coordinated directory aliases with bound identity checks

## Behavior

- macOS File Provider can return a decomposed Unicode spelling for a precomposed directory name, such as Japanese `ヴ` versus `ウ` plus U+3099. Library scans and organization accept that callback only after proving that both paths refer to the same current, root-constrained directory object and access policy.
- Keep registered paths and registration nonces as persistent library identity. Device/inode/type and owner/group/mode are temporary descriptor-binding evidence, not persistent media-library identity.
- Preserve no-follow bindings, registered-root containment and subsequent file identity/content guards. Different directory objects, symlinks, replacements, access-policy changes and failed revalidation remain rejected.
- A file callback may use a verified parent-directory alias only when its leaf name remains byte-identical. Read, write and move callbacks retain the requested namespace spelling after validation.
- Ignore directory mtime, ctime and link count as standalone mutation evidence, allowing ordinary child-entry churn.
- Keep directory read failures in the existing incomplete-snapshot/warning flow. Redirected directory contents are not attributed to the requested directory; a failed scan does not manufacture missing rows or authorize a move.

## Implementation

- Open and validate rooted directory descriptors through existing RootedFs bindings, compare object and ordinary access-policy proofs, and use no-follow directory opens for filesystem name semantics.
- Share callback-path validation across root setup, rooted reads/writes and moves instead of comparing equivalent callback path bytes directly.
- Keep the fix independent of user media paths, private migration adapters, lookup/network behavior and deployment state.

## Validation

- Six focused helper regressions pass, covering canonical-equivalent Japanese paths, benign child-entry churn, different directory objects, out-of-root callbacks, symlink/unreadable paths, and object replacement or access-policy changes.
- The permission-denial assertion applies only to non-root test users, since root can bypass ordinary directory permissions. The fixture restores its access mode before asserting; symlink rejection is always checked.
- Two macOS temp-root provider regressions cover scan through preview and execution, unchanged synthetic media bytes/inode, NFO/cover references, preserved unrelated XML, and rejection of a different-directory callback before a media move.
- The offline workspace/all-targets suite passed with two test threads: 1,774 bot tests passed and 17 were ignored, plus 27 and 8 converter-related tests passed. The final additional out-of-root assertion passed in the six-test helper rerun; production logic did not change afterward.
- Workspace/all-targets Clippy passes with warnings denied. Workspace formatting and diff whitespace checks pass.
- Existing macOS Rust CI runs these regressions through its workspace/all-targets test command; no workflow or new dependency is required.
