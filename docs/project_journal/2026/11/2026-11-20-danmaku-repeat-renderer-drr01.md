---
id: 20261120-drr01
title: Group nearby scrolling danmaku in a reusable ASS renderer
status: completed
created: 2026-11-20
updated: 2026-11-20
branch: "codex/danmaku-repeat-renderer"
pr: ""
supersedes: []
superseded_by: ""
---

# Group nearby scrolling danmaku in a reusable ASS renderer

- Keep the Rust library and CLI in a workspace crate that depends only on a bounded XML parser and general error handling. Consumers can use the library directly or invoke the CLI.
- Sort accepted comments by video timestamp, group identical scrolling text in the same direction within a 1500 ms window anchored to the first comment, then size and place the resulting events. The total count appears as a smaller suffix. Body growth is gradual and capped.
- Use the same renderer for new downloads with verified companion XML and for explicit danmaku refresh. Retain the legacy rendering path when grouping is disabled or new downloads have only ASS.
- Keep raw XML as the source of truth. Refresh verifies a versioned grouped ASS by reproducing it from the relevant document-order prefix before sorting and grouping. This protects custom ASS and permits safe recovery after XML publication succeeds before ASS publication.
- The renderer and CLI have independent tests. Bot coverage exercises download pairing, the legacy fallback, and the mocked refresh workflow across cancellation, repeated confirmation, and restart. A libass render of a synthetic sample compares the grouped and ungrouped output.
