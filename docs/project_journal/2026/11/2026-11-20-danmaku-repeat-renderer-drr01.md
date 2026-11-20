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
- Place each sized event using its vertical extent and estimated on-screen overlap area over time. A four-line comment in a real sample previously constrained the whole video to one scrolling lane. Event-local placement reserves space for tall comments only while they can overlap other comments and retains event timing when the screen is crowded.
- Two real libass A/B tests sampled 21 frames each with the same defaults and input XML. Mean overlapping character-fill pixels decreased by about 47% in a repeated-text burst and 21% in a crowded mixed-text sample. All tested frames stayed within the top half, with identical ASS events apart from layout coordinates.
- Refine initial placements using nearby earlier and later events. Score their projected on-screen overlap over each pair's shared visible interval while preserving the original event durations. Keep the initial placement for an event when more than 16 neighboring paths intersect it, where the rectangle estimate is less reliable.
- Compare the refinement against the one-pass layout in three real libass windows of 21 frames each. Keep the original margins after smaller margins increased overlap in both crowded windows. With the selected refinement, the crowded windows retained their mean and 95th-percentile overlap, and the ordinary-density window decreased from 290 to 113 mean overlapping character-fill pixels and from 886 to 339 at the 95th percentile.
- New generated sidecars use layout v3. Keep the exact v1 and v2 reproduction paths available to refresh generated files and to upgrade complete older download sidecars with the same input and profile. Full-output comparison protects custom or edited ASS.
