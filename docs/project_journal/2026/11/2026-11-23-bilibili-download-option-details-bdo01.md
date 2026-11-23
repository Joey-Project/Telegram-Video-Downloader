---
id: 20261123-bdo01
title: Show part titles and collection part counts in Telegram
status: completed
created: 2026-11-23
updated: 2026-11-23
branch: "codex/bilibili-download-option-details"
pr: ""
supersedes: []
superseded_by: ""
---

# Show part titles and collection part counts in Telegram

- Show the selected part's title in the Telegram choice message, including a one-part video that belongs to a UGC collection.
- Label the entire-collection button with the number of parts found in the resolved collection. Use BBDown's bounded UGC resolver, which expands multi-part videos. When membership reports an item count, require the resolved videos to match it before displaying a number. An unavailable or timed-out count leaves the button usable.
- Count probes share the bot's metadata and credential I/O limits and reuse existing Bilibili sign-in state. Direct links with an uploader ID support counting. Legacy collection links without an uploader ID show the ordinary button immediately.

## Verification

- A mocked Bilibili collection contains one single-part video and one two-part video. The selection message shows the single part's title and the collection button reports three parts.
- Direct collection and member-video fixtures check legacy credential reuse. A mismatched item count leaves the collection button usable without a number. The page resolver fixture selects P2 and checks the corresponding part title.
- A mocked Telegram prompt sends the count in its inline keyboard and continues to reject callbacks from the superseded prompt generation.
