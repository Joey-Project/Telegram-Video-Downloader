---
id: 20261123-pdf01
title: Reply with the saved PDF
status: completed
created: 2026-11-23
updated: 2026-11-23
branch: codex/pdf-reply-on-complete
pr: ""
supersedes: []
superseded_by: ""
---

# Reply with the saved PDF

- In the configured private owner chat, the bot replies to new requests with the generated PDF after verifying the published output and recording completion. This covers `/pdf` and links routed to PDF automatically.
- The attachment uses the existing MTProto sender and the original filename. The upload uses a private temporary copy checked against the saved output digest.
- An attachment error appears in the completion message. The verified PDF remains available in the download directory, with a completed task record.

## Verification

- Mock Telegram exercises confirmed and unconfirmed attachment delivery. Both outcomes retain the saved PDF and completed record. The test checks the original message ID, exact digest, and private-copy cleanup.
- Fake Telethon verifies that PDF replies use the PDF document MIME type.
