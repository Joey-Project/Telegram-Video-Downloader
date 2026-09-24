---
id: 20260924-ugc02
title: Bilibili UGC Membership Probe Authentication
status: completed
created: 2026-09-24
updated: 2026-09-24
branch: wip/bilibili-ugc-membership-auth
pr:
supersedes: []
superseded_by:
---

# Bilibili UGC Membership Probe Authentication

## Summary
- Fix UGC collection membership discovery to migrate compatible legacy auth state and load the saved BBDown credential instead of issuing an anonymous Bilibili view request.
- Refuse to start a normal-video download when collection membership cannot be verified, rather than silently treating a probe failure as no collection.

## Current State
- A deployed Telegram probe for `BV1kk4y1T7cd` showed anonymous `/x/web-interface/view` returning HTTP 412 while the later credential-aware download plan succeeded. The old fallback incorrectly began a single-video download.
- The revised probe first runs the existing locked legacy-state migration, then uses the credential-aware `bbdown-core` client. Credential migration is single-flight, so concurrent inbound Bilibili links wait asynchronously instead of exhausting Tokio blocking workers while an auth lock is held. Local loopback tests cover both a saved credential file and a legacy state file, and verify that the membership request includes its cookie.
- Any membership probe error now sends a retry message and does not enqueue a download. The interrupted pre-fix job was stopped and its retained staging directory was discarded by startup recovery.

## Next Steps
- Deploy the fix and repeat the positive BV Telegram test. It must show the current-video / entire-collection / cancel keyboard before any download starts.

## Evidence
- Production observation: Bilibili returned `412 Precondition Failed` only for the anonymous membership probe; the credential-aware plan for the same URL resolved and started media download.
- Regression tests cover saved-credential forwarding, legacy-state migration before the local mock view request, asynchronous credential-migration queueing, and probe-error refusal of automatic single-video download.
