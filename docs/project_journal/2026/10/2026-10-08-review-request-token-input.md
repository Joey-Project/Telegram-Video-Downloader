---
id: 20261008-rrt-tvd
title: Review Request Token Input
status: completed
created: 2026-10-08
updated: 2026-10-08
branch: codex/review-request-token-rollout
pr:
supersedes: []
superseded_by:
---

# Review Request Token Input

## Summary
- Pass the repository-scoped `CODEX_REVIEW_GATE_REQUEST_TOKEN` secret to the controller action through `review_request_token`.

## Current State
- The controller passes the secret as the single new input immediately after `github_token`.
- The verifier workflow and workflow triggers, permissions, runner, concurrency, and repository settings remain unchanged.

## Next Steps
- None within this workstream.

## Evidence
- The controller baseline matched canonical blob `c6290c800903303151cbfb34ca706463118b0d09`; verifier baseline blob `cdf232e84431dc1abf0c494339758ca848dca43f` is unchanged.
