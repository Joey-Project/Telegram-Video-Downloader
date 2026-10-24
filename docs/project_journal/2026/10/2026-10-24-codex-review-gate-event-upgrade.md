---
id: 20261024-v2212-event
title: Codex Review Gate Event Upgrade
status: completed
created: 2026-10-24
updated: 2026-10-24
branch:
pr:
supersedes: []
superseded_by:
---

# Codex Review Gate Event Upgrade

## Summary
- Synchronize the default-branch consumer verifier/controller pair with the canonical v2.2.13 workflows.
- Canonical source: [codex-review-gate PR #146](https://github.com/Joey-Tools/codex-review-gate/pull/146), released as v2.2.13.

## Current State
- The verifier retains `opened`, `reopened`, `synchronize`, `ready_for_review`, `stacked`, and `edited`; every event uses the standard verifier title, per-PR concurrency group, and job name.
- The controller is byte-identical to the canonical source and continues to filter for verifier-titled workflow runs.
- The floating `@v2` action, configurable runner-label macro and fallback, review-request token mapping, auto-request setting, limits profile, and permission envelope remain unchanged.
- No production rulesets, repository settings, secrets, variables, or transport workflows were changed.

## Evidence
- Both workflow files match the canonical v2.2.13 source byte-for-byte and passed the exported source validators.
- `actionlint` 1.7.12 passed with only its exact unsupported `stacked` activity diagnostic ignored; project-journal validation and `git diff --check` passed.
- No live activation, production behavior, or full CI result is claimed.
