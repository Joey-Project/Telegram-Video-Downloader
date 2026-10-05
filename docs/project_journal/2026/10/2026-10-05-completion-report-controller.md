---
id: 20261005-completion-report-controller
title: Codex Review Gate v2.1.7 Completion Report Controller
status: completed
created: 2026-10-05
updated: 2026-10-05
branch:
pr:
supersedes: []
superseded_by:
---

# Codex Review Gate v2.1.7 Completion Report Controller

## Summary
- The target-branch controller matches the canonical v2.1.7 completion-reporting workflow while preserving the existing opt-in automatic review-request path.

## Current State
- Completion admission accepts only completed runs for the canonical verifier workflow path (bare or `@ref`-qualified), the `pull_request` event, and at most one associated pull request. It is independent of automatic-review settings, attempt number, and run conclusion.
- When a run has no PR association, the controller passes the `0` sentinel for the action's anchored canonical run-name fallback, which independently verifies the PR, run, attempt, check, and current scope.
- `report-completion` is the workflow-run operation. Automatic `begin-review` and `request_review` remain limited to the existing enabled, first-attempt failure with one associated PR.
- Completion reporting updates diagnostic state only; it does not request a review, rerun a verifier, reconcile provider findings, or write a required check or status.
- This controller update leaves the verifier, `.github/CODEOWNERS`, event declarations, permissions, concurrency, runner selection, repository and organization variables, and rulesets outside its change scope.

## Next Steps
- None within this controller-sync workstream.

## Evidence
- Canonical controller template: v2.1.7 source checkout at `7e1069c6a6f4c4b319b1c5f33da262ee97460242`.
- Consumer base: `8d8bab440cce72ff873ec9b68933fbdc64ac92cc`.
- The controller matches the canonical template byte-for-byte; the verifier and controller workflows pass `actionlint` 1.7.12.
- The canonical source `test/v2-workflow-contract.test.mjs` passes under Node v24.15.0.
