---
id: 20261025-obq01
title: Sample Worker telemetry within the free observability allowance
status: completed
created: 2026-10-25
updated: 2026-10-25
branch: "wip/observability-budget"
pr: ""
supersedes: []
superseded_by: ""
---

# Sample Worker telemetry within the free observability allowance

## Incident and evidence

The owner received daily 90% Observability allowance alerts on October 24 and 25.
The deployed Worker enabled invocation/custom logs and automatic traces at 100%.
Adaptive invocation analytics reported 33,680 downloader invocations on October 24
UTC and 26,823 on October 25 through 14:54:59 UTC, with zero execution errors in
both windows. Invocation counts are not a complete account of retained telemetry:
WebSocket callbacks, Durable Object calls, custom logs, and trace spans can add
records independently of the HTTP request count.

Stored telemetry aggregate queries returned HTTP 403 with the existing OAuth
permissions. The exact event mixture and account meter were therefore not
reconciled. Do not interpret the alert as a CPU failure or claim that D1 spans
alone caused it. Canonical Cloudflare documentation describes a December 1, 2026
transition from legacy log-event pricing to pooled byte-based Observability
usage; the mitigation reduces both streams without depending on that attribution.

## Sampling policy

- Keep Observability, invocation logs, and automatic traces enabled.
- Sample logs at `0.1` (10% of incoming request contexts), and traces at `0.01`
  (1%), instead of full capture. This reduces expected log admission by 90% and
  trace admission by 99% relative to the previous settings, for comparable traffic.
- Log sampling also applies to detailed warning/error logs. Aggregate invocation
  and error analytics are separate; sampling is not a guarantee to retain a rare
  failure's full context, an exact daily cap, or proof of future quota headroom.
- Mirror this policy in the private production configuration, then read back the
  actual deployed settings. Changing only the tracked template would leave the
  separately configured production Worker unchanged.
- During a focused incident, temporarily increase sampling for a bounded window
  and restore the normal policy afterward. Do not enable a paid plan implicitly.

This is a configuration-only change. Download, queue, snapshot, WebSocket,
authentication, D1, and media-management behavior remains the same. The Mac binary
requires no rebuild or restart. Deployment source/version and rollback evidence
remain in ignored local production records rather than this journal.

## Validation

Use the pinned Wrangler schema, existing Worker/Mini App type and integration/UI
checks, and dry-runs of both default and exact production configurations. Live
acceptance reads back sampling settings and the active version/traffic, compares
deployed assets with committed bytes, and checks fresh owner/library snapshots.
GET-only acceptance does not execute a download or media-management action.

Local validation passed with Wrangler 4.149.0: generated binding/source/test types,
353 Worker/logic tests, 68 mounted Mini App UI tests, both configuration dry-runs,
and journal/whitespace checks. No runtime source or media action changed.

## References

- [Workers Logs: invocation logs and request-context sampling](https://developers.cloudflare.com/workers/observability/logs/workers-logs/)
- [Workers Traces: independent sampling](https://developers.cloudflare.com/workers/observability/traces/)
- [Observability pricing and the December transition](https://developers.cloudflare.com/observability/pricing/)
