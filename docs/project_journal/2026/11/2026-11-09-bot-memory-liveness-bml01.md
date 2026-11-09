---
id: 20261109-bml01
title: Bound native coordination memory and detect silent cloud disconnects
status: completed
created: 2026-11-09
updated: 2026-11-09
branch: "wip/bot-memory-liveness"
pr: "63"
supersedes: []
superseded_by: ""
---

# Bound native coordination memory and detect silent cloud disconnects

## Incident and protected behavior

- A long-lived Mac process stopped receiving a Telegram request although the
  Worker had durably accepted it. Its physical memory footprint was 39.1G,
  largely swapped, after about 14 days without exiting. The process retained
  a valid trusted signature and an established cloud TCP connection.
- Restarting the existing executable recovered the queued request successfully
  and reduced the new process footprint to 19.1M. This recovery establishes a
  runtime fault, not the allocation origin of every retained object.
- Keep the existing media, queue, authentication, publication locks, paths and
  recovery identities. Ordinary tests/builds do not change the active service.

## Repair contract

- Bound each synchronous Foundation file coordination operation with its own
  autorelease pool, including native URL/coordinator creation, callbacks,
  native error formatting and object drops. Return owned Rust values.
- Native temporary objects must be released after each read, write or move,
  including errors and nested operations, rather than waiting for a worker
  thread to exit. This follows the Objective-C ownership contract for
  Foundation calls on long-lived Rust threads.
- Probe otherwise quiet cloud WebSockets with protocol Ping and require the
  matching Pong within a bounded interval. Reuse authoritative REST backlog
  reconciliation before reconnecting; preserve persist-before-ACK and ordering.
- Protocol control frames are answered by Cloudflare without waking the
  hibernating Durable Object. Keep hourly REST fallback and on-demand snapshots;
  a transport probe does not publish state or query D1.
- A transport idle detector does not establish a watchdog for a permanently
  blocked local filesystem handler. Avoid introducing handler cancellation or
  transactional concurrency changes under this repair.

## Validation

- Deterministic native lifetime tests using real coordination on a long-lived
  thread, including nested and error cases; prove failure on unchanged baseline
  product code and success on the fix.
- Isolated bounded baseline/fixed allocator measurements under repeated real
  inbox/FileProvider calls, distinguishing live allocation growth from RSS.
- Real local WebSocket tests for healthy/missing/mismatched Pong and reconnect
  recovery with authoritative REST, durable local records and mock Telegram.
- Formatting, strict Clippy, full Rust tests and release build use isolated
  Cargo target/build paths; remote CI/review remain separate evidence.

The five native lifetime cases fail against the unchanged baseline product
implementation and pass with per-operation pools. A first sandboxed run failed
before the native accessors were invoked and is not lifetime evidence; the
valid comparison ran with normal macOS file coordination on temporary fixtures.

An isolated component comparison ran 10,000 real `CloudInbox::pending` calls,
with 256 calls as warmup. Both variants used the same harness, a long-lived Rust
thread and a fallback thread-level pool. The fix alone adds operation-level
draining; the experiment does not use production media or network requests.

| Post-warmup change over 9,744 calls | Baseline | Per-operation pools |
| --- | ---: | ---: |
| Live malloc bytes | +122,274,160 | +8,304 |
| Live malloc blocks | +390,397 | +57 |
| Physical footprint bytes | +123,060,544 | +131,136 |

The baseline grows by about 12.55 KB per call. The fixed live allocation curve
is effectively flat at this workload; retained measurement records themselves
also contribute a small allocation cost. This isolates the repaired native
coordination component and does not substitute for whole-branch validation.
The soak is a manually invoked ignored test, not an automatic CI pass.

A real temporary viewer connection to the deployed Cloudflare endpoint also
received three exact protocol Pongs. The probe submitted no business command
or snapshot refresh. This verifies runtime support; the documented absence of
Durable Object handler wakeups remains a platform contract rather than a billing
measurement.

Whole-branch validation with Rust/Cargo 1.99.1 passed formatting, strict
all-target Clippy, all-target tests (**1,353 passed, 0 failed, 17 ignored**) and
an offline release build. The ignored count includes the existing 16 cases and
the new manual memory soak. Five new native lifetime cases and five new socket
cases, including the mock Telegram reconnect E2E, run automatically in the
existing macOS Rust CI job. Three existing socket ordering/backlog/long-idle
regressions also passed their focused gates.

The successful full run used normal native macOS coordination. An earlier
default-sandbox invocation failed with coordination access errors and was
retained separately; it is not a code regression result. The 45 Rust/Cargo
input hashes were unchanged across the successful checks and release build.

## References

- [Implementation and review: PR #63](https://github.com/Joey-Project/Telegram-Video-Downloader/pull/63).
- [Apple: autorelease pools and long-lived threads](https://developer.apple.com/library/archive/documentation/Cocoa/Conceptual/MemoryMgmt/Articles/mmAutoreleasePools.html).
- [objc2 0.6.4: autorelease pools](https://docs.rs/objc2/0.6.4/objc2/rc/fn.autoreleasepool.html).
- [Cloudflare: automatic protocol Ping/Pong handling](https://developers.cloudflare.com/durable-objects/best-practices/websockets/#automatic-pingpong-handling).
