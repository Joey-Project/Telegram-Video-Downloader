---
id: 20261113-mfi01
title: FLAC uploads in automatic Music imports
status: active
created: 2026-11-13
updated: 2026-11-13
branch: codex/music-flac-integration
pr: "https://github.com/Joey-Project/Telegram-Video-Downloader/pull/68"
supersedes: []
superseded_by: ""
---

# FLAC uploads in automatic Music imports

## Agreed scope

Connect the independent native converter from PR #67 to the existing durable
Music job pipeline. Accept FLAC audio/documents through the existing authorized
Telegram ingress and direct Mac download path. Archive the unchanged original
FLAC in iCloud Drive `Collection/Music`; import only the verified local ALAC into
Music, and require an independent Music-owned copy before archive eviction.

Preserve the source audio losslessly and migrate metadata best-effort, including
multilingual and repeated values. Persist conversion reports and show bounded
warnings in the existing updated task message. Keep failed conversion staging
and the downloaded source. Reuse existing cancellation, safe retry and durable
import-intent behavior. Existing M4A, MP3 and AAC behavior must remain compatible.

## Implementation plan

1. Add FLAC ingress and download-path support, backward-compatible conversion
   settings and optional report storage in prepared-audio records.
2. Run the converter as an isolated, bounded child with the existing cancellation
   flag. Verify its source/output bindings before the manager can import it.
3. Package the helper and its native runtime dependencies during explicit
   profile installation; preserve both fixed Music automation identities.
4. Cover full FLAC job orchestration with mock Telegram/Music boundaries and real
   native codec integration, including original-versus-import archive selection,
   Unicode metadata, warnings, failures, cancellation and recovery/deduplication.

## Validation boundaries

Normal builds/tests do not install executables or restart the production bot.
Real Music automation remains attributed to the fixed signed test or production
binary. Verification results and any untested live boundaries will be recorded
after implementation. Production deployment is a separate explicit gate.

## Implementation

Telegram audio/document ingress and both transfer destination validators accept
FLAC, including MIME-only filenames and case-insensitive extensions. Cloudflare
continues forwarding only raw message/file metadata; no audio bytes pass through
the Worker. The native backend invokes the client-only process API with the
shared cancellation flag, then binds the report to the original and output hashes
and validates the complete ALAC before returning prepared audio.

The existing durable prepared/catalog state carries an optional conversion report;
old records without it remain readable. Full create-only success/failure reports
remain in each private preparation directory. Job status edits include verified
lossless conversion and bounded warnings. Original/archive bytes remain limited
to 512 MiB; a converted import may be up to 1 GiB.

Native helper builds statically link FLAC/Ogg. The explicit installer checks
system-only Mach-O dependencies, signs a staged copy and installs the applicable
profile's helper and upstream notices. Production deployment builds/signs before
stopping the service and retains helper rollback data. Test helper replacement
requires an explicit converter input; normal builds/tests never install it.

## Validation

- The real-converter manager E2E passed with generated tagged 8-bit stereo
  FLAC: source archive bytes/extension, ALAC import/copy, precision warning,
  retry after pre-add failure, restart recovery and exactly one preparation/add.
  The test also rejects a mismatched report and reads legacy prepared JSON.
- Full workspace validation passed: 1,435 main-program tests, 23 converter unit
  tests and six real native-codec E2E tests; 17 existing tests remain ignored.
  The converter suite covers Chinese, Japanese, Korean, Russian, French, Italian,
  mixed Unicode, repeated values and artwork; the manager test covers orchestration.
- Workspace Clippy passed with warnings denied.
- Client-only tests passed 9/9; the fixed test-runtime feature's Music suite
  passed 72/72, including the real-converter manager recovery test.
- Cloudflare metadata ingress: four tests passed; full TypeScript/syntax checks
  passed. The helper links only system libraries/frameworks under `otool -L`.
- Packaging/runtime/service Python suites: 135 tests passed; all 27 offline
  transfer tests passed using the existing pinned Python 3.14.5 test runtime.
  The default interpreter initially skipped two Telethon-dependent cases; the
  pinned-runtime run completed without skips or dependency installation.
- Launch-agent shell tests, both `bash -n` checks and ShellCheck passed.
- The restricted sandbox's macOS coordinator failures were not counted as
  acceptance. Tests using real codec/coordination services run outside that
  sandbox. New fixture paths and tool paths are canonical/absolute, preserving
  the product's existing path restrictions. System audio was verified muted.

## Next steps

Before production rollout, verify the signed helper installation and a real
FLAC-to-Music import through the fixed
test identity. No new real Music track or production service update has been
performed by this integration workstream so far.
