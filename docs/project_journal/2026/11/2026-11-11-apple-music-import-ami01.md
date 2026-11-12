---
id: 20261111-ami01
title: Optional Apple Music import and iTunes Match synchronization
status: active
created: 2026-11-11
updated: 2026-11-12
branch: codex/music-auto-import
pr: ""
supersedes: []
superseded_by: ""
---

# Optional Apple Music import and iTunes Match synchronization

## Confirmed scope

- Prioritize a usable music-import feature before the other pending backlog items.
- Start with files uploaded to the Telegram bot as `.m4a`, `.mp3` or `.aac`.
- The user also requested future `.wav` and `.flac` support by converting to ALAC,
  preferably through Music where supported; evaluate a separate converter if needed.
- Add accepted audio to the Mac's existing Music.app library, which the user
  reports is connected to iTunes Match, and request cloud-library synchronization.
- The user chose automatic import for supported uploaded files, without per-file
  confirmation or a prerequisite settings-page interaction. Broader special handling
  of music obtained from other sources remains separate future scope.
- Support for files larger than the hosted Bot API's 20 MB download limit is required
  in the first release. A small-file-only first slice is not sufficient.
- The user proposed retaining CF as the receiver of message/file metadata, with the
  Mac downloading actual file bytes directly from Telegram. Preserve this division
  if the required large-file transport can be validated safely.
- The initial authorization covered isolated large-file transport, followed by
  actual Music import and cloud synchronization of three approved `.m4a` files.
  The user then authorized automatic-ingress implementation and isolated integration
  validation. Production deployment remains a separate step.
- Archive original files in iCloud Drive `Collection/Music`. Prefer Music's native
  copy/organization behavior so the playback file lives in its own local media library.
  Evict only the archived original's local bytes, after independently verifying the
  iCloud Drive upload and a distinct, local Music-owned playback copy. iTunes Match
  song synchronization and iCloud Drive original-file upload are separate outcomes.

## Current validation

The three approved real M4A files passed the isolated component pipeline on
2026-11-12. This is not a production automatic-ingress deployment.

| Boundary | Observed result |
| --- | --- |
| Large Telegram downloads | All three exceed 20 MB; size, container and full decode checks passed. |
| Original archives | All three uploaded to iCloud Drive `Collection/Music`; metadata still reports not downloaded. |
| Music local import | All three added once, with persistent IDs saved; Music made independent local media-library copies. |
| Music copy integrity | Whole-file SHA-256 and size matched again after Music cloud upload. |
| Music cloud library | All three report `uploaded`; no explicit cloud-library UI command was needed. |
| Authorization | The fixed test identity retained its grant across signed updates; scoped child-event attribution was verified. |
| Earlier component regression | 1387 Rust tests passed, 17 ignored, zero failed; 17 Python runtime-helper tests passed. Strict feature-enabled Clippy, formatting and the production compile check passed before automatic-ingress implementation. |

Private final acceptance is retained in the ignored task's
`state/final-acceptance.json`, alongside native receipts and copy proofs. Keep the
task's verified audio fixtures, isolated build cache and rollback artifacts for the
upcoming production integration. The production bot was not deployed or restarted.

## Automatic-ingress implementation

The user requested proceeding from component validation to automatic bot integration.
The implementation is on `codex/music-auto-import`; this workstream has not refreshed
production. GPT-6.1 coordinates GPT-6 Luna Max implementation, test and research tasks.

- Parse supported Telegram audio/document metadata and authorize the private sender
  before admitting a job. Save the job before advancing polling or cloud-dispatch
  completion. Cloud ingress remains metadata-only; do not migrate the hosted webhook.
- Keep music jobs and their content catalog separate from URL/video/PDF task records,
  beside the existing media-root state. Use the unified hidden download-root staging
  area and preserve interrupted/cancelled files. Slow operations run off the Telegram
  handler; cloud-waiting songs must not block later imports.
- Package the proven, pinned low-level MTProto transfer as an isolated helper owned
  by the Rust service. Enforce its no-updates RPC boundary before every send, explicit
  process/output/time bounds, partial retention and safe migration/reference errors.
  Install dependencies explicitly; never install packages during bot startup.
- Validate the original and retain its identity separately from any lossless AAC
  remux used for import. Permanently archive original bytes under `Collection/Music`.
  Import from completed staging, never from a possibly evicted archive placeholder.
- Reuse the fixed native identity and owned AppleScript interpreter. Normal operation
  checks consent without a system-mute gate; controlled live tests still require mute.
  Check readiness before the durable add intent. Preserve intent-before-add and
  PID-before-status ordering, SHA-based deduplication and query-only recovery.
- Update one Telegram status message per music job. Provide `/music` inspection and
  explicit authorization/retry/cancel controls; never silently repeat an ambiguous
  add. Record confirmed edit text only after Telegram accepts it; bounded retries
  update the same message, including terminal results. An uncertain initial send
  is never blindly repeated.
- Add mocked Telegram/job E2E tests, transport safety tests and CI coverage. Verify
  the independent test profile before a separately controlled production deployment.
  Retain the three accepted real-track receipts for dedupe handover rather than
  adding those tracks again.

Validation uses isolated Cargo paths and the independent signed test identity.
The production compile check and 52 targeted Rust Music tests passed. The latter
requires execution outside the desktop sandbox so macOS File Coordination can
access its system service; the initial sandboxed coordination failures did not
reproduce there. File-only codec validation covers M4A, MP3, raw AAC lossless
remuxing and corrupt input rejection. The Python Music helpers passed 48 tests;
the existing service helpers passed 101 tests and their shell integration test.
`bash -n`, ShellCheck and formatting checks passed. After the fixes below, the full
feature-enabled Rust regression passed 1424 tests with 17 ignored and zero failures.
All 52 targeted Music tests, 50 cloud-inbox tests and strict Clippy with and without
the test-runtime feature passed. The local Worker/D1 metadata E2E file also passed
all three tests. Remote CI and PR review have not been run for this local checkpoint.

The first query-only catalog handover exposed a native compatibility issue before
any catalog write or Music add: Foundation can successfully return no optional
ubiquity property for an ordinary local Music file. The adapter now checks the
regular, materialized, independent copy's full content hash and stable identity
first, then consults `NSFileManager.isUbiquitousItemAtURL` only for that absent value.
Actual Foundation read/type errors still fail, and either API reporting iCloud
storage rejects the playback copy. The same ordering applies before archive
eviction. A macOS test covers a private regular local file without Music access.

The explicit test-runtime installer also completed against the pinned dependency
set. `pyaes==1.6.1` has no public wheel, so that one pinned pure-Python dependency
allows a source distribution; the remaining dependencies stay wheel-only. No
package installation happens during bot startup.

The real helper startup exposed two additional gaps in fake-only transport tests:
generated auth/upload response types live in Telethon's submodules, and legitimate
redundant DC endpoints must not cause the whole DC to be discarded. The adapter
uses the exact generated response types, rejects unsupported responses, and chooses
one deterministic eligible main IPv4 endpoint per DC. The pinned-profile Python
suite now passes 26 tests, including real generated-type and DC-option regressions;
CI installs that exact dependency set before running them. A corrected bounded
helper probe downloaded and published a complete 69,134,734-byte real file.

Failed and cancelled tasks now remain terminal across restart. Only explicit retry
resumes them; recognized expired-reference, rate-limit, timeout and busy errors use
fixed safe guidance, while arbitrary backend errors remain generic. Tests cover
resend guidance without archive/add side effects and terminal-state preservation.

All three real test-profile jobs completed through source validation, saved-PID
deduplication, copy/cloud verification and confirmed in-place Telegram completion.
The resumed run reused the retained 32 MiB fragment. One saved-PID status read failed
transiently; an explicit test-user retry recovered through that ID without another
add. Final independent hashing verified all 197,187,578 source bytes and all three
Music copies against the earlier acceptance hashes. Music IDs, archive paths and
the original three status-message IDs were unchanged; cloud originals remained
dataless placeholders. Private proof is retained in
`.codex-tmp/music-auto-e2e-20261112/final-acceptance.json`.

The automatic-ingress validation deliberately reuses the three approved tracks
already added by the component tests. It does not add another track. The native
add operation has the earlier three-file component proof and mocked job integration;
a new song's first add through automatic ingress is a separate live acceptance case.

Real control messages also exposed local command latency: Telegram-to-Worker
ingress stayed below one second, but busy local dispatch took up to about 40 seconds.
An idle before-fix `/help` control completed in 4.5 seconds. Source inspection found
that each one-second dispatcher poll performed another coordinated terminal-history
sweep while holding the global backlog gate. Terminal retention now runs hourly
after a stable bounded pass; a confirmation losing active protection requests a
fresh pass without rescanning all completed records on every poll. Directory churn,
private-access failures and active confirmation protections keep their existing
safety checks. The counting-provider regression checks both skipped idle reads and
eventual expiry cleanup. The after-fix idle control, using the same test profile and
process policy with completed jobs, took 2.42 seconds. This comparison does not
establish an exact improvement for the earlier busy-download case.

The full test-bot supervisor uses the production service's default process policy;
forcing `Background` in the initial private harness introduced an extra scheduling
variable. Its debug validation build optimizes the SHA-256 dependency so large-file
integrity checks do not spend minutes in unoptimized software hashing. These are
test-harness choices, not production deployment changes.

## Initial proposal

- Reuse durable ingress/task receipts and one message updated as the import progresses.
- Validate actual audio/container metadata, then retain the source in stable managed
  storage before handing it to Music.app. Preserve useful embedded tags and artwork.
- Use Music.app's supported automation interface for import. Record the returned
  track identity and distinguish local import from cloud synchronization outcomes.
- Show matched and uploaded as separate successful cloud outcomes, with waiting,
  ineligible and error states remaining visible. A local import is not cloud success.
- Keep original files. Duplicate suppression should cover retried/repeated submissions;
  ambiguous import delivery must not cause blind repeated additions.
- Accept both Telegram audio messages and audio sent as documents. Update a single
  task message through receipt, local import and cloud synchronization states.

## Initial capability findings (before integration)

- The installed `/System/Applications/Music.app/Contents/Resources/com.apple.Music.sdef`
  defines `add`, returned track references, persistent IDs, file-track locations and
  readable cloud status. It was inspected without launching Music or sending events.
- Cloud status includes matched, uploaded, ineligible, error, duplicate and not uploaded.
  Matching and uploading are distinct successful outcomes; completion follows the actual
  reported state rather than a successful `add` call.
- The dictionary exposes no explicit Update Cloud Library command or copy-to-Media-folder
  preference. Use Music's native Sync Library behavior first; explicit UI refresh would
  require separately designed UI automation rather than a fictitious scripting command.
- The bot currently models text updates only and has no `getFile` helper. Both direct
  and Cloud ingress need audio/document modeling; the existing durable job/receipt and
  status-message patterns are reusable.
- Telegram's current hosted Bot API documents a 20 MB download limit. Its official
  local Bot API server, in `--local` mode, removes that download limit and can return
  an absolute local path from `getFile`. This requires an application `api_id` and
  `api_hash` in addition to the existing bot token.
- Moving the same bot to the local Bot API is an ingress migration, not a proven
  download-only add-on: Telegram requires cloud `logOut` before local operation and
  does not guarantee update delivery with simultaneous Bot API server instances.
- A Mac-hosted local Bot API could send its webhook outbound to the existing public
  CF Worker while listening only on loopback for the Rust client. This topology is
  a design inference, not a live-tested deployment. It introduces Mac availability
  into the path before CF durably stores incoming messages, weakening the current
  cloud-first offline-ingress property. Telegram documents at most 24-hour retention
  of pending Bot API updates; do not promise indefinite offline delivery.
- Telegram supports bot-token MTProto authorization with an application API ID/hash,
  and `invokeWithoutUpdates` requests that do not subscribe a connection for updates.
  File-transfer connections are excluded from ordinary update subscriptions. These
  are relevant building blocks for a download-only helper, not proof that a particular
  client library safely coexists with the hosted Bot API webhook.
- The official Bot API server passes the opaque Bot API `file_id` directly into TDLib
  `getRemoteFile`, then downloads the returned internal file ID with `downloadFile`.
  This establishes a source-backed interoperability path; absence of explicit wording
  in the high-level TDLib docs is not evidence that these IDs are incompatible.
- Apple's current Mac Music guide documents a 200 MB per-file cloud-library limit.
  Receiving a large file and adding it locally must remain distinct from cloud
  eligibility; do not promise cloud synchronization for every accepted upload.

## Original design questions

- Confirm `.aac` container handling. If normalization is needed, prefer lossless remuxing
  into `.m4a` and retain the original; do not silently transcode based on extension alone.
- Validate Music's native copy/organization behavior without reorganizing the entire
  existing library. If it still references the archive, do not evict that source.
- Define first-run macOS Automation authorization and a bounded, resumable waiting state
  when the user session, Music or cloud synchronization is unavailable.
- Choose and validate the required larger-file transport. Compare official local
  Bot API migration against a bot-authorized MTProto/TDLib download-only helper;
  prefer the helper if it preserves hosted webhook delivery. Audit the chosen client's
  authentication and automatic background requests: ignoring delivered events is not
  equivalent to avoiding an update subscription. A generic TDLib event loop must not
  be assumed to provide download-only isolation.
- Before committing to the helper, demonstrate a real upload larger than 20 MB with
  local size/content integrity checks, ongoing text/audio/callback webhook delivery,
  Mac-offline durable ingress and subsequent download, and interrupted/retried transfer.
  The partial transport proof and independent test-user setup below do not yet cover
  this complete acceptance matrix.
- Keep retained audio originals separate from interrupted-download cleanup candidates.

## Transport validation

- The user created `/Users/joey/.config/telegram-video-downloader/mtproto.json` after
  registering a Telegram application. Set its mode to `0600` as explicitly requested,
  repaired one missing comma without changing either credential value, and verified
  the JSON fields without printing their values. The existing bot token remains in
  the canonical configuration.
- Recorded only the credential-file path and its privacy preference in both the task
  worktree's and canonical checkout's `AGENTS.md`. Preserved the canonical checkout's
  unrelated changes; neither credential values nor the credential file were added
  to the repository.
- Reviewed current Worker ingress: the authenticated owner webhook stores the original
  update in D1 and exposes it through the existing desktop request-fetch interface.
  No file bytes need pass through CF for this design. The Rust Telegram message model
  still needs audio/document support in the future production implementation.
- Added `cloud/test/music-metadata-ingress.test.ts`. Targeted local validation passed
  all three tests using the real local Worker/D1 test environment and synthetic Telegram
  updates: owner audio and MP3-document metadata above 2^31 bytes survives desktop fetch;
  unacknowledged requests remain available on repeat fetch; duplicate delivery preserves
  one original payload; a foreign sender is rejected. These are metadata tests, not
  actual multi-gigabyte file transfers or Rust-side audio handling tests.
- Command: `cd cloud && npm exec vitest run -- test/music-metadata-ingress.test.ts`.
  Final run passed one file / three tests. Wrangler reported a non-fatal permission
  error writing its default debug log outside the sandbox; Vitest exited successfully.
- At the metadata-only test stage, no additional Telegram session had been authenticated
  and no webhook had been changed; the later isolated test-user setup is recorded below.
- Client audit found a second live-test prerequisite: a stock "no updates" option is
  insufficient if authentication/startup still invokes update-state or difference RPCs.
  The isolated probe must audit initialization and reject such RPCs before transport;
  suppressing callbacks after receiving updates is not an acceptable substitute.
- Audited Telethon v1 and Pyrogram startup paths have update-state calls beyond simple
  event delivery. Telethon's convenience Bot API ID decoder also supplies empty file
  references. These findings led to the isolated raw-sender probe below rather than
  using a stock high-level client. Production music-import integration is still pending.
  Telethon's GitHub archive records a migration to Codeberg, not proof of abandonment.
- Register the application at `https://my.telegram.org` under API development tools.
  Store `api_id` and `api_hash` in a private local JSON file, preferably
  `~/.config/telegram-video-downloader/mtproto.json` with mode `0600`, and share only
  its path in the task. Do not put application credentials into tracked configuration,
  test fixtures, logs or chat messages.
- Prepared a deterministic 32 MiB binary fixture for real transport acceptance:
  SHA-256 `3ed677b1cf7c0a9e993958c199e8f8f280a79d20230a481e7cb594415c96f62e`.
  It is retained privately under `.codex-tmp/music-credentials-20261111/`. Following
  the user's suggestion, automate file creation on Telegram with one Bot API
  `sendDocument` to the already-configured owner chat and use the returned file ID;
  manual upload is not required to validate the large-file download path. This is a
  bot-authored outgoing message, not a user-authored incoming webhook event. The
  isolated download-only probe and dependency
  environment are retained under `.codex-tmp/music-transport-20261111/` for the next
  validation step; no session keys or credential values are persisted there.
- Verified existing Wrangler authentication matches the production manifest's CF
  account and includes D1 access. A bounded read-only SELECT over the latest 200 inbox
  sequence values found no uploaded fixture/control message yet. It did not acknowledge,
  delete or mutate requests. The private read-only helper is retained beside the fixture;
  that directory has mode `0700`. Installed Wrangler 4.149.0 emits D1 `--json` through
  its `log` level; narrower `error`/`info` levels suppressed the result during initial
  attempts. The final read succeeded after checking the installed logger source.
- The raw-sender probe uses Telethon 1.45.0 and a bounded file-ID decoder preserving
  file references. It enforces an outer `invokeWithoutUpdates` wrapper and a strict
  outgoing RPC allowlist, avoiding high-level login/update-state synchronization and
  suppressing credential-bearing request logging. Fifteen final offline tests passed; a
  separate read-only source audit checked bootstrap, DC auth, download and teardown.
  The HTTP fixture helper passed six mocked tests. Local mocked Worker/D1 ingress,
  real file transfer and real user-authored webhook delivery remain distinct evidence.
- Application API ID/hash identify the Telegram application; they do not authenticate
  the user's personal Telegram account. Bot-token automation uses the bot identity.
  Do not silently create or reuse a personal account session to manufacture incoming
  messages, and do not inject fake Telegram update IDs into the production inbox.

### Live result on 2026-11-11

- Sent the approved 32 MiB fixture exactly once through hosted Bot API `sendDocument`
  to the configured owner private chat (message 627), with notifications disabled.
  Captured its returned file ID privately; manual user upload was unnecessary.
- Real file metadata exposed a decoder assumption that synthetic tests had missed:
  the server's major-4/minor-61 document ID has no extra inner version pair. Corrected
  the parser against an independent decoder and added a sanitized regression fixture.
- The first raw bot authorization explicitly requested DC5. Added one bounded auth
  migration using an endpoint from `help.getConfig`, retaining the outer NoUpdates
  wrapper and rejecting second/unknown/unconfigured migrations. Added fake-sender
  regression tests for migration, request order and the cross-DC path.
- Downloaded 4,194,304 real bytes and stopped intentionally. Verified the unpublished
  `.part` was exactly 4 MiB and its content matched the source prefix. Restarted the
  probe on that partial, downloaded the remaining 28 MiB, and verified all 33,554,432
  bytes against SHA-256
  `3ed677b1cf7c0a9e993958c199e8f8f280a79d20230a481e7cb594415c96f62e`.
- Before and after the MTProto runs, `getWebhookInfo` matched the production CF
  webhook URL, reported zero pending updates, and reported no webhook error date.
  This checks configuration/status; it does not prove concurrent real user-authored
  ingress delivery. No personal account login or synthetic production inbox insertion
  was used, and no production service deployment or Music import occurred.
- Final executed probe source SHA-256:
  `a60f1b60974a883ddf800354381901e8da1f6213edd7882dc5aa982689a40679`.
  Sanitized evidence is retained privately in
  `.codex-tmp/music-credentials-20261111/bot-api-roundtrip/evidence.json`, alongside
  the source/download samples and private API response metadata. Keep these local
  artifacts for the implementation handoff; do not commit file IDs or credentials.

## Real-user E2E sender setup discussion

- The user selected an existing independent test account after considering the main
  account, and requested displaying its login QR code as an image in the chat.
- The supplied `e2e-bot.json` token was verified against the Telegram API as
  `@joey_local_downloader_test_bot`, distinct from production and initially without a
  webhook. Its file mode is `0600` and the private credential directory is `0700`.
  The first pending-update check was empty; after the user resent `/start`, verified
  its private sender was an independent user and saved `e2e-identity.json` privately.
- Recorded the test token/identity/session paths in the task and canonical `AGENTS.md`
  without storing values. Production configuration and its unrelated edits were preserved.
- The current cloud deployment intentionally has one owner. Both Worker webhook/
  Mini App authorization and Rust cloud configuration enforce that model. A separate
  test bot, Worker environment, D1 and Durable Object namespace/bindings can reuse the
  same application code while retaining the single-owner model in each environment.
- A second manual Mac process can use the same binary with its own configuration
  file. Give it a separate config directory, video/PDF roots, library registry,
  queues/cloud local state and explicit Bilibili auth/credential paths. The latter
  default to shared HOME paths and are not isolated merely by copying the config.
- Reuse the existing application API ID/hash file for the test user's login. A test
  bot needs its own bot token, and the test user needs its own private local session.
  Configure the test Worker owner and Rust allowed chat ID to match that test user.
- The isolated raw-sender QR helper passed 11 focused offline tests and dependency/
  syntax checks. It accepts only QR auth/self operations, has a bounded update queue,
  rejects history/update-state calls and mismatched accounts, and uses a local hidden
  password dialog if 2FA is required. It saves a `0600` SQLite session only after the
  exact test-user ID matches. No test messages are sent by the login helper.
- The first QR expired before use. A fresh QR was displayed and the user scanned it.
  Verified the helper's `authorized` status, the saved nonempty `0600` session, and
  a separate live read-only self/bot identity check against the configured independent
  test identities. No new QR login is needed for subsequent test runs.
- The raw test-user command runner uses the authorized session read-only, rejects
  other peers and accounts, and preserves a durable send intent rather than blindly
  resending after unknown delivery. The initial eleven fake-sender tests passed.
- An offline environment generator/runbook was prepared and its five targeted tests
  passed with temporary paths. A test initially referenced a default home path; this
  was fixed before live use. The independent environment was then created as recorded
  below; the production allowlist, Worker, LaunchAgent and Music library were preserved.

### Independent live E2E environment and result

- Created Worker `telegram-video-downloader-e2e` at
  `https://tg-downloader-test.mahane.me`, a new D1 with the same test-only name, and
  its independent `NotificationHub` Durable Object namespace. Read-only inventories
  proved the names were absent before creation and exactly one test database/Worker
  existed afterward. The test D1 differs from the production ID.
- Applied the six tracked schema migrations and passed Wrangler's deployment dry run
  before publishing version `3b1fac26-626b-4817-b266-edf49b4b8d53`. The generated
  config retains the application's existing compatibility date and observability
  sampling. Test owner and bot identities are checked against the private verified file.
- Persistent private test configuration and Worker secrets are under
  `~/.config/telegram-video-downloader/e2e/`, including `config.toml` and
  `wrangler.jsonc`. Media/queue/state roots are isolated under
  `~/.local/share/telegram-video-downloader/e2e/`; Bilibili auth paths are explicit
  test paths. No MTProto credentials or authorized user session were uploaded to CF.
- Changed only the independent test bot's webhook, preserving pending updates.
  Its previously sent `/start` arrived through the real webhook and was processed
  by a manual second instance of the existing signed stable binary. The wrapper
  verifies isolated configuration, clears inherited cloud-secret overrides, bounds
  runtime and private logs, and stops only its own verified child process group.
- With that Mac instance stopped, the independent user runner sent `/help` once.
  Verified the real command and expected sender in the test D1 before restarting
  the Mac instance. Reused the send checkpoint without resending; the expected help
  reply arrived. A subsequent live `/queue` returned the expected empty-queue message.
- All three local request receipts reached `completed`. Final test D1 pending count
  was zero; test webhook URL matched, pending count was zero, and no webhook error
  date/message was reported. Both manual test processes exited cleanly after the
  wrapper signalled only their own groups. The test cloud resources and private
  configuration/session remain available for on-demand runs; no test LaunchAgent
  or recurring test job was installed.
- Focused offline helper coverage includes QR authentication (11 tests), environment
  generation (5), bounded CF operations (6), test-user messaging (14), and manual
  process isolation (8). All were run with fake transports/temporary paths. The
  messaging tests cover Telegram's short send acknowledgments and private incoming
  messages without an explicit sender field. The final combined suite passed all
  44 tests; project-journal validation and `git diff --check` also passed.
- Retained helpers and sanitized evidence locally under
  `.codex-tmp/telegram-e2e-20261111/`. Cloud metadata integration tests, the earlier
  real 32 MiB bot-authorized transfer, and these real text-command E2E checks are
  separate proofs: incoming large-file plus concurrent download/callback acceptance
  and automatic Music import/cloud synchronization remain pending.

### Real incoming large-document acceptance (passed)

- The user authorized continuing after the independent command E2E setup. Validate
  an actual user-originated 32 MiB document, with the Mac test service stopped,
  then capture its real Bot API metadata from the independent CF inbox. Reuse the
  already verified deterministic binary fixture; do not import it into Music.
- Download using only the independent bot's credentials and captured file ID.
  Stop at 4 MiB, verify the retained unpublished prefix, then resume and verify the
  complete 32 MiB SHA-256. Preserve durable send intent to avoid duplicate uploads
  after ambiguous message delivery. Keep all file IDs and raw envelopes private.
- The starting checks found no test Mac process, no pending CF requests or Telegram
  updates, and no webhook error. The source fixture still matches its recorded hash.
  Production services remain outside this validation scope.
- The independent user uploaded the approved binary once with an exact caption/name.
  The frozen sender source was
  `4ba964b0d291404618230152d3cafea790bef48794b2f772f9d4027597a23b49`.
  Re-running against its existing durable intent returned `already-attempted` before
  any Telegram connection. No duplicate media send was made.
- With the Mac test service still stopped, two real CF reads found the same pending
  document at sequence 4, with matching owner/chat, size, filename, caption and
  envelope SHA-256. The serialized private checkpoint occupied 1,134 bytes; the
  capture enforced a 64 KiB metadata ceiling. The 32 MiB file bytes traveled directly
  between the Mac and Telegram. File IDs and raw envelopes remain in `0600` files.
- Preflight accepted the real incoming Bot API file ID. A supervisor used only the
  independent bot configuration and the previously frozen raw downloader; it enforced
  a whole-process-group 180-second deadline and a 256 KiB log limit per phase.
- Stopped the real download after exactly 4,194,304 bytes. The supervisor verified
  that the final file was absent and the retained `.part` matched the source prefix.
  The expected controlled-stop child exit was 2. Resume consumed that safe partial,
  downloaded the remaining 28 MiB, and produced all 33,554,432 bytes with SHA-256
  `3ed677b1cf7c0a9e993958c199e8f8f280a79d20230a481e7cb594415c96f62e`.
  A separate final verifier confirmed the full hash and absence of the partial file.
- The hosted test webhook URL stayed correct with no reported error. Started the
  existing signed binary in its isolated test profile afterward; its normal receipt
  path completed the generic document request and a new real `/help` got the expected
  reply. This is a post-download command check, not a concurrent-download/callback
  test or proof of audio import support. All five local test receipts were completed
  and the final CF pending count was zero. The manual test process exited cleanly.
- The final helper suite passed 69 tests (including 10 media-send, 7 metadata-capture
  and 8 transfer-supervisor tests); the raw downloader source remained unchanged.
  Full proof, private input/checkpoints and the 32 MiB downloaded sample are retained
  locally in `.codex-tmp/telegram-e2e-20261111/large-ingress/` for the implementation
  handoff. No production service, Music library or cloud synchronization was changed.
- Next phase: integrate typed audio/document ingress, durable audio-import tasks,
  the validated large-file transfer, and Music automation. Real supported audio,
  callback delivery while a download is active, macOS Automation permission, duplicate
  import recovery and actual iTunes Match outcomes still need acceptance coverage.

### Real Music test preparation and automation diagnosis

- Prepared test-only audio capture/download helpers, per-track Music import receipts,
  a native iCloud archive/status/eviction helper and bounded diagnostics under
  `.codex-tmp/music-import-live-20261111/`. The original frozen large-file probe
  remains unchanged. The audio transport copy only changes the accepted size range
  to positive bytes through 512 MiB; capture additionally caps the three-file total
  at 600 MiB. Fake tests passed: audio capture 7, audio transport 5, Music bridge 11.
- System audio was muted and read back as muted before Music operations. Keep it
  muted after this test. No track was added, converted or evicted during preparation.
  The independent test inbox was empty at the latest checked baseline; the three
  user-selected `.m4a` inputs had not arrived yet.
- The exact iCloud Drive `Collection/Music` folder exists. A real native resource-key
  query reported ubiquitous, uploaded, current, not uploading/downloading and no
  conflicts for the directory. This is not evidence that future individual audio
  files have uploaded. Per-file upload plus an independent Music-owned local copy
  remain mandatory before eviction.
- Music's static dictionary supports selecting an app-level current encoder and
  converting tracks, but no per-call encoder or per-track consolidation interface.
  Native WAV-to-ALAC remains the preferred future test, with prior encoder settings
  preserved/restored. Direct FLAC handling remains unverified. Avoid running global
  Organize Library against existing unrelated content as a test.
- Music version lookup returned 1.6.3. Read-only playback-state and encoder queries
  timed out with `-1712`; the stage-tagged bridge confirmed `Music.player-state` was
  the first failing property after mute/process/version checks. The user confirmed
  the UI was normal with no startup/account prompt. Removing text coercion did not
  resolve the playback-state timeout.
- A normal quit succeeded, Music exited, and it was reopened normally. One subsequent
  stage-tagged read-only preflight still timed out at playback-state. The command
  context is Aqua, UID 501, matching Music. The responsible Codex app declares both
  the AppleEvents usage string and automation entitlement. Scoped TCC logging did
  not establish a denial; the bounded no-prompt native permission probe did not
  return a permission decision. Do not attribute this to missing Full Disk Access
  or silently reset TCC.
- A one-second Music process sample found an idle main event loop and an AppleEvents
  `designatedCodeRequirementsCacheQueue` waiting on a semaphore. This narrows the
  investigation to the AppleEvents signing-identity path but does not prove the
  daemon's root cause. Read-only launchd inspection identified the running service
  as `system/com.apple.coreservices.appleevents`, not a user-domain service.
- A system AppleEvents-service restart was previously authorized and attempted via
  the standard administrator path, but a restart was not observed: the service PID
  remains 816. The earlier authorization request is no longer present. Under the
  user's updated requirement to authorize the fixed native test binary, do not repeat
  the service restart; validate authorization against that binary after signing and
  installing it. Production deployment remains unchanged.

### Fixed-identity native Music bridge status (2026-11-12)

- The user requires two separately named and signed automation identities. Production
  remains fixed at `/Users/joey/.local/lib/telegram-video-downloader/bin/telegram-video-downloader`
  with identifier `io.github.telegram-local-downloader.bot`. The test identity is
  `/Users/joey/.local/lib/telegram-video-downloader-test/bin/telegram-video-downloader-test`
  with identifier `io.github.telegram-local-downloader.bot.test`. Do not change or
  replace the production executable or its identity as part of this work.
- The Rust in-process AppleEvents implementation, embedded `Info.plist`, test-runtime
  feature, and isolated signing/install helper are implemented. Latest-source checks
  passed: 3 native-module tests, a complete test-runtime Cargo build, default production
  profile `cargo check`, 10 helper mock tests, and formatting.
- The test binary was signed with the existing certificate whose SHA-1 is
  `C24FE0C91539BD95BA4081CD944A8E17053FB304` and installed at the fixed test path.
  Strict designated-requirement verification matched identifier
  `io.github.telegram-local-downloader.bot.test`.
- An attempt to run `--music-authorize` from Cargo's temporary binary path was explicitly
  rejected before any AppleEvents request. With system audio muted and verified, a
  one-shot `--music-check` using the installed fixed test binary returned
  `needs_authorization` without displaying a dialog. The scoped TCC event explicitly
  recorded `Prompting for access to indirect object Music.app by
  telegram-video-downloader-test`, identifying the intended sender. The user selected
  Allow in the system window. The fixed test binary's first explicit `--music-authorize`
  returned `musicPermission=granted`, then its query timed out after 7 seconds; this is
  a query timeout after permission was granted, not an authorization failure.
- A subsequent GUI one-shot `--music-check` started a fresh process of the same installed
  test binary without prompting and returned `musicPermission=granted`, `ready=true`,
  Music version `1.6.3`, player state `stopped`, app mute `false`, and encoders `AAC
  Encoder`, `AIFF Encoder`, `Lossless Encoder`, `MP3 Encoder`, and `WAV Encoder`. System
  output mute was verified `true`. This establishes a successful query and permission
  reuse for the currently installed binary.
- A new one-second Music process sample did not show the earlier
  `designatedCodeRequirementsCacheQueue` blockage. The evidence does not establish the
  exact cause of the first query timeout. The production executable's SHA-256 remained
  unchanged during installation and testing.
- Rebuilt and signed the lint-corrected source (`0844` hash prefix) at the same test
  path. Its code-directory hash changed from `73f8491d9e66f36a50de7651937ca5f75c90b3e3`
  to `d85727bf9e103ee6f99ad0bb51d9fcbf8854ef3c`; certificate, identifier, and strict
  designated requirement stayed the same. An updated-process check eventually returned
  `musicPermission=granted` without another authorization request, establishing
  permission reuse across this signed update. Its Music query timed out, so this result
  alone is not full read-only readiness for the updated artifact.
- One updated-process check exceeded its expected duration before any Music request.
  A one-second sample of that exact test LaunchAgent PID showed `system_output_is_muted`
  inside CoreAudio `HALSystem::InitializeShell` / AVFCapture initialization. This is
  separate from loss of Music consent. Stopped only that one-shot test job and added a
  worker-thread deadline to the mute check: inability to verify mute returns an error,
  never permission to proceed. The new source has pure tests for
  successful results, preserved errors, and a blocked probe timing out with explicit
  release of the test worker. The status helper now exposes bounded diagnostics for
  completed non-JSON failures, with 12 offline helper tests passing.
- The first full-suite attempt stopped at the 3 GiB disk-reserve guard. A subsequent
  sandbox run completed with 1116 passed, 257 failed, and 17 ignored; sampled failures
  were macOS `NSFileCoordinator` reads of temporary test directories returning
  `NSCocoaErrorDomain` 256. The non-sandbox full-suite run of the mute-deadline source
  passed 1375 tests, with 17 ignored and no failures (1392 total), in 315.56 seconds;
  those filesystem errors did not reproduce outside the sandbox. A final equivalent
  integer-predicate lint correction (`871e` source hash prefix) passed strict Clippy,
  formatting, and all five focused native-module tests. The final binary was built,
  signed and installed at the same fixed test path, with code-directory hash
  `147b65f37975942ec2d836d7203a15f79eb62cb2` and the unchanged strict designated
  requirement. Both subsequent nonprompting checks exited with verified system mute
  and `musicPermission=granted`, establishing permission reuse for this final update.
  Both returned `ready=false` because the read-only Music query exceeded 7 seconds.
  The user then normally quit Music; reopened it with `open -b com.apple.Music`
  (new Music PID 74094). One controlled check using the same final fixed test binary
  returned `musicPermission=granted`, `ready=true`, version `1.6.3`, player state
  `stopped`, and all five encoder names, including `Lossless Encoder`. System output
  mute was verified `true`. Permission reuse and read-only query readiness therefore
  passed after the normal Music restart. This does not establish the exact cause of
  the earlier stalls. No additional authorization request or global AppleEvents-service
  restart was made. The private result is retained as `after-music-restart.json` in the
  task's ignored native-identity evidence directory. The full-suite result precedes
  only the equivalent integer-predicate lint correction.
- The three user-approved `.m4a` uploads have now arrived in the independent test
  inbox, totaling about 197 MB, with each file larger than 20 MB. The verified owner
  and private-chat checks passed. Telegram labels them `audio/mpeg`; the private
  capture helper now accepts that hint for `.m4a` names, with eight offline tests
  passing. All three downloads passed declared-size checks, SHA-256 recording, M4A
  container inspection, and full decode-to-null validation without playback. They
  are 48 kHz stereo ALAC and require no conversion. The originals were archived
  create-only under the requested iCloud Drive `Collection/Music` folder, with copy
  hashes checked and staging retained. Individual iCloud resource checks confirmed
  all three uploaded with no pending upload or conflicts.
- Added native test-runtime import/status operations through the same fixed signed
  sender, with private content-bound requests and durable per-hash receipts. Seven
  mock tests passed for path/literal validation, unknown-add no-retry, saving the PID
  before status, and status without source bytes. The final lint-corrected source
  passed strict feature-enabled Clippy; the Python helper passed 16 mock tests for
  request validation, flag mapping, status diagnostics and single-flight behavior.
  The signed import build passed a live Music readiness check. Its first import
  attempt stopped in the shared CoreAudio mute probe before source verification,
  intent creation or any Music add; the receipts directory did not exist afterward.
  Both CoreAudio initialization and an embedded Standard Additions query proved
  unreliable here. A fixed volume-only system `osascript` child now verifies mute
  with a deadline, capped output and kill/reap cleanup. Its checks passed. The
  updated source passed strict feature-enabled Clippy and 16 focused Music tests;
  the Python runtime helper passed 17 tests, including the normal-quit action.
  A controlled normal quit succeeded, Music's old PID exited, and Music was reopened.
  The embedded read-only Music query still timed out despite the retained grant.
  This does not establish a TCC permission failure or a specific Music internal bug.
- The fixed signed LaunchAgent program now owns a bounded system AppleScript
  interpreter for internally generated Music scripts. The final build passed all
  16 focused Music tests; strict feature-enabled Clippy passed before an equivalent
  diagnostic-wording correction. The installed update returned `ready=true`, retained
  Music consent and verified system mute. Scoped TCC evidence tied the real
  nonpreflight Music event's interpreter PID to the fixed test path and exact signing
  requirement, with that bot as the responsible process. The successful query supplies
  the observed allowed outcome; the reply log does not expose an authorization value.
  No additional consent was requested. Stable authorization ownership is the user's
  requirement; same-process interpretation was an implementation assumption.
- The first import using this runner passed Music preflight but blocked opening its
  archived source before intent creation or any add. A scoped one-second sample
  located the wait in `verify_source` / `open`. The original now has `dataless` flags;
  iCloud metadata confirms it remains uploaded but not downloaded and not downloading.
  No explicit archive eviction was run, so this is an automatic local eviction.
  The TCC full-disk-access preflight was denied; that result alone does not establish
  the cause of this file wait. Check for a targeted access prompt and explicitly
  request materialization before attributing it to permission failure. All three
  cloud originals are uploaded and already not downloaded. Stopped the blocked
  one-shot job before any intent/add and retained its log.
- Corrected the import source to the complete local download, rather than rehydrating
  the permanent cloud archive. Created private hash-named local sources after rechecking
  all three hashes, retained the old archive requests and original staging, and updated
  only requests with no existing import intent. This source-path correction passed
  strict feature-enabled Clippy and all 17 focused Music tests. The signed update
  retained the same fixed test identity. Sequence 6 was added exactly once and its
  Music persistent ID saved. Both initial and later saved-ID queries found the track
  but returned cloud status `unknown` and no local location. Read-only diagnostics
  identified an AppleScript object-specifier coercion error, not an unsupported audio
  format: Music reported an Apple Lossless file track. Fixed the query to explicitly
  get the location before alias/POSIX conversion, and initialized diagnostic outputs
  outside the tell/error scopes. This affects reads, not the already-completed add.
- All three approved tracks have now been added exactly once, with durable IDs saved
  before status. The two later adds were retained through a diagnostic error and
  recovered through status-only calls. All three Music-managed files are under the
  local Music media folder and independently match the originals' full size/SHA-256.
  The cloud originals remain uploaded and were already not downloaded. All three
  tracks initially reported `unknown` and later reported `uploaded`, without another
  add or an explicit cloud-library UI operation. The final acceptance helper rehashed
  all current Music copies after upload and checked only archive metadata, confirming
  matching bytes and uploaded/dataless originals for all three. It wrote a private
  create-only acceptance JSON after every check passed. The latest-source full suite
  passed 1387 tests with 17 ignored and zero failures in 338.09 seconds outside the
  sandbox; strict feature-enabled Clippy, formatting and the production compile
  check also passed. Keep the real-track receipts and copy proofs for later
  production integration, to prevent reimporting these accepted tracks. Production
  has not been deployed or restarted.

## Evidence

- User requested the feature and discussion on 2026-11-11 and confirmed an existing
  iTunes Match subscription/library.
- Apple documents Music file import and the copy-versus-reference setting:
  https://support.apple.com/en-ie/guide/music/mus3081/mac
- Apple documents iTunes Match synchronization:
  https://support.apple.com/en-ie/108935
- Apple documents waiting cloud status and Update Cloud Library:
  https://support.apple.com/en-ie/118287
- Telegram documents hosted and local Bot API download limits:
  https://core.telegram.org/bots/api#getfile
- Telegram documents local server credentials, operation and migration:
  https://github.com/tdlib/telegram-bot-api#usage
  https://github.com/tdlib/telegram-bot-api#moving-a-bot-to-a-local-server
- Telegram documents bot-token MTProto authorization and connections without updates:
  https://core.telegram.org/api/bots
  https://core.telegram.org/method/invokeWithoutUpdates
  https://core.telegram.org/api/updates
- Client startup and file-ID audit references:
  https://github.com/LonamiWebs/Telethon/blob/v1/telethon/client/auth.py
  https://github.com/LonamiWebs/Telethon/blob/v1/telethon/client/telegrambaseclient.py
  https://github.com/LonamiWebs/Telethon/blob/v1/telethon/utils.py
- The official Bot API implementation maps `file_id` to TDLib and downloads it:
  https://github.com/tdlib/telegram-bot-api/blob/master/telegram-bot-api/Client.cpp#L8947-L8954
  https://github.com/tdlib/telegram-bot-api/blob/master/telegram-bot-api/Client.cpp#L16461-L16477
- Apple documents current Mac cloud-library size limits:
  https://support.apple.com/guide/music/musa3dd5209/1.7/mac/27
