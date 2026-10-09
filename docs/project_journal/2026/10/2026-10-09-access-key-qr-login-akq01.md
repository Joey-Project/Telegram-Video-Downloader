---
id: 20261009-akq01
title: Automatic Access-Key QR Authorization
status: completed
created: 2026-10-09
updated: 2026-10-09
branch: wip/access-key-qr-login
pr: null
supersedes: []
superseded_by:
---

# Automatic Access-Key QR Authorization

## Problem

- The previous `/bbdown login access-key` flow sent a BiliPlus landing-page QR and
  expected users to copy a callback URL or BALH credential message into Telegram.
- The live BiliPlus page uses `getauth` and `authpoll`, saves the authorization
  result internally, and redirects. It does not display the expected callback.

## Implementation

- The bot obtains the actual Bilibili authorization QR from BiliPlus and polls the
  provider until scan, confirmation, expiry, or success. A successful result saves
  the generic access key through the existing authentication transaction.
- Existing credentials are not sent during QR acquisition or polling. Polling
  posts the short-lived authorization code in the request body. Redirects are
  disabled, authorization QR origins are checked, and response bodies are capped
  at 64 KiB. Provider response bodies and credential values are excluded from errors.
- The private-chat login ownership marker blocks competing logins and manual
  callback processing. Success, timeout, expiry, failure, and logout clean up the
  matching ticket. Existing generation and cross-process epoch checks prevent
  stale login completion from restoring credentials after logout.
- Web-cookie and TV login behavior is preserved. The bot still depends on BiliPlus
  for generic access-key acquisition; automatic login does not establish that a
  public resolver can serve a particular episode.

## Validation

- Live protocol preflight on 2026-10-09: `getauth` returned code `0`, an
  authorization code, and a QR URL on `https://passport.bilibili.com`; one unscanned
  `authpoll` returned `86039`. No existing account credentials were sent and no
  account login was completed.
- Five provider mock tests cover automatic acquisition/polling, generic-key
  mapping, request method and body, cookie exclusion, scan/confirm/expiry states,
  untrusted QR origins, redirects, malformed/oversized responses, and error redaction.
- The pending-ticket regression covers callback exclusion and exact-ticket cleanup.
- The Telegram authorization-link regression verifies delivery preserves the QR
  link while ordinary logs contain only a fixed redacted placeholder.
- Synthetic credential fixture: `joey-private-v3`, `access-a`.
- `cargo clippy --all-targets --locked --offline -- -D warnings`: passed.
- `cargo test --all-targets --locked --offline --quiet`: 544 passed, 11 ignored,
  zero failures (555 total).
- `cargo build --release --locked --offline`: passed with Cargo 1.95.0.
- `cargo fmt --all -- --check`, `git diff --check`, and project-journal
  validation: passed.
- Local review was explicitly omitted for this task.

## Runtime Deployment

- Built from the worktree into the existing canonical checkout's release target.
  The existing LaunchAgent continued to use the canonical binary, config, and
  working directory. Process inventory confirmed only one bot instance.
- The updated binary did not reach `telegram local downloader started`. A
  bounded process sample showed `QueueManager::open` waiting inside macOS
  `NSFileCoordinator` and `__open` while coordinating a download-root read.
  One restart reproduced the same startup block.
- Restored the pre-update binary and restarted the same LaunchAgent. A second
  bounded sample showed the identical directory-coordination block in that
  original binary as well. The restored process exists but is not verified ready.
- The access-key fix is committed and locally validated. Runtime deployment and
  real scan/confirmation remain blocked by the download-root startup issue.
  No logout, credential deletion, task cleanup, or resolver configuration change
  was performed.
- The fixed binary and bounded validation/sample artifacts are retained in
  `/private/tmp/telegram-access-key-flow` for recovery.

## Download-Root Pinning

- On 2026-10-09, Joey authorized pinning the configured download roots. The video
  root `/Users/joey/Movies/Downloads` has no managed File Provider item according
  to `fileproviderctl evaluate`; the PDF root is managed by File Provider.
- The PDF root was initially downloaded but not kept downloaded, and was not
  recursively downloaded. After checking the implementation in
  [icloud-tools Pinner.swift](https://github.com/icanhasjonas/icloud-tools/blob/main/Sources/icloud/Core/Pinner.swift),
  wrote the File Provider pin marker `com.apple.fileprovider.pinned#PX` with byte
  `0x31` to `/Users/joey/Documents/Downloads` only. No third-party tool was installed.
- System verification with `fileproviderctl evaluate` confirmed
  `isKeepDownloaded = 1`, followed by `isRecursivelyDownloaded = 1`. The hidden
  `.telegram-video-downloader-queue` directory also reports both flags as `1`
  through the inherited pin. File contents and access permissions were not edited.
- Read-only API research used GPT-6 Luna at high reasoning. Public File Provider
  download requests do not provide an equivalent client-facing pin setter; the
  pin marker is an implementation detail, and the actual system state was verified.
- Re-deployed the previously validated fixed binary to the canonical release
  path and restarted the existing LaunchAgent. A final restart after recursive
  downloading completed still did not reach the startup-ready log. A one-second
  process sample confirmed the same `QueueManager::open` -> `NSFileCoordinator`
  -> `__open` wait. Pinning and materialization are complete; the remaining
  startup-coordination block is unresolved. The canonical release path currently
  contains the fixed binary; the original binary and bounded diagnostic samples
  remain in the task-scoped temporary directory for recovery.

## Signing Diagnosis and Deployment Policy

- Independent read-only probes opened both download roots and completed default
  `NSFileCoordinator` reads from background threads immediately, including file
  URLs with the same non-directory hint as the Rust wrapper. This did not reproduce
  the bot's process-specific wait and reduced the likelihood of a path, URL-hint,
  materialization, or queue-owner-lock explanation.
- Scoped TCC logs on 2026-10-09 reported a Documents code-requirement mismatch
  after the release build. The trusted original used identifier
  `io.github.telegram-local-downloader.bot` and certificate root fingerprint
  `C24FE0C91539BD95BA4081CD944A8E17053FB304`; the rebuilt release instead used
  linker ad-hoc signing with a `cdhash`-only requirement.
- Restored the existing `Telegram Video Downloader Local Signing` identity on
  a staged copy. `codesign --verify --strict --verbose=2 --test-requirement`
  confirmed signature integrity and the exact existing identifier/certificate
  requirement before atomic deployment to the canonical path.
- The restarted canonical process is recognized by TCC under the original
  identifier. Startup is still unverified. TCC also retains a Full Disk Access
  grant tied to an old `cdhash`; the initial Documents prompting request has no
  completion in the scoped evidence. These observations do not prove that signing
  alone resolves every remaining access wait. No TCC reset, database modification,
  or broader privacy grant was performed.
- Added repo-level `AGENTS.md` and `docs/macos-bot-signing.md`: preserve the existing
  identity, isolate release build output from the live executable, sign and verify
  before deployment, and require readiness evidence after restarting. Documentation
  validation used `git diff --check`, project-journal validation, and existence
  checks for the new Markdown links. Runtime code and test fixtures were unchanged.

## Runtime Recovery

- Joey completed the macOS authorization prompt. The previously pending
  Documents request `55269.99` returned `authValue=2` without an error at
  2026-10-09 16:13:33 UTC. The existing signed process then logged
  `telegram local downloader started` at 16:13:35 UTC, without another restart
  or any change to its binary location, working directory, or download roots.
- The canonical LaunchAgent has one instance, PID `89764`. Its TCP connection to
  Telegram is established, and the scoped startup evidence contains no subsequent
  polling errors. This confirms that the pending privacy authorization was the
  remaining startup gate; directory pinning alone could not finish that request.
- Non-sandbox `codesign --verify --strict --verbose=2 --test-requirement` confirms
  the deployed binary matches the original identifier/certificate requirement.
  The sandbox-only certificate trust error was not a bad application signature.
- Automatic access-key QR login was verified with Joey's actual account at
  2026-10-09 16:31 UTC: the bot observed scanning and saved the access-key after
  app confirmation, without manual callback extraction.
- The original binary and bounded build/test/sample evidence remain in
  `/private/tmp/telegram-access-key-flow` for rollback and follow-up. The separate
  directory-coordination probe and generated Swift module cache were cleaned up.

## Credential-Backed Bangumi Follow-Up

- At 16:31:46 UTC, the new `md1376` latest-episode job failed during plan probing
  with API code `-10403` (region unavailable). The live bot configuration has no
  restricted-area API proxies configured, so this job used the official API.
- At 16:33:54 UTC, an ordinary Bilibili video completed downloading, muxing,
  publication, and verification. Directory access and the ordinary download
  pipeline are operational.
- After Joey explicitly authorized access-key egress to `https://bili.lli.cx`,
  a bounded probe used the newly saved generic access-key, without forwarding
  Web cookies or following redirects. For `ep247472`, both `/pgc/player/web/playurl`
  and `/pgc/player/web/v2/playurl` returned `-10403` in all three configured probe
  areas (`cn`, `hk`, `tw`), with no playable streams. Official anonymous season
  metadata returned all 203 episodes successfully.
- Earlier success recorded for this proxy concerned a different series and
  episode. These results do not establish that the proxy can resolve `md1376`.
  No ineffective proxy configuration was installed and the service was not
  restarted during this follow-up.

## Other Public Resolver Probes

- Joey requested testing other servers in the stored proxy catalog. Anonymous
  probes covered 19 other hosts, excluding the previously tested `bili.lli.cx`
  and the SEA-only entry. The initial pass made 23 requests; a mainland/Taiwan
  follow-up to `bilibili.suysker.xyz` made four more. No playable streams were
  found in these anonymous responses.
- `atri.ink` explicitly required `access_key`; `bili.nepnep.moe` and
  `bstar.kirara-fantasia.moe` reported that the account was not logged in.
  `bilibili.suysker.xyz` reported a region restriction for Hong Kong/Taiwan and
  an unsigned-in account for mainland China. These are credential-gated
  candidates, not verified working resolvers for this episode.
- Other responses included a disabled Web API, HTTP 404/567, invalid JSON,
  redirects, and transport failures. Transport failures were recorded without
  asserting that every failure was DNS-related. Credential egress to these
  hosts was not performed during the anonymous phase; the existing authorization
  then covered only `bili.lli.cx`.
- Each request was bounded to eight seconds and 1 MiB, redirects were disabled,
  concurrency was capped at four, and the initial run had a 180-second deadline.
  Anonymous probe scripts and compact JSON reports were retained under
  `/private/tmp/telegram-other-resolvers-20261009` for follow-up.

## Authorized Resolver Verification

- Joey explicitly authorized login-information egress to all four nominated
  hosts. The saved generic access-key was sent only to those exact HTTPS hosts,
  without Web cookies, redirects, or media downloads.
- A 24-request probe tested both Web routes in `cn`, `hk`, and `tw`. The
  `/pgc/player/web/playurl` route in `cn` returned code zero with 19 DASH video
  streams and three audio streams for each of `atri.ink`, `bili.nepnep.moe`,
  `bstar.kirara-fantasia.moe`, and `bilibili.suysker.xyz`.
- `bili.nepnep.moe` also returned streams for the HK/TW query variants. The
  other three hosts returned region errors for HK/TW. The v2 routes were
  unsupported or returned no playable streams; code zero alone was not counted
  as a playable result.
- The cached `bbdown-core` release library also passed actual `plan_download`
  calls for the media URL with `Selection::Latest` against each mainland proxy:
  one entry, `ep247472`, `StreamSource::PgcProxy`, 19 videos, three audios.
  A temporary loopback relay enforced the exact episode/area/route allowlist and
  no-redirect credential forwarding because the library's default HTTP client
  otherwise follows redirects. Credentials stayed in the relay's memory; the
  library received the original upstream JSON and did not download media.
- The verified configuration is Web mode, mainland area, and a restricted API
  proxy such as `cn=https://atri.ink`, with `cn=https://bili.nepnep.moe` as an
  optional fallback. These validation runs did not install runtime configuration
  or restart the LaunchAgent.

## Runtime Proxy Configuration

- Joey authorized applying the verified settings. The canonical `config.toml`
  now has Web playurl mode, area `cn`, and ordered restricted API proxies
  `cn=https://atri.ink` and `cn=https://bili.nepnep.moe`. TOML parsing and comparison
  confirmed that all other configuration settings were preserved.
- A concurrent video-fix deployment changed the canonical executable during
  preflight. The signing gate caught its temporary ad-hoc signature; that
  deployment then restored the original certificate and identifier. Non-sandbox
  strict verification against the original requirement passed before this
  workstream restarted the service. This workstream did not rebuild or replace
  the other deployment's executable.
- The existing GUI LaunchAgent was restarted once to load the proxy settings.
  It logged `telegram local downloader started` at 2026-10-09 17:20:57 UTC
  (18:20:57 Europe/London). Exactly one bot process was observed, PID `93790`,
  with its canonical config path and an established Telegram HTTPS connection.
- A follow-up more than one minute later showed the same PID and run count,
  the Telegram connection still established, and no new polling/startup errors.
  The temporary configuration rollback copy was removed after verification;
  login state and the existing download queue were preserved.
