# macOS Bot Deployment and Signing

## Production Layout and Cache Cleanup

The production executable is installed at:

```text
$HOME/.local/lib/telegram-video-downloader/bin/telegram-video-downloader
```

This is independent of Cargo's `target-dir` and `build-dir`. Explicit
`scripts/launch_agent.sh install` / `deploy` commands own deployment; ordinary
Cargo builds, tests, and cache cleanup do not install or restart the service.
The installer isolates both Cargo output directories, including when a global
Cargo configuration redirects intermediate artifacts to a shared cache.

Existing configuration, authentication files, working directory, environment,
and log paths are preserved when installing over an existing LaunchAgent. They
are outside Cargo output and do not need relocation for `cargo clean -p`.
An existing plist must configure an absolute `StandardErrorPath` so the
installer can observe readiness. Missing, relative, special-file or inaccessible
destinations fail preflight before the running service is stopped; choose its
log destination separately instead of silently changing existing paths during
deployment.
For a first installation, choose a configuration outside Cargo output. Its
relative paths retain their existing meaning; the PDF helper also needs the
Python project and helper resources beside that configuration.

The configuration argument is relative to the LaunchAgent working directory.
Media, authentication and PDF-helper paths are relative to the configuration's
physical parent directory. Configuration HOME expansion follows the preserved
service environment and supports `~`, `$HOME` and `${HOME}`; aliases followed by
`..` retain filesystem resolution semantics. Bare tool names still use the
preserved PATH, while relative command paths retain their task-specific working
directories. The installer does not verify external tool availability.

Queue, completion records, staging, publication locks, receipts, and recovery
metadata intentionally live in the configured media roots. Media paths remain
unchanged. Several locks and recovery records bind device/inode identities and
publication uses same-filesystem atomic renames. Do not move this state into
Application Support or remove it as part of Cargo cache cleanup. Copied lock
backups are recovery evidence; they cannot be blindly substituted for original
inode-bound objects.

Configured configuration, working-directory and media aliases remain unchanged.
The installer binds their physical targets and configuration content, rejecting
retargeting, replacement or configuration edits across the build and snapshot.
Ordinary directory child-entry changes remain allowed. Metadata traversal errors
abort deployment instead of silently omitting unreadable state. Unrelated
symlinked media subdirectories are
skipped; links selected as transactional metadata still fail validation.
Media hardlink anchors such as `output.anchor`
and committed overwrite anchors are recorded by device/inode, size, and link
evidence; their video payloads are neither copied nor charged as metadata.

## Explicit Commands

The macOS installer requires `python3` 3.11 or newer, Apple command-line tools
(`codesign`, `security`, `ditto`, `launchctl`, `lsof`), Cargo for a source build,
and the existing trusted local signing identity. It uses only Python's standard
library and does not install Python dependencies during deployment.

```sh
scripts/launch_agent.sh plan
scripts/launch_agent.sh install
scripts/launch_agent.sh deploy --no-build --binary /path/to/release/telegram-video-downloader
scripts/launch_agent.sh status
```

`plan` is read-only and emits only necessary paths, signature identity, and
state locations. `--binary` selects an input artifact, never the
LaunchAgent execution path. The installer signs a staged copy and leaves the
input artifact untouched. A release build by itself never deploys.

Before the first controlled switch, retain the old signed executable, its
designated requirement, the original plist and configuration, and private state
backups outside Cargo output. Stop both the GUI and legacy user services and
verify their processes have exited before taking the final snapshot. Install
the verified executable with a same-directory atomic rename and atomically
replace the plist. For an existing service, only its executable argument changes.

Require a single registered/running service, the installed executable and
expected working directory, the startup-ready marker, and two successful
Telegram polls from the new process. Startup writes its ready marker and
`telegram polling completed` directly to stderr, independently of `RUST_LOG`,
with distinct `poll=1` and `poll=2` markers for its first two successful polls;
these logs contain no token, URL, or message
payload. Check queue/completion readability and preserved ownership/permissions.
Do not use a second `getUpdates` consumer to test polling.

Process checks cover this user's service and download workers, independently of
other users' installations. Working-directory checks compare directory identity.
Readiness scanning advances through bounded log chunks so verbose output does
not hide later markers; observed rotation or truncation fails verification.

For a first installation, the installer validates an explicit .NET root or
discovers the runtime through `dotnet --list-runtimes` and the standard private
and system locations. It sets the runtime-root variables for BBDown apphosts
when a root is found. Existing LaunchAgent environments are preserved.

The installer's retained backup path is its rollback handle:

```sh
scripts/launch_agent.sh rollback --backup /path/printed/by/the/installer
```

Rollback restores the previous executable and plist after stopping the current
service. It preserves current runtime state instead of rewinding queues or
authentication files that may have advanced since deployment. If state repair
is needed, stop the service, retain both versions, and perform a separate
validated repair respecting lock identities.

Default rollback keeps the restored executable at the fixed installation path,
so it also works after Cargo output has been removed. It does not prove that the
restored process reached startup-ready. Both rollback modes validate and capture
the saved plist, service targets and previous registrations, and verify the
previous executable before stopping any service.
If the new path needs a privacy grant
that the user declines, explicitly restore the original entrypoint instead:

```sh
scripts/launch_agent.sh rollback --backup /path/printed/by/the/installer --original-entrypoint
```

This option requires the original executable to still exist and match the saved
bytes and designated requirement. It verifies those conditions before stopping
the service and restores the original plist bytes. It fails before stopping if
the original executable was cleaned or changed. Runtime state remains in place.

Moving an executable can trigger a fresh macOS Documents request even with the
same certificate and identifier. Finish that system prompt explicitly. Stopping
the requesting PID or restoring a signature does not itself finish a pending
prompt. If the user declines the new grant, restore the original entrypoint,
check startup-ready again, and report any remaining coordinator/authorization
blocker without claiming polling resumed. Never reset TCC or broaden access.

After successful validation, Cargo output can be cleaned independently. Keep
the installed executable, rollback backups, configuration, authentication data,
logs, and media-associated state. This workflow does not run cache cleanup.

## Local Cutover Record (2026-10-19)

- Production destination: `/Users/joey/.local/lib/telegram-video-downloader/bin/telegram-video-downloader`.
- Existing config: `/Users/joey/Program/Codex-workspace/Telegram-Video-Downloader/config.toml`.
- Existing working directory: `/Users/joey/Program/Codex-workspace/Telegram-Video-Downloader`.
- Existing logs: `/Users/joey/Library/Logs/TelegramVideoDownloader/`.
- Media roots remain `/Users/joey/Movies/Downloads` and
  `/Users/joey/Documents/Downloads`; existing authentication state remains in
  `/Users/joey/.local/state/telegram-video-downloader/`.
- Retained rollback/state metadata backup:
  `/Users/joey/.local/lib/telegram-video-downloader/backups/20261019T103210.929774Z`.
  Payloads in media staging stay in place; the backup contains configuration,
  authentication, queue, completion, lock, and recovery metadata, not downloaded
  media payloads. Copied metadata retains owner/group/mode and content hashes;
  the manifest records original device/inode identities.
- The fixed-path process requested `kTCCServiceSystemPolicyDocumentsFolder`.
  Joey declined granting new-path access and requested restoring the original
  entrypoint. The original executable still matches the saved SHA-256 and
  designated requirement; the original plist has been restored. New-path
  activation is deferred and its two successful poll markers were not observed.
- Original-path startup verification is still pending: the restored process is
  waiting in `NSFileCoordinator`; no startup-ready/polling claim is made.
  Both queue indexes remain readable, including all 19 video records (14
  completed, 5 cancelled), with no missing indexed record.

**The current service still depends on the original Cargo release output. Do
not clean that output until a later authorized cutover passes runtime checks.**

## Why Signing Is Required

The canonical macOS LaunchAgent uses the release executable with identifier
`io.github.telegram-local-downloader.bot`. macOS privacy permissions validate
code requirements, not just the executable's filesystem path.

Cargo emits a linker-signed ad-hoc executable. Rebuilding directly into the
canonical target directory replaces the stable application signature. A worktree
using the same `CARGO_TARGET_DIR` can do this too. Git commit signing does not
restore the Mach-O application signature.

On 2026-10-09, TCC reported that the rebuilt bot did not match its existing
Documents authorization requirement. The background process waited in
`NSFileCoordinator` before its read accessor, while independent directory-read
probes succeeded. Pinning and recursive materialization did not remove that wait.

## Deployment Gate

1. Resolve the existing LaunchAgent's binary, configuration, and working
   directory. Record `codesign -dr - <canonical-binary>` before building and retain
   a rollback copy outside any target directory the build may overwrite.
2. Resolve the existing signing identity with
   `security find-identity -v -p codesigning`. Match its certificate to the
   recorded requirement. Do not create a replacement certificate or choose a
   different identity when the original is unavailable.
3. Build and validate the code in a target path that does not overwrite the live
   LaunchAgent executable. Stage a copy of the release executable and restore the
   original certificate signature and fixed identifier before installation.
4. Run `codesign --verify --strict --verbose=2` and verify the staged executable
   against the recorded requirement with `--test-requirement`. Inspect
   `codesign -dr -` to ensure the identity is stable rather than a lone `cdhash`.
5. Atomically install the signed executable at the canonical path and restart the
   existing LaunchAgent. Confirm a single instance, the
   `telegram local downloader started` log, and successful Telegram polling.
6. If startup still waits, inspect bounded TCC logs for this executable/PID and
   relevant request IDs. Distinguish Documents permission from Full Disk Access.
   A stale grant tied to an old `cdhash` is not repaired merely by restoring a
   certificate signature. Report the exact remaining authorization gate and let
   Joey approve any privacy-permission change; do not reset TCC or edit its database.

Restoring a valid signature does not itself complete an authorization prompt that
was already pending. On 2026-10-09, the earlier Documents request completed only
after Joey approved it; the existing signed process reached startup-ready two
seconds later, without moving the executable or changing its working directory.
Check the specific request's result and the runtime log before restarting again.

## Current Local Identity

The verified identity on Joey's current Mac is `Telegram Video Downloader Local
Signing`, certificate fingerprint `C24FE0C91539BD95BA4081CD944A8E17053FB304`.
Resolve and match it again on each host; this fingerprint is public identity
metadata, not a private key. The existing Documents requirement is:

```text
identifier "io.github.telegram-local-downloader.bot" and certificate root = H"c24fe0c91539bd95ba4081cd944a8e17053fb304"
```

After resolving the identity and choosing a staged release path, the current-host
signing and verification commands are:

```sh
codesign --force --sign C24FE0C91539BD95BA4081CD944A8E17053FB304 --identifier io.github.telegram-local-downloader.bot "$staged_binary"
codesign --verify --strict --verbose=2 --test-requirement '=identifier "io.github.telegram-local-downloader.bot" and certificate root = H"c24fe0c91539bd95ba4081cd944a8e17053fb304"' "$staged_binary"
codesign -dr - "$staged_binary"
```

Use narrow non-sandbox execution when macOS signing, certificate lookup, or
LaunchAgent operations require it. Keep rollback files and diagnostic output in
a task-scoped temporary directory, and clean up when recovery no longer needs them.
