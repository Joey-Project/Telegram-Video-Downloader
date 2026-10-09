# macOS Bot Signing

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
