# Stable macOS Music automation identities

Music automation has two fixed executable identities on this Mac:

| Profile | Installed executable | Signing identifier |
| --- | --- | --- |
| Production | `$HOME/.local/lib/telegram-video-downloader/bin/telegram-video-downloader` | `io.github.telegram-local-downloader.bot` |
| Test | `$HOME/.local/lib/telegram-video-downloader-test/bin/telegram-video-downloader-test` | `io.github.telegram-local-downloader.bot.test` |

Keep each path, executable name, identifier and designated requirement stable across
updates. Both profiles use the existing trusted local signing certificate documented
in [the bot signing procedure](macos-bot-signing.md). Test and production require
separate Music permissions. A fixed pathname alone does not prove that macOS will
reuse a grant: verify the actual sender and the result after each controlled update.

The fixed bot controls internally generated Music scripts through a bounded system
`osascript` child. A standalone Music grant to Codex, Python, `osascript`, an unsigned
build or a temporary executable is not evidence of a grant to either installed bot.
The macOS build embeds profile-specific `Info.plist`
metadata and `NSAppleEventsUsageDescription`; the test installer signs with the
AppleEvents automation entitlement. An entitlement allows requesting consent; it
does not grant consent.

## Automatic Telegram music imports

On macOS, automatic imports are enabled by default. Other targets default to
disabled; Apple Music and iCloud Music automation are supported only on macOS.
The `[music]` section in `config.example.toml` shows the 512 MiB size limit and
archive location. With the default video library, the private queue state and
download staging live in the hidden
`~/Movies/Downloads/.telegram-video-downloader-music` and
`~/Movies/Downloads/.telegram-video-downloader-staging/music` directories. If the
bot uses a different configured video download root, the defaults follow that
root when configuration is loaded. Changing the selected video library in the
Mini App does not relocate existing Music state or staging.
The original archive defaults to
`~/Library/Mobile Documents/com~apple~CloudDocs/Collection/Music`.
Music resolves bare ffmpeg/ffprobe commands through the bot process's PATH.
Explicit relative codec paths are resolved from the configuration directory.
Missing tools fail the affected Music task without preventing bot startup.

The Python transfer helper is installed only by an explicit operator command. The
default command prints a plan without changing files; select one profile to apply:

```sh
python3 scripts/install_music_runtime.py
python3 scripts/install_music_runtime.py --apply-test
python3 scripts/install_music_runtime.py --apply-production
```

The two apply flags install a private per-profile Python runtime and helper; they
do not deploy or restart a bot. Bot startup never invokes pip. Install the runtime
for the same profile as the fixed executable that will run it. The MTProto app
credentials remain in the private file at
`~/.config/telegram-video-downloader/mtproto.json`; keep its parent directory mode
0700 and file mode 0600.

Send an `.m4a`, `.mp3` or `.aac` audio/document upload to the bot from the allowed
owner's private chat. Group uploads, other chats, unsupported extensions and
files larger than the configured limit are rejected. `/music` shows the task list;
use `/music retry TASK_ID` to resume a failed task or `/music cancel TASK_ID` to
request cancellation. Cancellation and transfer failures keep resumable partial
download data. Cancellation also preserves existing originals and imported
tracks.
When the queue has no safe room for another task, the upload is rejected with a
queue-full message and later Telegram commands remain available. Resend the file
after making room. Lowering the configured upload limit does not discard or
invalidate previously accepted tasks.

Only `/music authorize` requests Music permission. In production, send that command
to the running production bot. The executable checks its fixed installation path
and requests consent through its bounded `osascript` child. Signing integrity and
the designated requirement are verified by the installation procedure. Confirm the
Automation/TCC attribution for the actual request identifies the production bot;
launchd parentage alone does not prove grant reuse. A grant to the test executable
does not grant production access. After granting permission, retry any waiting task
with its task ID. The separate test-runtime authorization flow below starts a
one-shot LaunchAgent with the fixed test executable; it is for controlled test
validation and must not be used as evidence of a production grant. Mute system audio
and verify it is muted before controlled tests, then leave it muted afterward.

The bot stages and validates the original upload, creates a separate original
archive in iCloud Drive, and adds a verified import to Music. M4A and MP3 import
bytes remain unchanged. Raw AAC is losslessly remuxed into an M4A import using audio
stream copy; the original AAC remains the archive source. WAV and FLAC are not
supported. Music's cloud status and the original archive's iCloud upload are
separate checks. The task waits for Music cloud completion and verifies an
independent local Music copy; it also waits for the archive upload to finish before
allowing local archive bytes to be evicted. The cloud archive remains the retained
original. Completed download staging is retained in this first implementation;
archive eviction does not remove those separate staging files. A later explicit
staging-cleanup feature must protect active tasks and Music playback dependencies.

Music must make a separate local media-library copy. If its file-copy setting is
disabled, enable it and consolidate the requested tracks in Music before retrying
the saved task. Verification refuses a playback file that still references the
staging source or original archive; it does not reorganize unrelated library items.

The add operation is durably recorded before it is sent to Music. If its outcome is
ambiguous and no persistent ID was saved, the task is marked `outcome unknown` and
cannot be retried automatically. The bot will not add the track again. If a
persistent ID was saved before a later status or verification failure, retry queries
that saved track and never sends another add. Use `/music` to inspect the task state
before deciding what to do next.

The optional `--music-seed-accepted CONFIG ACCEPTED_PROOF_JSON` command is an
operator-only handover for previously accepted test records. For example:

```sh
"$HOME/.local/lib/telegram-video-downloader-test/bin/telegram-video-downloader-test" \
  --music-seed-accepted /path/to/test-config.toml /path/to/accepted-proof.json
```

It revalidates the private proof, saved native receipt, Music track and uploaded
archive, then seeds completed records into the test queue. It reports `added: false`
and performs no Music add. It is not part of normal startup or automatic import.

## Test installation

Build with the `test-runtime` feature in isolated Cargo target and build directories.
The Cargo artifact keeps the package basename; the installer validates its embedded
test identity before renaming the installed copy:

```sh
CARGO_TARGET_DIR="$PWD/.codex-tmp/music-test/target" \
CARGO_BUILD_BUILD_DIR="$PWD/.codex-tmp/music-test/build" \
cargo build --features test-runtime
python3 scripts/music_test_runtime.py plan --binary "$PWD/.codex-tmp/music-test/target/debug/telegram-video-downloader"
python3 scripts/music_test_runtime.py install --binary "$PWD/.codex-tmp/music-test/target/debug/telegram-video-downloader"
```

Set both `CARGO_TARGET_DIR` and `CARGO_BUILD_BUILD_DIR` for local validation; do not
let the build replace an installed executable. `install` signs and atomically
replaces only the fixed test executable, retains an existing test rollback copy,
and verifies the exact test designated requirement. It never installs or restarts
the production bot. The existing production deployment procedure remains separate.

## Request and verify Music permission

Before tests, mute system audio and verify it remains muted. Leave it muted afterward.
Music must already be running; these diagnostics do not launch it or play audio.

```sh
python3 scripts/music_test_runtime.py check
python3 scripts/music_test_runtime.py status
python3 scripts/music_test_runtime.py authorize
python3 scripts/music_test_runtime.py status
```

The helper loads a one-shot GUI LaunchAgent with label
`io.github.telegram-local-downloader.bot.test.music`. Its program is the fixed test
binary, with either `--music-check` or `--music-authorize`. It does not load a bot
configuration, consume Telegram updates, run downloads or install a persistent bot
service. The completed job stays registered for status inspection; an active or
undecided job is preserved rather than replaced by another request.

`check` uses a nonprompting native AppleEvents permission query. Only explicit
`authorize` can request the Music consent dialog when the system reports that
consent is needed. The prompt runs on a worker thread and waits for the user's
decision. A denied permission is reported without repeated prompting. Normal
permission checks, system-output mute validation and read-only Music queries run
on worker threads with finite deadlines. An unavailable or timed-out mute check
stops the diagnostic before any Music request. `status` includes a bounded
diagnostic tail for a completed run that returned a non-JSON error.

On a busy Mac, set `TELEGRAM_VIDEO_DOWNLOADER_MUSIC_QUERY_TIMEOUT_SECONDS` in the
environment inherited by the fixed executable to change the bounded Music query
deadline. It defaults to 180 seconds and accepts integer values from 5 through
300; an unset, malformed or out-of-range value uses the 180-second default. This
deadline covers nonprompt permission checks, opening the fixed Music bundle when
needed, read-only preflight queries, and normal add/status scripts. AppleScript
gets that timeout, with the owned `osascript` child supervisor allowing an
additional 5 seconds to kill and reap the child.

`TELEGRAM_VIDEO_DOWNLOADER_MUSIC_MUTE_TIMEOUT_SECONDS` sets the separate system
mute-query deadline used by the muted diagnostic preflight. It defaults to 60
seconds and accepts integer values from 5 through 300; invalid or out-of-range
values use the 60-second default. The `osascript` child gets that deadline and
the parent probe allows 5 additional seconds for cleanup. This mute timeout does
not gate normal Music imports. Explicit authorization prompts remain untimed and
wait for the user's decision; these variables do not choose a consent response.

System mute validation uses a fixed volume-only `/usr/bin/osascript` child, with
output caps and a child deadline. Music scripts use a separate owned interpreter,
passed over stdin rather than process arguments. Only internal fixed templates are
exposed; there is no arbitrary-script command. Source is capped at 16 KiB, stdout at
64 KiB and stderr at 8 KiB. Query, quit and import/status have finite deadlines;
timeout or excess output kills and reaps the interpreter, without terminating Music.

Launchd starts the actual sender independently of the interactive Codex subprocess
tree. Confirm the resulting Automation entry or scoped TCC attribution identifies
the test program; launchd parentage by itself is not a public guarantee of TCC's
responsible-process selection.

The controlled test on this Mac recorded a successful Music query and a real
nonpreflight AppleEvents request whose child PID was the interpreter while the
target identity, path and certificate requirement were those of the fixed test
bot. TCC's attribution chain also identified that bot as responsible. No additional
grant was requested. Preserve this distinction between the interpreter and the
identity holding consent when validating later changes.

Successful permission and successful metadata reading are separate results:
`musicPermission` records the permission outcome; `ready` and `musicQuery` record
whether Music responded with version, player state, mute and encoder information.
`musicQueryError` preserves a read failure after a grant. The `check` and `authorize`
operations only inspect permission and state.

Production uses its existing executable path and signing identity. Deploying the
Music-capable production build and requesting its distinct permission requires the
normal explicit production deployment step. Test authorization does not authorize
that production identity.

## Controlled test-session restart

When an updated sender remains authorized but Music queries time out, a normal
Music restart is a controlled recovery experiment. The test-only `quit` action uses
the same fixed sender, existing permission and verified system mute. It sends a
normal quit request with a deadline and preserves cancellation or errors:

```sh
python3 scripts/music_test_runtime.py quit
python3 scripts/music_test_runtime.py status
```

Verify that Music actually exited before explicitly reopening it. This is an
operator-selected test action; the helper does not force termination or run an
automatic restart loop. The production build rejects the quit mode before system
access.

## Three-file native import validation

The test-runtime build also accepts `--music-import REQUEST.json` and
`--music-import-status REQUEST.json`, through the same fixed sender identity:

```sh
python3 scripts/music_test_runtime.py import --request "$HOME/.local/lib/telegram-video-downloader-test/music-import/requests/6.json"
python3 scripts/music_test_runtime.py status
python3 scripts/music_test_runtime.py import-status --request "$HOME/.local/lib/telegram-video-downloader-test/music-import/requests/6.json"
```

Requests must be owned mode-0600 regular files named with a positive sequence number
under the fixed private `music-import/requests` directory. Their fields are
`schema: 1`, `source_path`, `expected_sha256`, and `expected_size`. Prefer the complete
local download: put it at the fixed private `music-import/sources/<sha256>.m4a` path,
with mode 0600 and a private 0700 sources directory. The filename must match the
request's lowercase SHA-256. Archive the original separately in iCloud Drive
`Collection/Music`; adding from the local copy avoids redownloading an archive that
macOS has already evicted. Direct M4A children of that cloud folder remain accepted
for compatibility, but must be materialized before a new import.
Source validation protects both the selected file object and its content: device/inode
checks bind the AppleEvent pathname to the file that was hashed, while size and SHA-256
bind the bytes to the request. Timestamp and directory child-entry changes are not
treated as content changes.

A durable private receipt is written before the single add event. Its returned Music
persistent ID is saved before the separate status query. An ambiguous add is never
repeated automatically. Repeated requests with a known receipt query the saved track
instead of adding another copy. Status reads only that track's persistent ID and does
not open the original or staging source, so a later status check cannot rehydrate an
evicted archive file.

`found` means the track exists locally; inspect `cloud_status` separately for matched,
uploaded, waiting or error outcomes. Verify Music's actual location and independent
local copy before considering archive eviction. This explicit test harness does not
connect production Telegram ingress to automatic imports.

The saved-ID query prefers a file track and explicitly gets its location before
converting the returned alias to a POSIX path. Converting an unevaluated Music property
specifier can fail even for a valid imported ALAC file. Class, kind and location-error
details remain visible; an empty location is not proof that the media copy is missing.
An initial `unknown` cloud status is not cloud success: the first accepted real track
later reported `uploaded` without another add or an explicit cloud-library UI command.
