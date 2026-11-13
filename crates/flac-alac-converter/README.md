# Native FLAC to ALAC converter

A standalone file-only worker with a typed Rust process client. It decodes with
the official libFLAC API, encodes Apple Lossless in M4A through AudioToolbox, then
reopens and decodes the result to verify the complete PCM SHA-256. It does not
launch Music, play audio, import tracks, use Telegram or contact a server.

The bot is not connected to this tool yet. Its eventual iCloud archive must keep
the original upload, not the converted M4A. Match's cloud delivery quality is
independent of this converter's local losslessness guarantee.

## Build and test

The native worker requires macOS, Rust 2024, libFLAC and pkg-config. No FFmpeg,
qaac or external encoder is used by the worker. E2E fixtures use the official
`flac`/`metaflac` CLI, and the independent verifier uses Apple's `afconvert` and
`ffprobe`.

This is a separate executable, not yet a self-contained distribution: the
development build links the installed libFLAC. Bundling, signing and installing
it alongside the production bot belong to the later integration step.

```sh
brew install flac pkg-config ffmpeg
cargo build --locked -p flac-alac-converter
cargo test --locked -p flac-alac-converter --all-targets
cargo test --locked -p flac-alac-converter --no-default-features --lib
```

Use a separate `CARGO_TARGET_DIR` and `CARGO_BUILD_BUILD_DIR` for local builds in
this repository, following the deployment guidelines. Ordinary builds never
install or restart the production bot. Follow the repository's mute policy for
audio-related tests. The test fixtures and worker only read/write audio files.

The client-only library (`default-features = false`) does not link libFLAC or
Apple frameworks. The supervised client requires Unix process/pipe support; the
native codec currently supports only macOS. Missing native dependencies fail at
build time instead of silently selecting another encoder.

## JSON Lines API

Run `flac-alac-converter --json`. Write one request to stdin, followed by a newline:

```json
{"command":"convert","version":1,"input":"/path/日本語 한국어 Русский.flac","output":"/path/日本語 한국어 Русский.m4a","allow_compatibility":true,"timeout_seconds":1800}
```

The output parent must already exist; the output must not exist. Paths may be
relative (resolved against the worker's working directory). Existing files,
including destination symlinks, are never replaced. The input must be a regular
file rather than a symlink. Unicode text is not normalized or replaced.

Each stdout line is a versioned event with `event` equal to `started`, `progress`,
`warning`, `completed`, `failed` or `cancelled`. Diagnostics belong to stderr.
`completed.report` includes source/output audio properties, input/output file
SHA-256, canonical decoded PCM SHA-256, `audio_bit_exact`, metadata migration
details and warnings. Progress frame counts restart for the verification pass;
they are not a single monotonically increasing percentage across both passes.

An optional subsequent line requests cancellation:

```json
{"command":"cancel"}
```

EOF after the first line allows conversion to continue. An unsupported version,
invalid request or damaged audio fails explicitly. Exit status is 0 for a
verified completed conversion, 130 for cooperative cancellation, 124 for the
worker's hard-deadline backstop, and nonzero for other failures. Forced shutdown
may prevent a final JSON event; the client treats missing completion as failure.

The default deadline is 1,800 seconds (valid range 1–86,400). Native callbacks
cooperate with cancellation; an unresponsive native call is terminated after
the deadline plus a five-second grace. `ConverterClient` also enforces its own
process deadline, bounded pipes and a two-second cancellation grace.

```rust,ignore
use flac_alac_converter::{ConverterClient, ConvertRequest};
use std::sync::atomic::AtomicBool;

let client = ConverterClient::new("/fixed/path/flac-alac-converter");
let request = ConvertRequest::new("/music/source.flac", "/music/converted.m4a");
let report = client.convert(&request, &AtomicBool::new(false), &mut |event| {
    eprintln!("{event:?}");
    Ok(())
})?;
assert!(report.audio_bit_exact);
# Ok::<(), anyhow::Error>(())
```

This client is synchronous: invoke it from a blocking worker rather than an
async executor's event-loop thread. Use the process client for isolation and a
hard deadline; an in-process native API call cannot kill a wedged framework.

## Audio and metadata guarantees

- Single-channel and stereo FLAC (4–32 bits), with a known nonzero STREAMINFO sample count.
  Sample rate and channel count are preserved. No automatic downmix or resample
  is performed. Unsupported formats leave the original untouched.
- ALAC depths are 16, 20, 24 and 32 bits. Other supported FLAC precisions may be
  promoted losslessly with a `precision_promoted` warning when compatibility is
  enabled. `allow_compatibility=false` rejects that promotion. A warning never
  permits unexplained PCM differences. No lossy fallback is implemented.
- Canonical PCM is frame-major, channel-interleaved, full-scale left-aligned
  signed 32-bit little-endian. All samples, including the low bits of 32-bit
  sources, participate in the digest. libFLAC frame errors and a present invalid
  STREAMINFO MD5 are errors.
- Known text, sort, track and disc fields map to standard Music/M4A atoms. All
  valid Vorbis comments additionally retain their exact key casing, UTF-8 values
  and per-key value order under the `org.joey.flac-original` freeform namespace.
  Chinese, Japanese, Korean, Russian, French, Italian, mixed scripts, combining
  accents and emoji require no language-specific conversion.
- Cover bytes and picture descriptors are preserved; suitable JPEG/PNG front
  covers also populate standard artwork. Unsupported fields, malformed text,
  opaque blocks and picture data use the separate `org.joey.flac-original.raw`
  archival namespace and warnings.
  The report distinguishes standard mapping, archival preservation and fields
  unavailable as standard Music metadata. It does not claim Music displays all
  preserved information. The original FLAC remains the archival authority.
- Migrated metadata is reopened and compared before publication. The resulting
  ALAC is then decoded again to catch accidental audio damage during tagging.

## Filesystem and resource behavior

The worker creates a private `0700` `.flac-alac-*` directory in the destination
parent, with `0600` source snapshot and output. It verifies source content and
object identity before publishing the completed file atomically without
replacement. Benign File Provider timestamp/flag changes alone do not mean the
original audio changed.

Successful disposable staging is removed. Failed or cancelled staging is
retained for inspection/recovery; the error event includes `staging_directory`
and any existing `partial_output`. A forced kill may leave a directory without
a final event. Before encoding begins there may only be an input snapshot in
the retained staging directory.
Nothing deletes the original. Callers should expose retained staging for later
explicit cleanup rather than automatically deleting failed work.

Limits: 8 GiB source/output; 32 MiB FLAC metadata across 1,024 blocks; 8 MiB parsed
comment data, 8,192 comments and 1 MiB per comment; 24 MiB parsed picture data.
Data that exceeds a subparser limit may be preserved opaquely within the total
budget; exceeding the total budget fails. Protocol limits are 1 MiB per request,
4 MiB per event, 8 MiB total client stdout and 256 KiB stderr. These bounds are
intentional first-version limits, not promises to accept every valid FLAC.

Before tagging, the worker moves only the bounded existing MP4 movie index
(`moov`, at most 64 MiB) to the end and leaves media (`mdat`) bytes and absolute
chunk offsets in place. This avoids the tag library's whole-media buffering
when editing a front-loaded index. Fragmented or ambiguous layouts and oversized
non-media spans are rejected. Audio is decoded and verified after this rewrite.
