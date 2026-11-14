---
id: 20261114-tmi01
title: Torrent downloads and selected Music imports
status: completed
created: 2026-11-14
updated: 2026-11-14
branch: "codex/torrent-music"
pr: ""
supersedes: []
superseded_by: ""
---

# Torrent downloads and selected Music imports

## Agreed scope

- Accept uploaded `.torrent` documents and magnet links and submit them to the existing local
  qBittorrent service. Download every file; Music import is a separate explicit
  selection, not a download-priority selector.
- Let the user browse supported audio by folder and select individual files or
  an album/folder for Music import. Use native Telegram inline buttons with
  pagination and explicit review/confirmation; a separate Mini App is not required.
- Keep the qBittorrent payload unchanged and available for seeding. Snapshot
  selected completed files into private Music staging before using the existing
  lossless conversion, original-audio archive, Music-copy verification, cloud
  synchronization, deduplication and completed-staging cleanup pipeline. The
  archive keeps the selected source; an ALAC conversion is a separate import copy.
- Reuse one task card for progress, selection and results. Persist the torrent
  association and import selections so restart or repeated clicks do not add a
  duplicate torrent or Music track.

## Implementation boundaries

- Mac owns qBittorrent access and file bytes. Cloud ingress carries only Telegram
  metadata and authorized callback queries, using the existing durable inbox.
- Use only supported audio formats already admitted by Music. Never import
  incomplete files, follow an untrusted path outside the selected torrent root,
  or remove/move qBittorrent payloads as Music cache cleanup.
- Card controls must validate the actual Telegram user/chat/message, bind
  selections to a fresh file inventory, answer callbacks promptly and enforce
  Telegram text/callback byte limits with pagination.
- Keep idle Cloud traffic low; button interactions wake the Mac through the
  existing ingress path, and status edits go directly to the Telegram Bot API.
- Ordinary validation uses mock Telegram/qBittorrent/Music and synthetic files.
  Production deployment and real Music imports remain separate controlled steps.

## Implementation

- Added durable qBittorrent torrent tasks, identity reconciliation, full-payload
  download tracking, folder/file selection, revision-bound confirmation and
  restart-safe Music job association.
- Added typed local Music sources. Completed selections are captured and copied
  into private Music staging after rooted source checks; the qBittorrent payload
  remains in place. Local Music jobs use the existing archive, conversion,
  deduplication, import verification and cleanup pipeline without per-track
  Telegram messages.
- Added the private native Telegram card with paginated file/folder selection,
  review pages, stale-card rejection and direct edits from the Mac. Telegram's
  ordinary-bot Widget restriction was confirmed by a minimal live API probe, so
  no production UI depends on Widget availability.
- Magnet submission persists a single add intent and a scoped save path before
  the mutating request. Reconciliation/export validates metadata before selection;
  old local qBittorrent installations can explicitly enable a read-only BT_backup
  fallback. Matching existing tasks stay in place and retain their priorities.
- Bounded filesystem work outside the message receiver, paginated before card
  aggregation, and covered 5,000-file selection without rebuilding full snapshots
  per file. Replayed confirmations and cleared pending selections derive their
  phase from the persisted import ledger.
- Documented setup, existing message/callback ingress, fixed signed Mac release,
  source preservation and recovery limits in `docs/torrent-music-import.md`.

## Initial implementation validation

- The full mocked Telegram-to-qBittorrent-to-Music E2E passed, covering selected
  completed files, private snapshots, original payload retention, status message
  suppression, replay, card generations and restart recovery.
- Cloud 407, UI 94 and Widget 4 tests passed (505 total); TypeScript checks and
  bundle generation passed.
- Full `test-runtime` compilation passed.
- The final Rust suites passed 1,579 tests; 17 pre-existing tests were ignored.
- Final strict Clippy passed.
- Validation used mocked services and synthetic files. No real torrent was
  added and no system Music library was changed.
- A read-only qBittorrent 4.3.9 / Web API 2.8.2 schema check confirmed the required
  torrent and file fields. It did not change a task or service setting.

## Native buttons and magnet acceptance

- The final torrent-focused suite passed 45 tests, including both mocked
  Telegram-to-qBittorrent-to-Music E2Es. qBittorrent's 10 parser/HTTP regressions
  passed, including unknown metadata sizes and absent/null/empty optional paths.
  Strict workspace/all-targets Clippy and formatting passed.
- A broader bot run passed 1,568 tests with 17 existing ignores. Its four failures
  were resolved: the test converter artifact was built and both FLAC/WAV tests
  passed; corrected native-card mock IDs and optional-path parsing made both
  torrent E2Es pass on focused reruns. A single final full-suite rerun was not
  performed.
- Real acceptance used the independent Telegram bot/user and existing local
  qBittorrent 4.3.9. The private sample completed all nine files and remained
  available for seeding. Its original metainfo bytes were preserved exactly.
- The public magnet was submitted without importing its matching `.torrent`.
  qBittorrent first exposed no metadata, then discovered all 48 files and began
  downloading. All file names/sizes matched the supplied torrent; no file was
  skipped. Both actual tasks and their payloads were retained without pausing or
  deleting them.
- Sixteen real callback operations exercised cross-page selection, folder
  selection, paginated review and clearing. Restart preserved the selection and
  original card generation. Test selections were cleared without sending a Music
  import confirmation; real Music import was not repeated during this acceptance.
- Only the fixed signed test executable was updated. Production deployment was
  left unchanged; normal build and validation commands do not deploy the bot.

## Official interface references

- [Telegram Widgets](https://core.telegram.org/bots/widgets)
- [Telegram Bot API](https://core.telegram.org/bots/api#sendwidget)
- [qBittorrent Web API 4.x](https://github.com/qbittorrent/qBittorrent/wiki/WebUI-API-%28qBittorrent-4.1%29)
- [qBittorrent Web API 5.x](https://github.com/qbittorrent/qBittorrent/wiki/WebUI-API-%28qBittorrent-5.0%29)
