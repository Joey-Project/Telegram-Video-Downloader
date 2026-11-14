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

- Accept uploaded `.torrent` documents and submit them to the existing local
  qBittorrent service. Download every file; Music import is a separate explicit
  selection, not a download-priority selector.
- Let the user browse supported audio by folder and select individual files or
  an album/folder for Music import. Prefer Telegram's new message-embedded HTML
  Widgets after checking current official capabilities; a separate Mini App is
  not a prerequisite.
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
  metadata and authorized widget queries, using the existing durable inbox.
- Use only supported audio formats already admitted by Music. Never import
  incomplete files, follow an untrusted path outside the selected torrent root,
  or remove/move qBittorrent payloads as Music cache cleanup.
- Widget controls must validate the actual Telegram user/chat/message, bind
  selections to a fresh file inventory, answer within the Bot API deadline and
  enforce documented HTML/state/query byte budgets with pagination.
- Keep idle Cloud traffic low; widget interactions wake the Mac through the
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
- Added the private Telegram Widget card and query handling, including authorized
  Cloud Worker ingress, bounded state and queries, stale-card rejection and
  direct card edits from the Mac.
- Bounded filesystem work outside the message receiver, paginated before card
  aggregation, and covered 5,000-file selection without rebuilding full snapshots
  per file. Replayed confirmations and cleared pending selections derive their
  phase from the persisted import ledger.
- Documented setup, webhook/Worker release ordering, fixed signed Mac release,
  source preservation and recovery limits in `docs/torrent-music-import.md`.

## Validation evidence

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

## Official interface references

- [Telegram Widgets](https://core.telegram.org/bots/widgets)
- [Telegram Bot API](https://core.telegram.org/bots/api#sendwidget)
- [qBittorrent Web API 4.x](https://github.com/qbittorrent/qBittorrent/wiki/WebUI-API-%28qBittorrent-4.1%29)
- [qBittorrent Web API 5.x](https://github.com/qbittorrent/qBittorrent/wiki/WebUI-API-%28qBittorrent-5.0%29)
