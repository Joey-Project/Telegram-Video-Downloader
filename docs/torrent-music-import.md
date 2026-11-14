# Torrent downloads and selected Music imports

Send a `.torrent` document to the bot in its authorized private chat. qBittorrent
downloads the entire torrent using its existing save location and category
settings. Music selection is a separate, optional action: the task card shows
download progress and supports folder navigation, file selection and explicit
Music import. A torrent without supported audio remains an ordinary qBittorrent
download.

Use folder selection for an album, or select individual tracks across pages.
Selections can be confirmed while downloading; each chosen file waits until it
is complete before entering Music. Supported audio is `.m4a`, `.mp3`, `.aac`,
`.flac` and supported PCM `.wav`. FLAC/WAV use the existing verified lossless ALAC
converter. Other files remain in the torrent payload and are not imported.
Clearing a selection removes waiting import requests that have not been handed
to Music yet. It does not remove tracks or withdraw an already admitted Music
job; existing Music tasks remain manageable through `/music`.

For a completed selection, the bot verifies the source under the torrent's save
root and copies it into private Music staging. It does not move, hardlink, evict
or delete qBittorrent files. The Music archive retains the selected original
audio. For FLAC/WAV, the ALAC file is a separate import copy; it does not replace
the original in the archive. Music verifies the library copy and configured
archive/cloud conditions before cleaning only that completed Music job's private
staging subtree. Failed or cancelled local jobs retain their Music staging for
inspection and retry. Existing content deduplication prevents another Music add
for content already accepted by that pipeline.

Source checks bind the chosen file's path, object identity, size and content.
Symlinks, traversal, changed files and paths inside Music's managed state,
staging or archive directories are rejected. If the selected qBittorrent file is
missing or its captured object/content has changed, import fails closed and the
payload is left alone. A replacement at the same path does not automatically
replace the saved source proof; retry only after restoring a source that still
matches that proof.

## Configuration

The feature is disabled by default. Enable it in the existing private bot config:

```toml
[torrent]
enabled = true
base_url = "http://127.0.0.1:8080"
```

Keep `telegram.allow_all_chats = false` and authorize only the intended private
owner through `telegram.allowed_chat_ids`. The torrent task itself is private to
that owner. qBittorrent credentials, when needed, belong only in private local
configuration.

Use the existing local qBittorrent WebUI API. If it already allows local clients,
no additional credentials are needed. Otherwise supply its existing username and
password through private configuration; do not put them in version control or a
Telegram message. The bot does not change WebUI authentication, bind addresses or
service settings. The API client supports the documented 4.x/5.x add protocol and
checks the actual task afterward instead of treating HTTP success as admission.
Actual v2/hybrid download support depends on the installed qBittorrent/libtorrent
version; the bot does not upgrade or reconfigure the client.
Selected-audio imports additionally require the existing `[music]` integration
to be enabled and authorized; qBittorrent downloads work independently of Music.

Private task records and metainfo default to
`<video root>/.telegram-video-downloader-torrents`. Metainfo may include private
tracker credentials and is kept as private control data, not exposed in Widget
state. This is separate from the qBittorrent payload save path.

The default metainfo limit is 4 MiB and the file inventory limit is 5,000. These
limits do not cap the total size of the torrent's downloaded content. Each Music
source still observes `music.max_file_bytes` and the Music queue's admission
limits.
API responses default to 2 MiB; `torrent.max_api_response_bytes` can be increased
up to 16 MiB for inventories with unusually long paths.

## Telegram card and recovery

The native card uses [Telegram Widgets](https://core.telegram.org/bots/widgets),
available in private bot chats on supported Telegram clients. It needs no separate
Mini App URL or network access from its HTML. The bot retains the full selection;
the Widget carries a bounded page and short identifiers, not local filesystem paths.
Its caption also exposes task status and the `/torrents` recovery command.

`/torrents` lists retained tasks. Choose a task or use `/torrents TASK_ID` to open a
new card generation while keeping its download/import state. Ordinary retries and
replayed updates do not automatically create another card or another download.

A changed inventory invalidates old selection controls. Refresh before confirming
the new list. Import confirmation also binds the selection shown at confirmation;
another device changing that selection requires a fresh confirmation, while
ordinary download progress does not. Completed sources are rechecked at import
admission and during snapshotting. Missing, replaced or unavailable sources fail
closed without modifying the payload; a retry never silently accepts new bytes or
a replacement object. An uncertain Music add retains the existing
no-blind-reimport protection.

## Cloud ingress and release

Cloud mode requires the matching Worker owner-filter update and a webhook
subscription that includes `widget_query`. Deploy the matching Worker owner
filter that accepts authorized widget queries, then add `widget_query` to the
existing webhook's `allowed_updates` while preserving every other enabled update,
the webhook URL, secret token and pending updates. Never register a second
polling consumer to test a production webhook. The Worker durably stores the
original authorized update and the Mac processes it through its local inbox.

Widget queries are answered promptly; qBittorrent requests, source hashing,
conversion and Music operations run on background workers. Widget refresh is
explicit, and active progress edits go directly from the Mac to the Telegram Bot
API. This feature adds no idle Mini App polling or repeated D1 state snapshots.

Install the Mac change only through the existing fixed-identity signed release
procedure in [macOS bot signing](macos-bot-signing.md), using the production
executable at `$HOME/.local/lib/telegram-video-downloader/bin/telegram-video-downloader`
with identifier `io.github.telegram-local-downloader.bot`. Do not run a Cargo
output binary as the bot. Normal builds/tests do not deploy or restart the
production bot, add real torrents or import tracks into the system Music library.
Rollback must preserve current runtime state; older binaries cannot understand
the new typed local-source Music records and must not rewrite or discard them.
