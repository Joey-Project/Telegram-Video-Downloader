# Torrent downloads and selected Music imports

Send a `.torrent` document or a magnet URI (also `/magnet LINK`) to the bot in its authorized private chat. qBittorrent
downloads the entire torrent using its existing save location and category
settings. Music selection is a separate, optional action: the task card shows
download progress and supports folder navigation, file selection and explicit
Music import. A torrent without supported audio remains an ordinary qBittorrent
download.

`/torrents` opens a paginated catalog of existing qBittorrent tasks, including
per-task progress. On a task, tap **Choose music** to adopt that exact task in the bot's
local records and open its existing selection card. Adoption reads and validates
the selected task; it does not submit another torrent, change its save location
or file priorities, or move or delete payload files. **Pause** and **Resume** act
on only the selected qBittorrent task. Pausing temporarily stops its download or
seeding activity; resuming returns it to qBittorrent's existing task settings.
Bot-submitted torrents request every file; adopting an existing qBittorrent task
preserves its current priorities. Music selection does not change those
priorities, and the payload remains available for seeding when the task is active.

The catalog list is a private, in-memory view bound to the authorized owner and
the Telegram list message. Its callback session expires after one hour idle and the
process retains at most 48 sessions; a new `/torrents` request creates a fresh
list message, and its **Refresh** button updates that message. The catalog view
itself is not durable. Bot task records and Music selections are saved locally,
so `/torrents saved` lists those retained records even when qBittorrent is
unavailable. Use `/torrents TASK_ID` to reopen a saved task card. Catalog actions
do not submit another torrent after an uncertain result. If a pause or resume
result cannot be confirmed, send `/torrents` to read the current state before
acting again.

New magnet tasks use `<qBittorrent default save path>/Telegram Downloads/<task ID>`
until their metadata can be validated. The bot removes magnet `so` download-selection
parameters so every file downloads. Metadata is exported from qBittorrent and checked
against the magnet identity before any Music selection becomes available. A matching
existing task is associated in place without changing its path or file priorities.
After an uncertain add response, the bot reconciles by identity instead of posting
another add. Private torrents should be supplied as their original `.torrent` files;
the bot does not convert them to public-discovery magnets.

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
Magnet selection uses the metainfo export API available in qBittorrent 4.5 and
newer. For an older local installation, explicitly configure `torrent.metainfo_dir`
to its `BT_backup` directory (on macOS usually
`~/Library/Application Support/qBittorrent/BT_backup`). This fallback reads only the
hash-named `.torrent`, rejects symbolic links, enforces the metainfo size limit and
verifies its parsed identity before showing selectable files. It never writes to
the backup directory or uses an unrelated uploaded torrent to seed metadata.
Selected-audio imports additionally require the existing `[music]` integration
to be enabled and authorized; qBittorrent downloads work independently of Music.

Private task records and metainfo default to
`<video root>/.telegram-video-downloader-torrents`. Metainfo may include private
tracker credentials and is kept as private control data, not exposed in Telegram card
state. This is separate from the qBittorrent payload save path.

The default metainfo limit is 4 MiB and the file inventory limit is 5,000. These
limits do not cap the total size of the torrent's downloaded content. Each Music
source still observes `music.max_file_bytes` and the Music queue's admission
limits.
API responses default to 2 MiB; `torrent.max_api_response_bytes` can be increased
up to 16 MiB for inventories with unusually long paths.

## Telegram card and recovery

The card uses ordinary Telegram messages and inline keyboard buttons. Four rows per
page support individual track selection, folder selection, navigation, clearing,
and a separate paginated review with explicit import confirmation. The Mac retains
the selection across pages. Buttons carry short task identifiers and revisions;
neither absolute paths nor tracker URLs are sent. No Mini App is needed.

Telegram currently restricts Widgets for ordinary bots, including private chats.
The live API returned `INPUT_MEDIA_WIDGET_NOT_ALLOWED`; the restriction is also
[confirmed by Telegram's maintainer](https://github.com/tdlib/telegram-bot-api/issues/947).

`/torrents` browses the qBittorrent task catalog eight tasks per page (up to
20,000 tasks). Its summary shows
task name, state, progress and size without exposing save paths. **Choose music**
opens the native selection card for that existing task; the card then supports
folder/file selection, review and explicit import confirmation. It does not add
the torrent again. Use `/torrents saved` to browse durable bot task records, or
`/torrents TASK_ID` to open a new card generation while keeping that task's
download/import state. The catalog list and callback revisions are ephemeral;
the adopted task association and saved Music selection survive restart.
Pagination and refresh are explicit, with no idle qBittorrent polling. Ordinary
retries and replayed updates do not automatically create another card or another
download.

A changed inventory invalidates old selection controls. Refresh before confirming
the new list. Import confirmation also binds the selection shown at confirmation;
another device changing that selection requires a fresh confirmation, while
ordinary download progress does not. Completed sources are rechecked at import
admission and during snapshotting. Missing, replaced or unavailable sources fail
closed without modifying the payload; a retry never silently accepts new bytes or
a replacement object. An uncertain Music add retains the existing
no-blind-reimport protection.

After an interrupted Music handoff, the bot first looks up existing jobs by
their saved source keys. It retains any job it finds, including when the file
was later deselected. If no job exists, a still-selected file requires a fresh
review and confirmation; a cleared selection is retired without importing.
Newly completed files do not change the identity of an interrupted batch.
Status refreshes use bounded requests even for large, long-lived selections,
and retain known completed results when old Music job records have been pruned.

## Cloud ingress and release

Cloud mode uses the existing authorized `message` and `callback_query` ingress.
Preserve the existing webhook URL, secret token and pending updates. Never register a second
polling consumer to test a production webhook. The Worker durably stores the
original authorized update and the Mac processes it through its local inbox.

Callback queries are answered promptly; qBittorrent requests, source hashing,
conversion and Music operations run on background workers. Card refresh is
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
