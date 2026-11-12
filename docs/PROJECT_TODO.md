# Project TODO

- [pending] Extend automatic Apple Music import to `.wav` and `.flac` by converting to ALAC, preferring Music's converter where supported. Uploaded `.m4a`, `.mp3` and `.aac` imports, larger-file transfer, original archiving and separate Music/iCloud verification are implemented; see [the music import workstream](project_journal/2026/11/2026-11-11-apple-music-import-ami01.md) for validation and deployment boundaries.
- [pending] Add a file-manager view of temporary directories belonging to interrupted, unfinished downloads, with explicit bulk selection and cleanup. Protect active writers, finished media, queue/control identities and recovery bindings; permanent retention remains the default until the user requests cleanup.
- [pending] Integrate a reusable `BBDown-rust danmaku update` action for existing videos, collections and whole libraries without downloading media again. Refresh must append/merge without losing previously saved comments. The one-time offline recovery is complete; see [the closed recovery workstream](project_journal/2026/10/2026-10-16-bilibili-danmaku-deflate-bdd01.md).
- [pending] Support uploaded `.torrent` files by submitting them to the existing local qBittorrent service.
- [pending] Integrate login status and login operations into the `/settings` Mini App; library settings and migration already exist.
- [pending] Improve reliable association of legacy bare subtitles/covers, portable attachment manifests and interoperability with other libraries' NFO metadata.
- [pending] Follow up on BBDown-rust metadata query limits before response parsing and archive/page expansion; see [the core resolver follow-up](project_journal/2026/10/2026-10-24-library-metadata-recovery-lmr01.md).
- [pending] Investigate prerendering ASS danmaku into graphical subtitle sidecars such as PGS.
- [pending] If YouTube downloads fail because of yt-dlp JavaScript-runtime warnings, configure a supported deno or node runtime.
