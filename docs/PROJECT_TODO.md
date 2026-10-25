# Project TODO

- [pending] Integrate a reusable `BBDown-rust danmaku update` action for existing downloads without downloading media again. The one-time offline recovery and playback acceptance are complete; see [the closed recovery workstream](project_journal/2026/10/2026-10-16-bilibili-danmaku-deflate-bdd01.md).
- [pending] Keep collection videos and related files (NFO, subtitles, danmaku, artwork, etc.) flat within the collection directory where possible, instead of creating a subdirectory per video, to support continuous playback. Preserve unique sidecar names and collection order; include a migration path for existing nested collections without downloading media again.
- [pending] 调研 Bilibili 弹幕预渲染：以 ASS 弹幕为中间格式，评估生成播放器可加载的 PGO/PGS 等图形字幕 sidecar。
- [pending] 如 YouTube 下载遇到 yt-dlp JS runtime 警告导致失败，安装并配置 deno 或 node 给 yt-dlp 使用。
