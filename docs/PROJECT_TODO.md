# Project TODO

- [pending] 接入 `BBDown-rust danmaku update`：为已有 Bilibili 下载刷新弹幕 sidecar，避免重下媒体文件。
- [pending] 调研 Bilibili 弹幕预渲染：以 ASS 弹幕为中间格式，评估生成播放器可加载的 PGO/PGS 等图形字幕 sidecar。
- [pending] 如 YouTube 下载遇到 yt-dlp JS runtime 警告导致失败，安装并配置 deno 或 node 给 yt-dlp 使用。
- [pending] 增加持久任务队列与显式恢复：保存排队/运行中的任务、已完成条目和 Telegram 消息关联；重启后重新验证计划与已发布文件，再提供 Resume、Retry failed、Cancel，而不是盲目复用不完整 staging 目录。
