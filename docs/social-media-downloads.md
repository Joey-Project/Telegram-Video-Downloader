# 社交媒体下载与原文件回复

在 Bot 的所有者私聊中直接发送链接。Instagram 支持帖子和轮播、Reel、单条 Story、指向单条媒体的 Highlight 分享链接，以及用 https://www.instagram.com/USERNAME/ 取得头像。X 支持包含视频的 x.com 和 twitter.com 帖子。Bot 将下载到的文件以 Telegram 文档回复原消息。图片不做 Telegram 照片压缩。X 的视频和音频在需要时使用 FFmpeg 封装到同一个文件。

## Mac 配置

- 将 Telegram 的 allowed_chat_ids 配置为一个私聊用户 ID，allow_all_chats 设为 false。
- 配置 [tools] 中的 yt_dlp 和 gallery_dl。Instagram 使用 gallery-dl 1.32.21 验证过的浏览器 Cookie 读取和提取能力。LaunchAgent 可使用这两个工具及 FFmpeg 的绝对路径。
- Instagram 会话从 ~/.config/telegram-video-downloader/instagram-profiles.json 读取。文件权限为 0600，父目录为 0700。配置 primary 和可选 fallback 各自的 cookies_from_browser，值的格式为 chrome:/absolute/path/to/Chrome/Profile。使用独立浏览器目录完成登录。Bot 优先使用 primary。遇到账号访问或登录问题时试 fallback。用户端限流会直接报告。
- 回传复用当前 Bot 的 token，以及 ~/.config/telegram-video-downloader/mtproto.json 中的应用凭据和固定安装目录中的私有 Music Python Runtime。参见 [运行时安装](macos-music-automation.md)。Music 自动导入开关可以保持关闭。Cloudflare 接收原始 Telegram 更新，下载和发送文件均在 Mac 执行。
- 上传最多使用 4000 个 512 KiB 分片，即 2,097,152,000 字节，约 2 GB。超过上限时仍可在 Mac 取用原文件。见 [Telegram 文件上传文档](https://core.telegram.org/api/files)。

## 存储和重试

每个任务将完整原始文件发布到所选下载位置下的 .telegram-video-downloader-social/TASK_ID。上传失败时，可从 /queue 重试，同一 Bot 上的任务会复用已验证的文件和逐文件发送回执。超过上传上限的文件会留在 Mac，其余文件会继续发送。未完成的任务保留在原媒体库。下载失败或中断时，片段留在本媒体库根目录的 .telegram-video-downloader-social-staging 中。File Manager 的 Video staging 清理目前只处理它能识别的常规视频任务。
