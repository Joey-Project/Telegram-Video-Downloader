# Project TODO

- [pending] 下载已存在相同视频时增加交互选择：覆盖、两者并存、取消；覆盖和并存都应先下载到 staging 目录，确认新文件完整后再移动/替换。
- [pending] 正在进行的下载任务使用 Telegram edit-message 实时更新进度，保留足够日志帮助判断下载、合并、idle 等阶段。
- [pending] 继续追查 BBDown 下载 stall/合并阶段问题，基于现有失败摘要和进度日志做可复现 debug。
- [pending] 完善 Bilibili 专栏/opus PDF archive 行为。
- [pending] 如 YouTube 下载遇到 yt-dlp JS runtime 警告导致失败，安装并配置 deno 或 node 给 yt-dlp 使用。
