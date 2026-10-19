# Repository Guidelines

## macOS Bot Deployment

- Install production executables at
  `$HOME/.local/lib/telegram-video-downloader/bin/telegram-video-downloader`
  using the explicit deployment command. Never run the LaunchAgent from Cargo
  output directories; ordinary builds and tests must not deploy or restart it.
- Queue, completion records, staging, publication locks, and recovery metadata
  intentionally live beside the media. Preserve their paths and inode bindings.
  Keep existing configuration, authentication state, working directory, and logs
  in place when they are outside Cargo output; do not relocate them for cache cleanup.
- Retain rollback binaries, plist/configuration copies, and state backups outside
  Cargo output before a controlled switch. Do not blindly restore inode-bound
  lock state from copied backups; follow the deployment documentation.
- Follow [the signing procedure](docs/macos-bot-signing.md) before deploying or
  restarting the macOS LaunchAgent.
- Record the canonical executable's existing designated requirement before a
  build. Cargo replaces its signature, including when a worktree builds into a
  shared canonical `CARGO_TARGET_DIR`.
- Build releases into a separate target/staging path; do not let Cargo overwrite
  the live LaunchAgent executable before the signing and installation gate.
- Sign the staged release with the existing trusted certificate and identifier
  `io.github.telegram-local-downloader.bot`. Do not substitute ad-hoc signing,
  another certificate, or another identifier.
- Verify signature integrity and the recorded designated requirement before
  replacing the executable or restarting the existing service.
- A running PID is insufficient: require the startup-ready log and continued
  Telegram polling. On a directory-access hang, inspect scoped TCC evidence
  before attributing the issue to File Provider materialization or queue locks.
- Report missing signing identities or stale privacy grants. Do not reset TCC,
  modify its database, or grant broader filesystem access automatically.
