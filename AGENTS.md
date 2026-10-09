# Repository Guidelines

## macOS Bot Deployment

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
