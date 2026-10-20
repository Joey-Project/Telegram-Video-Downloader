# Media libraries, destinations, and migration

## Settings and downloads

In cloud mode, `/settings` opens the separate owner-only settings Mini App. Register an absolute
path on the deployment machine, optionally choose its default category, and explicitly enable
directory creation if the root does not exist. Registering an existing root scans it without
renaming media or patching its NFO. New user-created symlink roots, nested roots, and overlapping
registered roots are rejected. Existing configured access paths and trusted macOS system aliases
retain their verified directory bindings.

One library is the default. Changing the default library or category affects future ordinary
Telegram downloads. Existing tasks retain their saved library and category. The `/file` library
selector includes historical roots; **Download here** submits a video or collection to the selected
library/category. PDF destinations retain their existing independent configuration.

The settings page distinguishes applied local settings from requests waiting for the desktop.
An offline snapshot is a last known view, not evidence that a requested setting was applied.
An open Mini App retains the last successful view for the same library ID and root, marks it
read-only while unavailable, and replaces it after a successful scan. A fresh page without that
cached view reports the library as unavailable rather than inventing its contents.
Removing a registration preserves files and requires that dependent tasks and migrations have
been resolved. A library is never unregistered automatically after migration.

## Moving a library

Choose the entire source library or selected media, select the destination library, and inspect
the preview before confirming. Relative categories and collection structure are preserved.
Video, NFO, danmaku, subtitle, and cover associations move as a bundle. Conflicts default to
**Skip**; **Keep both** reserves a new bundle name without replacing existing target files.

The separate **include unfinished tasks** checkbox is shown only when the scope affects such
tasks and starts unchecked. When checked for a partially selected unfinished collection task,
the preview expands to all of that task's published members. Sizes of active staging files are
estimates until writers stop. If the task publishes additional media outside the approved scope,
create a fresh preview.

Without that checkbox, task records, fragments, recovery information, and their task states stay
in the source library. With it, affected writers pause and release their output ownership before
an exact private-file plan is bound. File payloads are verified and destination control records
are rebuilt against the destination objects. Queue records and indexes use a durable ownership
handoff; locks and inode-bound recovery JSON are not copied as ordinary media. Failed and
manually interrupted tasks retain their state; previously queued/running work returns to normal
scheduling after successful handoff. Retained fragments are relocated, not discarded.

Normal retained BBDown lifecycle records are rebound to the destination directory identities.
Unresolved publication/worker-request controls and path-bearing legacy `BBDown.config` files
are refused before queue handoff. Recover or inspect those source attempts first; all files in a
refused attempt remain preserved at the source.
The migration lock preflight also refuses staging states that ordinary startup recovery would
remove, so acquiring migration ownership cannot discard fragments before they are copied.

Migration copies through private destination staging even on the same filesystem. It verifies
size and SHA-256 before publication, publishes without replacement, and checks the source object,
content, and access policy again before source cleanup. A file/provider error preserves the
remaining source and checkpoint information. This is a recoverable sequence of file checkpoints;
it does not promise atomic rollback of an entire batch.

**Pause** preserves checkpoints for continuation. **Stop remaining** keeps completed moves and
leaves uncompleted source data available. **Retry failed** retries failed work. The Mini App shows
actual source and destination locations; Telegram edits one migration status message. Ambiguous
Telegram delivery is retained without automatically sending a duplicate message.

An independent unchecked option can set the target as default after success. That update is
recorded once so crash recovery cannot overwrite a later user choice. Migration manifests are
kept in the source library's private metadata directory, independently of cloud receipt retention.
A cloud database epoch change pauses automatic recovery until a new authorized resume.

## Storage and rollout

The private media-library registry lives beside the loaded bot configuration. Existing queue,
completion, download staging, and recovery records continue to live beside their media. Download
staging is shared under each library root's `.telegram-video-downloader-staging` directory, outside
ordinary category directories. Do not manually copy queue locks or edit migration manifests.

The existing cloud ingress inbox remains bound to its original configured root. Moving media or
changing the default library does not relocate that inbox; keep its original private metadata
directory accessible for cloud command intake and durable receipts.

Deploying the Worker requires applying D1 migrations, including
`cloud/migrations/0003_media_library_request_kinds.sql`, before new action kinds can be stored.
Follow [the cloud deployment instructions](CLOUD_FILE_MANAGEMENT.md). Building or running mock
tests does not change the live Worker, webhook, LaunchAgent, or production media.
