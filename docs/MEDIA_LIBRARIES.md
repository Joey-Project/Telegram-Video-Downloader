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
An explicitly empty category selects the library root; an omitted category uses that library's
configured default. Removing a video registration that shares the PDF root retains PDF queue
ownership until the process releases the PDF store.
If an accepted download request has left the recent operation history, the File Mini App can
offer an explicit control to clear its browser waiting notice. This does not establish the
request's outcome, cancel the task, or submit it again. Check the Telegram queue before choosing
to submit another request.

The settings page distinguishes applied local settings from requests waiting for the desktop.
An offline snapshot is a last known view, not evidence that a requested setting was applied.
Polling retains unsaved name and category edits for the same library ID and root. Each field
returns to the applied snapshot when its submitted value appears; unrelated updates do not
discard edits, and removed or rebound registrations do not inherit another library's drafts.
An opaque binding combines the saved directory identity with a persisted registration identity,
distinguishing a replacement registered at the same pathname. Removing and registering the same
directory again creates a new binding. Name/category edits, ordinary child-entry changes and
restarts leave it stable. Older registry entries without the new registration identity retain
their stable legacy binding until a new registration replaces them. Legacy snapshots without a
known binding do not retain drafts across snapshot updates.
Default, category, name and unregister requests also carry the displayed registration binding.
The desktop compares it with the current registration while applying the change; a stale tab
cannot modify a replacement registered at the same pathname. Missing or mismatched bindings
are rejected, including older queued requests. Refresh the applied library view before submitting
a new request rather than reusing the old confirmation.
An open Mini App retains the last successful view for the same library ID and root, marks it
read-only while unavailable, and replaces it after a successful scan. A fresh page without that
cached view reports the library as unavailable rather than inventing its contents.
Removing a registration preserves files and requires that dependent tasks and migrations have
been resolved. A library is never unregistered automatically after migration.
Successful removal evicts that registration's cached scan and unavailable state under the scan
publication gate, so re-registering a replacement at the same path cannot expose the old media view.
Retryable failed video tasks still depend on their registered library. Before a task resolves its
download configuration, the queue revalidates the registered root's saved directory identity;
a replacement directory at the same pathname is rejected while task and staging records remain
bound to the original directory.
The selected root is checked again before enqueue writes queue state, so a replacement between
configuration lookup and task creation cannot redirect durable task records.
Before writing a new video task, every registered queue must be searchable to prove the task ID
is unused. If a library is offline, creation refuses that uncertain absence; a safely found existing
task remains an idempotent replay. Reconnecting the original library allows replay without moving
the task to a newer default library.
Changing the default or a library's category also requires the registered root to be available
and identity-matched; rejection preserves the existing settings and revision.
Registration stores the identity of the directory opened during preparation with the new entry.
A replacement detected before that commit leaves the registry, default and revision unchanged.
Before a scoped cloud management operation opens its library manager, dispatch refreshes the selected
registration and checks that its saved root identity still matches an available queue store. A missing
status, unavailable root or identity mismatch fails closed before scanning or writing library metadata;
the selected library is marked unavailable and its cached view stays read-only. Ordinary child-entry
changes do not invalidate the root. A scoped refresh can restore service when the original bound root
returns, while a replacement at the same path remains rejected.

A failed refresh retains the last successful view for that library and marks it unavailable.
Cached items stay visible but read-only until a successful scan. Refresh and library switching
remain available so the user can recover or inspect another library.
Compacted file previews retain their original library ID, including reload-required placeholders.
Explicit malformed IDs are omitted rather than assigned to the currently selected library.
The media total remains the full reported count when the cloud retains only part of an item list;
the page discloses that filters and pagination apply to the retained rows.
The `/file` and `/settings` pages keep locally queued actions with their originating surface,
including receipt tracking before removing an action from their shared browser queue.
Requests awaiting a local result retain a sequence receipt by request ID so another tab can
help send them without consuming the origin tab's result. The origin recovers its own receipt
before polling or retrying; if necessary, it resubmits the same saved request ID and payload.
Permanent action rejections are recorded before dequeue as well. The origin clears the rejected
submission or exposes its conflict-refresh error rather than restoring that rejected payload.
Temporary transport errors and owner-authentication failures retain their existing recovery paths.

## Moving a library

Choose the entire source library or selected media, select the destination library, and inspect
the preview before confirming. Relative categories and collection structure are preserved.
Video, NFO, danmaku, subtitle, and cover associations move as a bundle. Conflicts default to
**Skip**; **Keep both** reserves a new bundle name without replacing existing target files.
Migration previews use an internal completeness proof from the bounded source scan. A
whole-library preview requires the scan to finish without entry or media limits, timeout, or
unavailable media, sidecar, or directory reads. For a selected migration, a localized incomplete
directory blocks only bundles whose primary or attachment paths are inside it, while an unreadable
individual media or sidecar path blocks only bundles containing that path. If the scan cannot
localize the incomplete scope, selected migration fails closed. Missing optional metadata and
low-confidence metadata do not by themselves make a scan incomplete.
Refreshing conflict choices durably supersedes the exact earlier preview. Reloading the Mini App
cannot make that earlier revision executable again. A refreshed candidate is durable before the
old revision is superseded; interrupted refreshes replay from those durable records after restart.
Conflict choices for unfinished tasks are validated before either preview document is written.
A rejected refresh keeps the old preview usable and does not leave an orphan replacement.
Migration preview and discovery validate each registered root's availability and saved directory
identity before opening its metadata, including both endpoints. Replaced roots are rejected;
ordinary child-entry changes remain supported.
Registered cloud file and migration managers retain the verified queue root's open directory
descriptor during metadata initialization. They do not reopen the pathname after validation,
so a directory replacement cannot receive another library's recovery metadata.
Starting a migration requires both endpoint IDs to resolve to registered libraries. Removing a
previously selected target disables that preview's Start control.
Polling and scope changes preserve a selected target while it remains registered and distinct
from the source. A temporarily offline target stays selected with preview disabled.
Expired, superseded and confirmed preview documents are retired through bounded cleanup.
Confirmation keeps the durable job plan and replay information needed for recovery; retiring a
preview does not remove media, retained download fragments or an active migration's checkpoints.
An expired migration preview is marked stale in the cloud view and cannot be started. Storage
read failures remain an attention state rather than being treated as a missing preview.
Large previews retain an actionable summary with counts, space requirements, sampled paths and
explicit omission totals. Starting this summary confirms the complete selection with its existing
conflict choices; per-item conflict editing is unavailable in the compact view.
Full previews show a bounded page of conflict controls. Choices remain associated with the
exact preview and revision while switching pages, and confirmation includes choices from every
page. A replacement preview does not inherit the previous revision's editing state.

The separate **include unfinished tasks** checkbox is shown only when the scope affects such
tasks and starts unchecked. When checked for a partially selected unfinished collection task,
the preview expands to all of that task's published members. Sizes of active staging files are
estimates until writers stop. If the task publishes additional media outside the approved scope,
create a fresh preview.
Task summaries include their frozen library identity and media kind. Per-library counts include
retryable failed tasks and do not depend on the truncated global task list; PDF and terminal
completed/cancelled tasks do not make the video-task migration option appear.

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
Publication also recovers an interrupted permission change before the final rename, using the
saved staging identity and access proof rather than accepting an arbitrary file at that path.

**Pause** preserves checkpoints for continuation. **Stop remaining** keeps completed moves and
leaves uncompleted source data available. **Retry failed** retries failed work. The Mini App shows
actual source and destination locations; Telegram edits one migration status message. Ambiguous
Telegram delivery is retained without automatically sending a duplicate message.
Task-transfer failures also expose retry and stop controls, even when no media bundle failed.
Media-only failed and partially completed migrations expose **Stop remaining** as well, allowing
the remaining source files to stay in place without retrying the move.
Startup completes or aborts a stopped migration's pending queue handoffs so the affected tasks
do not remain blocked by an orphaned transfer.
When a new independent queue store reconnects, its library remains unavailable until the existing
startup recovery pass finishes and the saved root identity is verified.
Participating libraries remain registered until pending queue handoffs are acknowledged, even
when the migration already has a terminal status. This preserves discovery of its source manifest.
After queue acknowledgement and verified payload removal, source staging controls and empty job
directories are cleaned through identity-bound checkpoints. Pending cleanup retains both library
registrations so restart can resolve either end. Unexpected or replaced entries are preserved
for inspection.
An older handoff without verifiable queue acknowledgement remains marked for attention with its
source controls preserved; a missing queue marker or task is not treated as proof of completion.

An independent unchecked option can set the target as default after success. That update is
recorded once so crash recovery cannot overwrite a later user choice. Migration manifests are
kept in the source library's private metadata directory, independently of cloud receipt retention.
A cloud database epoch change pauses automatic recovery until a new authorized resume.
Completed or stopped jobs with pending queue/source cleanup expose an explicit finalization
control after such a change. Completed jobs can also finish an unresolved default update.
This authorization preserves stopped work and does not restart uninstalled media or task plans;
fully finalized history does not show the control.
The default update is conditional on the settings revision durably recorded at confirmation,
before waiting for task writers or output ownership. A newer settings change causes it to be
skipped durably. Cloud snapshots include at most 32 migrations, prioritizing active
and recoverable jobs, including completed or stopped jobs with pending finalization,
then recent completed jobs. The UI displays every published entry in that priority order,
keeping all published recovery controls reachable, and reports backend omissions separately;
local manifests remain available for recovery.
Snapshot upload limits also compact migration item, task-handoff, and conflict-choice details.
Totals, recovery controls, error counts, bounded error details and omission counts remain
available; complete records stay in the source library's private migration manifest.
The registry keeps recent default-update results within its serialized byte limit. Older exact
outcomes are durably saved in a separate bounded private archive before removal from the registry document,
so replay of an old migration still preserves later user choices. Conflicting or unreadable
receipts require attention rather than being treated as an absent result.

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
