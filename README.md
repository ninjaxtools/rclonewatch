# rclonewatch

`rclonewatch` watches a local directory recursively with Linux inotify and syncs changes with an rclone destination.

The most common mode of operation wraps a program that changes the watched directory. In the following example `./build.sh` is executed through rclonewatch, and changes are synced every 30 seconds (`--interval 30s`), excluding temporary files (`--exclude '*.tmp'`), and a final sync runs after the command exits.

```sh
./rclonewatch --interval 30s --exclude '*.tmp' --logs \
  /srv/data remote:backup/data -- ./build.sh --release
```

The command is optional and if not provided rclonewatch keeps running until an interrupt is sent to the process which will cause a clean shutdown with a final sync.

Any changes to the watched directory, whether made by a wrapped program or not are synchronised. If the remote directory does not have a state file, a full local to remote sync is performed first. If a remote state file exists and its generation is ahead of the local state file, then a full remote to local sync is performed to reconcile the local directory with any remote changes performed from another location.

## Installation

Build it with `go build -o rclonewatch .`.

Prebuilt Linux binaries for amd64 and arm64 are attached to each [GitHub release](https://github.com/ninjaxtools/rclonewatch/releases).

## Remote configuration

For example, an AWS S3 remote can be configured in `~/.config/rclone/rclone.conf` without storing credentials in the file:

```ini
[s3]
type = s3
provider = AWS
env_auth = true
region = us-east-1
```

Alternatively, define the same remote entirely through environment variables without an `rclone.conf` file:

```sh
export RCLONE_CONFIG_S3_TYPE=s3
export RCLONE_CONFIG_S3_PROVIDER=AWS
export RCLONE_CONFIG_S3_ENV_AUTH=true
export RCLONE_CONFIG_S3_REGION=us-east-1
```

In either case, set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (and `AWS_SESSION_TOKEN` for temporary credentials), then use the config section or environment remote name as the destination:

```sh
./rclonewatch --interval 30s /srv/data s3:my-bucket/backup/data
```

For other S3-compatible services, set the provider and endpoint described in [rclone's S3 configuration documentation](https://rclone.org/s3/).

## Usage

Arguments:

- `SOURCE_FOLDER` is an existing local directory. A symlink root is resolved before watching and resolving local state paths.
- `RCLONE_DESTINATION` is any destination accepted by rclone.
- `--interval DURATION` waits that long after each successful sync before starting another. Failed syncs retry with exponential backoff from 1 second to 1 minute. With no interval, changes are synced only during shutdown.
- `--lock-ttl DURATION` overrides the default two-minute remote lock expiry. The TTL is stored with the lock, whose timestamp is refreshed at half the TTL.
- `--lock-wait 0|DURATION|inf` controls how long startup follows an active lock. By default, startup waits for the observed lock to expire but fails if it is refreshed. An explicit `0` exits immediately; durations use `s`, `m`, and `h`; `inf` waits indefinitely.
- `--persistent-lock ID` uses `ID` as the lock owner and retains the lock on exit. A later invocation with the same ID continues it immediately and refreshes it with the configured TTL.
- `--upload-only` sends local changes to the destination without creating or reading state, acquiring a lock, or reconciling remote changes. Lock and state options cannot be used with it.
- `--exclude GLOB` excludes files matching an [rclone filter glob](https://rclone.org/filtering/). Repeat the option to supply multiple patterns. Patterns apply to outgoing payload syncs and startup reconciliation, not sync-state metadata.
- `--no-consistent-writes` disables conditional state writes for direct S3-compatible destinations. S3 conditional writes are enabled by default and require rclone v1.73.0 or newer.
- `--force-delete-untracked-remote` permits initialization when the destination has no `.rcw-state` file but is not empty. Existing remote payload is deleted before the initial local-to-remote sync.
- `--fail-on-incomplete-sync` exits with status `1`, before lock acquisition or any remote write, if local or remote state records `"syncing": true`, or local state records `"active": true` from an interrupted session.
- `--state-file PATH` changes the state-file path on both sides. The path includes the filename and is resolved relative to each payload root. Glob characters in state paths are treated literally. Line breaks are rejected because rclone's single-file metadata reads and filters do not handle them consistently.
- `--state-file-local PATH` and `--state-file-remote PATH` set different paths and must be supplied together. They cannot be combined with `--state-file`.
- `--logs` writes rclonewatch diagnostics, sync status, changed paths, and rclone output to stdout. Without it, rclonewatch does not write any runtime output itself.
- `-- COMMAND [ARG...]` runs a command after inotify is ready with standard input, output, and error passed through unchanged. `rclonewatch` watches until it exits, then performs the final sync and returns the command's status when syncing succeeds. `SIGINT` and `SIGTERM` are forwarded to the command, and `rclonewatch` waits for it to exit.

Send `SIGINT` or `SIGTERM` to stop watching, finish one final sync, and exit. With a wrapped command, its status is returned when syncing succeeds. A failed final sync, lock/generation failure, or watcher failure exits with status `1`; invalid command-line usage exits with status `2`.

## Watching and payload synchronization

Watches are installed before startup reconciliation. Events are collected while initialization or download runs and retained for the next outgoing batch, including writes made after the startup scan has passed a file. Inotify queue overflow rebuilds the recursive watch set and requests a full reconciliation, so newly created or moved directories continue to be watched afterward.

File and directory changes are coalesced into the smallest set of affected paths and subtrees. For example, events for `a`, `a/b`, and `a/b/file` reconcile `a` once. A scoped sync transfers included files and removes obsolete contents within those scopes, preserving unrelated sibling payload. Deletions and uploads are grouped separately so an already-absent child does not cause a remote ancestor file to be replaced. Deleting an already absent directory succeeds, including when it was never uploaded or was removed by an earlier attempt.

Scopes use root-relative rclone filters, so anchored user exclusions and state-file protections retain their meaning. Obsolete entries are deleted before copying to allow file/directory replacements. If a queued child's parent has disappeared or become a file, reconciliation expands to that affected ancestor. A conflicting remote ancestor file can also be replaced without syncing its unrelated siblings. Excluded payload and metadata remain protected; a type conflict requiring their deletion fails the sync. Failed batches retain their scopes for retry rather than automatically expanding to a full-root sync.

Full-root synchronization is used for initialization, startup reconciliation, interrupted-session recovery, and explicit rescans such as inotify overflow. Paths whose names cannot be safely expressed in a filter use the nearest representable ancestor, which may be the root. Known-changed files and reconciliation transfers use `--ignore-times`: matching size and modification time cannot hide changed contents, including on backends without usable checksums. Scoped reconciliation retransfers included files only within its selected subtrees; full-root reconciliation retransfers all included files.

## Locking

Unless `--upload-only` is used, startup waits until the initially observed lock expires, then checks it again and fails if it was refreshed. A finite wait follows refreshed locks until its overall deadline; `--lock-wait inf` follows them indefinitely; and `--lock-wait 0` fails immediately. An expired or absent lock is replaced and verified before watching starts. A matching `--persistent-lock` ID bypasses this wait, adopts the existing lock after the initial state read, and immediately refreshes it with the configured TTL. Transient refresh failures retry until shortly before the current lock expires. If ownership is lost or the lock still cannot be refreshed, active rclone work and the wrapped command are terminated and no final sync is attempted. Shutdown clears the lock only when its owner, timestamp, and TTL still match the state last written by this process, except that a persistent lock is retained; `.rcw-state` itself remains.

Lock expiry is always measured locally by waiting the stored TTL from the time a lock is observed. The recorded timestamp is never used to calculate expiry, preventing clock skew between systems from shortening the wait.

Direct S3 remotes use conditional `If-None-Match` and `If-Match` writes by default. Other backends, and S3 providers used with `--no-consistent-writes`, use advisory write/read ownership verification; all writers must follow the same protocol.

Backend detection uses the configured backend type, including environment-defined and on-the-fly remotes and connection-string overrides. It does not depend on the remote's name. Acquisition rechecks incomplete-state and untracked-payload conditions after waiting or losing an acquisition race, before writing a new lock.

## State and reconciliation

`.rcw-state` combines generation, sync identity, and synchronization status with the optional lock:

- `generation` is a positive integer starting at `1`; remote generation `0` is reserved for an initialization in progress and is never written locally.
- `sync_id` is a unique, randomly generated identifier for an upload attempt. Initialization creates one, and each outgoing batch (including recovery) creates a new one, written locally before publication remotely. It is retained after completion and through lock refreshes and release.
- `syncing` is set to `true` locally and remotely, together with an incremented generation, before each outgoing payload batch. It returns to `false` only after every payload operation succeeds.
- `active` is a local-only session marker. Startup sets it to `true` before running a wrapped command, and it stays true through successful interval batches. Shutdown sets it to `false` only after the watcher has drained and all pending changes have synced successfully. A wrapped command's nonzero exit status does not prevent clearing it when syncing succeeds. Failed recovery, failed final sync, and watcher failures leave it true.
- `lock`, while held, contains an opaque owner token (or the supplied persistent ID), an RFC3339Nano timestamp, and the writer's TTL. Unlocking removes this field while preserving the rest of the state.

By default, startup initializes untracked destinations and reconciles generations as follows:

- If remote state is absent, an empty destination is locked at generation `0`, cleared, fully synced from local to remote, and promoted to generation `1`. A non-empty destination is rejected unless `--force-delete-untracked-remote` is supplied.
- A remote generation `0` records an interrupted initialization. After acquiring its lock, startup clears the remote payload, retries the full local-to-remote sync, and promotes both state files to generation `1`.
- If only the remote state file exists or its generation is higher, the remote is fully synced to the local source.
- Equal initialized generations with different sync IDs trigger a full remote-to-local sync. This prevents an unpublished local generation from overwriting another writer's upload that happens to have the same generation number.
- Equal initialized generations with matching sync IDs skip reconciliation only when neither state file records `syncing: true` and the previous local `active` marker is false. If either syncing flag or the previous active marker is true, startup performs a full local-to-remote sync. This recovers changes queued during an interrupted session even if no upload batch had started.
- A completed local generation higher than an initialized remote is an error. A one-generation local advance marked incomplete is an unpublished metadata update and is recovered with a full local-to-remote sync as well.
- Recovery uploads advance the generation and clear both syncing flags only after success; local `active` remains true until successful shutdown. If local state is absent, its generation is behind the remote, or equal generations have different sync IDs, the full remote-to-local sync takes priority even when either syncing flag or the local active marker is set. The remote's generation, sync ID, and syncing value are copied to local state, which is marked active for the new session.
- With `--fail-on-incomplete-sync`, startup refuses incomplete uploads and interrupted local sessions before modifying the remote.

Local state updates use a synced temporary file, atomic rename, and directory sync. Temporary sibling files matching `<local-state-file>.rcw-tmp-*` are reserved and excluded from payload transfers and change batching. When local state is initially absent, it is created with `active: true` after initialization or download succeeds; until then, absent local state or remote generation `0` ensures that interrupted startup work is retried.

Older state files without `sync_id` remain readable and gain an ID on the next upload. Equal generations with no IDs on either side can skip reconciliation when both are complete, but incomplete states are ambiguous and require manual reconciliation. If only one side has an ID at equal generations, the IDs differ and remote-to-local reconciliation applies. All writers must support the new state field; older versions that reject unknown fields cannot read it.

Local state without `active` is treated as inactive for compatibility. Completed legacy generations without sync IDs can recover an interrupted session using `active: true`; the ambiguous-incomplete-upload check still applies if either `syncing` flag is true. The active marker is never written to remote state. Older versions that reject unknown fields cannot read updated local state files containing it.

**Warning:** Files are not necessarily synced to the remote in the order they were written locally. If a program writes multiple files and a later write assumes that an earlier one has already been persisted, an interrupted sync may leave the remote in an inconsistent state.

## Releasing

Run the minor-version release script from a clean worktree:

```sh
./publish-minor
```
