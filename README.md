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

- `SOURCE_FOLDER` is an existing local directory.
- `RCLONE_DESTINATION` is any destination accepted by rclone.
- `--interval DURATION` waits that long after each successful sync before starting another. Failed syncs retry with exponential backoff from 1 second to 1 minute. With no interval, changes are synced only during shutdown.
- `--lock-ttl DURATION` overrides the default two-minute remote lock expiry. The TTL is stored with the lock, whose timestamp is refreshed at half the TTL.
- `--lock-wait 0|DURATION|inf` controls how long startup follows an active lock. By default, startup waits for the observed lock to expire but fails if it is refreshed. An explicit `0` exits immediately; durations use `s`, `m`, and `h`; `inf` waits indefinitely.
- `--persistent-lock ID` uses `ID` as the lock owner and retains the lock on exit. A later invocation with the same ID continues it immediately and refreshes it with the configured TTL.
- `--upload-only` sends local changes to the destination without creating or reading state, acquiring a lock, or reconciling remote changes. Lock and state options cannot be used with it.
- `--exclude GLOB` excludes files matching an [rclone filter glob](https://rclone.org/filtering/). Repeat the option to supply multiple patterns. Patterns apply to outgoing payload syncs and startup reconciliation, not sync-state metadata.
- `--no-consistent-writes` disables conditional state writes for direct S3-compatible destinations. S3 conditional writes are enabled by default and require rclone v1.73.0 or newer.
- `--force-delete-untracked-remote` permits initialization when the destination has no `.rcw-state` file but is not empty. Existing remote payload is deleted before the initial local-to-remote sync.
- `--fail-on-incomplete-sync` exits with status `1`, before lock acquisition or any remote write, if local or remote state records `"syncing": true`.
- `--state-file PATH` changes the state-file path on both sides. The path includes the filename and is resolved relative to each payload root.
- `--state-file-local PATH` and `--state-file-remote PATH` set different paths and must be supplied together. They cannot be combined with `--state-file`.
- `--logs` writes rclonewatch diagnostics, sync status, changed paths, and rclone output to stdout. Without it, rclonewatch does not write any runtime output itself.
- `-- COMMAND [ARG...]` runs a command after inotify is ready with standard input, output, and error passed through unchanged. `rclonewatch` watches until it exits, then performs the final sync and returns the command's status when syncing succeeds. `SIGINT` and `SIGTERM` are forwarded to the command, and `rclonewatch` waits for it to exit.

Send `SIGINT` or `SIGTERM` to stop watching, finish one final sync, and exit. With a wrapped command, its status is returned when syncing succeeds. A failed final sync, lock/generation failure, or watcher failure exits with status `1`; invalid command-line usage exits with status `2`.

## Locking

Unless `--upload-only` is used, startup waits until the initially observed lock expires, then checks it again and fails if it was refreshed. A finite wait follows refreshed locks until its overall deadline; `--lock-wait inf` follows them indefinitely; and `--lock-wait 0` fails immediately. An expired or absent lock is replaced and verified before watching starts. A matching `--persistent-lock` ID bypasses this wait, adopts the existing lock after the initial state read, and immediately refreshes it with the configured TTL. Transient refresh failures retry until shortly before the current lock expires. If ownership is lost or the lock still cannot be refreshed, active rclone work and the wrapped command are terminated and no final sync is attempted. Shutdown clears the lock only when its owner, timestamp, and TTL still match the state last written by this process, except that a persistent lock is retained; `.rcw-state` itself remains.

Lock expiry is always measured locally by waiting the stored TTL from the time a lock is observed. The recorded timestamp is never used to calculate expiry, preventing clock skew between systems from shortening the wait.

Direct S3 remotes use conditional `If-None-Match` and `If-Match` writes by default. Other backends, and S3 providers used with `--no-consistent-writes`, use advisory write/read ownership verification; all writers must follow the same protocol.

## State and reconciliation

`.rcw-state` combines generation and synchronization status with the optional lock:

- `generation` is a positive integer starting at `1`; remote generation `0` is reserved for an initialization in progress and is never written locally.
- `syncing` is set to `true` locally and remotely, together with an incremented generation, before each outgoing payload batch. It returns to `false` only after every payload operation succeeds.
- `lock`, while held, contains an opaque owner token (or the supplied persistent ID), an RFC3339Nano timestamp, and the writer's TTL. Unlocking removes this field while preserving the rest of the state.

By default, startup initializes untracked destinations and reconciles generations as follows:

- If remote state is absent, an empty destination is locked at generation `0`, cleared, fully synced from local to remote, and promoted to generation `1`. A non-empty destination is rejected unless `--force-delete-untracked-remote` is supplied.
- A remote generation `0` records an interrupted initialization. After acquiring its lock, startup clears the remote payload, retries the full local-to-remote sync, and promotes both state files to generation `1`.
- If only the remote state file exists or its generation is higher, the remote is fully synced to the local source.
- Equal initialized generations skip reconciliation.
- A completed local generation higher than an initialized remote is an error. A one-generation local advance marked incomplete is an unpublished metadata update and is rolled back safely.
- Without `--fail-on-incomplete-sync`, an old `syncing` value is retained until a successful payload sync supersedes or completes it. With the option, startup refuses incomplete local or remote state before modifying the remote.

The executable requires Linux and an `rclone` executable on `PATH`. Rclone configuration is inherited from the process environment and rclone's standard config locations. Changed-path batches use `--files-from0`, so filenames containing newlines are handled safely.

## Releasing

Run the minor-version release script from a clean worktree:

```sh
./publish-minor
```
