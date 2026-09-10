# rclonewatch

`rclonewatch` watches a local directory recursively with Linux inotify and syncs changed paths to an rclone destination. Changes are deduplicated between runs. Events received while rclone is running are retained for the next run.

The most common invocation wraps the program that changes the watched directory. Here inotify is ready before `./build.sh` starts, changes are synced every 30 seconds except temporary files, and a final sync runs after the command exits. Reusing `build-sequence` in a later invocation continues the same lock without waiting for expiry, and leaves it available for the next invocation after exit.

```sh
./rclonewatch --interval 30s --use-lock 2m --persistent-lock build-sequence \
  --reconcile-remote-changes --exclude '*.tmp' --logs \
  /srv/data remote:backup/data -- ./build.sh --release
```

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
- `--use-lock TIMEOUT` coordinates access through the persistent `.rcw-state` JSON file. Its lock timestamp is refreshed at half the timeout.
- `--lock-wait 0|DURATION|inf` controls how long startup follows an active lock. By default, startup waits for the observed lock to expire but fails if it is refreshed. An explicit `0` exits immediately; durations use `s`, `m`, and `h`; `inf` waits indefinitely.
- `--persistent-lock ID` uses `ID` as the lock owner and retains the lock on exit. A later invocation with the same ID continues it immediately, refreshing it first only when half the timeout has elapsed. It requires `--use-lock`.
- `--exclude GLOB` excludes files matching an [rclone filter glob](https://rclone.org/filtering/). Repeat the option to supply multiple patterns. Patterns apply to outgoing payload syncs and `--reconcile-remote-changes`, not sync-state metadata.
- `--no-consistent-writes` disables conditional state writes for direct S3-compatible destinations. S3 conditional writes are enabled by default and require rclone v1.73.0 or newer.
- `--reconcile-remote-changes` enables generation-based remote-to-local startup reconciliation. It requires `--use-lock`.
- `--force-delete-untracked-remote` permits initialization when the destination has no `.rcw-state` file but is not empty. Existing remote payload is deleted before the initial local-to-remote sync. It requires `--use-lock`.
- `--fail-on-incomplete-sync` exits with status `1`, before lock acquisition or any remote write, if local or remote state records `"syncing": true`. It requires `--use-lock`.
- `--state-file PATH` changes the state-file path on both sides. The path includes the filename and is resolved relative to each payload root.
- `--state-file-local PATH` and `--state-file-remote PATH` set different paths and must be supplied together. They cannot be combined with `--state-file`.
- `--logs` writes rclonewatch diagnostics, sync status, changed paths, and rclone output to stdout. Without it, rclonewatch does not write any runtime output itself.
- `-- COMMAND [ARG...]` runs a command after inotify is ready with standard input, output, and error passed through unchanged. `rclonewatch` watches until it exits, then performs the final sync and returns the command's status when syncing succeeds. `SIGINT` and `SIGTERM` are forwarded to the command, and `rclonewatch` waits for it to exit.

Send `SIGINT` or `SIGTERM` to stop watching, finish one final sync, and exit. With a wrapped command, its status is returned when syncing succeeds. A failed final sync, lock/generation failure, or watcher failure exits with status `1`; invalid command-line usage exits with status `2`.

## Locking

When locking is enabled without `--lock-wait`, startup waits until the initially observed lock expires, then checks it again and fails if it was refreshed. A finite wait follows refreshed timestamps until its overall deadline; `--lock-wait inf` follows them indefinitely; and `--lock-wait 0` fails immediately. An expired or absent lock is replaced and verified before watching starts. A matching `--persistent-lock` ID bypasses this wait and adopts the existing lock after the initial state read; no write is needed unless its normal half-timeout refresh is due. Refresh failures stop the process without syncing further. Shutdown clears the lock only when its owner and timestamp still match the state last written by this process, except that a persistent lock is retained; `.rcw-state` itself remains.

Direct S3 remotes use conditional `If-None-Match` and `If-Match` writes by default. Other backends, and S3 providers used with `--no-consistent-writes`, use advisory write/read ownership verification; all writers must follow the same protocol.

## State and reconciliation

`.rcw-state` combines generation and synchronization status with the optional lock:

- `generation` is a positive integer starting at `1`; remote generation `0` is reserved for an initialization in progress and is never written locally.
- `syncing` is set to `true` locally and remotely, together with an incremented generation, before each outgoing payload batch. It returns to `false` only after every payload operation succeeds.
- `lock`, while held, contains an opaque owner token (or the supplied persistent ID) and an RFC3339Nano timestamp. Unlocking removes this field while preserving the rest of the state.

With `--use-lock`, startup initializes untracked destinations and optionally reconciles generations as follows:

- If remote state is absent, an empty destination is locked at generation `0`, cleared, fully synced from local to remote, and promoted to generation `1`. A non-empty destination is rejected unless `--force-delete-untracked-remote` is supplied.
- A remote generation `0` records an interrupted initialization. After acquiring its lock, startup clears the remote payload, retries the full local-to-remote sync, and promotes both state files to generation `1`.
- With `--reconcile-remote-changes`, if only the remote file exists or its generation is higher, the remote is fully synced to the local source.
- Equal initialized generations skip reconciliation.
- A completed local generation higher than an initialized remote is an error. A one-generation local advance marked incomplete is an unpublished metadata update and is rolled back safely.
- Without `--fail-on-incomplete-sync`, an old `syncing` value is retained until a successful payload sync supersedes or completes it. With the option, startup refuses incomplete local or remote state before modifying the remote.

Startup initialization and remote-to-local reconciliation use full `rclone sync` operations. Initialization first deletes all existing remote payload and then syncs local to remote; reconciliation deletes local files that are absent remotely. User-supplied `--exclude` patterns apply in both sync directions. When exclusions are configured, outgoing changed-path batches use a full filtered sync so creates and deletions consistently obey rclone's glob semantics. Sync-state paths inside a payload root are reserved and excluded from payload transfers. When local and remote state paths differ, both relative names are excluded. A path resolved outside its root needs no exclusion. `..` components are supported after resolution; absolute state paths are rejected.

The executable requires Linux and an `rclone` executable on `PATH`. Rclone configuration is inherited from the process environment and rclone's standard config locations. Changed-path batches use `--files-from0`, so filenames containing newlines are handled safely.

## Releasing

Run the minor-version release script from a clean worktree:

```sh
./publish-minor
```

The script pushes the current branch, increments the latest stable version tag from `vMAJOR.MINOR.PATCH` to `vMAJOR.(MINOR+1).0`, and pushes the tag to `origin`. Pushing the tag runs the tests and publishes both Linux binaries with SHA-256 checksums.
