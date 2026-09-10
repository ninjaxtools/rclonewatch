# rclonewatch

`rclonewatch` watches a local directory recursively with Linux inotify and syncs changed paths to an rclone destination. Changes are deduplicated between runs. Events received while rclone is running are retained for the next run.

```sh
go build -o rclonewatch .
./rclonewatch --interval 5m --use-lock 2m --sync-remote --logs /srv/data remote:backup/data
```

Prebuilt Linux binaries for amd64 and arm64 are attached to each [GitHub release](https://github.com/ninjaxtools/rclonewatch/releases).

Arguments:

- `SOURCE_FOLDER` is an existing local directory.
- `RCLONE_DESTINATION` is any destination accepted by rclone.
- `--interval DURATION` waits that long after each successful sync before starting another. Failed syncs retry with exponential backoff from 1 second to 1 minute. With no interval, changes are synced only during shutdown.
- `--use-lock TIMEOUT` coordinates access through the persistent `.rcw-sync` JSON file. Its lock timestamp is refreshed at half the timeout.
- `--lock-wait 0|DURATION|inf` controls how long startup follows an active lock. By default, startup waits for the observed lock to expire but fails if it is refreshed. An explicit `0` exits immediately; durations use `s`, `m`, and `h`; `inf` waits indefinitely.
- `--no-consistent-writes` disables conditional state writes for direct S3-compatible destinations. S3 conditional writes are enabled by default and require rclone v1.73.0 or newer.
- `--sync-remote` enables generation-based remote-to-local startup reconciliation. It requires `--use-lock`.
- `--fail-on-incomplete-sync` exits with status `1`, before lock acquisition or any remote write, if local or remote state records `"syncing": true`. It requires `--use-lock`.
- `--sync-file PATH` changes the sync-state path on both sides. The path includes the filename and is resolved relative to each payload root.
- `--sync-file-local PATH` and `--sync-file-remote PATH` set different paths and must be supplied together. They cannot be combined with `--sync-file`.
- `--logs` writes sync status, changed paths, and rclone output to stdout.

Send `SIGINT` or `SIGTERM` to stop watching, finish one final sync, and exit. The exit status is `0` only if no changed paths remain unsynced. A failed final sync, lock/generation failure, or watcher failure exits with status `1`; invalid command-line usage exits with status `2`.

When locking is enabled without `--lock-wait`, startup waits until the initially observed lock expires, then checks it again and fails if it was refreshed. A finite wait follows refreshed timestamps until its overall deadline; `--lock-wait inf` follows them indefinitely; and `--lock-wait 0` fails immediately. An expired or absent lock is replaced and verified before watching starts. Refresh failures stop the process without syncing further. Shutdown clears the lock only when its owner and timestamp still match the state last written by this process; `.rcw-sync` itself remains.

Direct S3 remotes use conditional `If-None-Match` and `If-Match` writes by default. Other backends, and S3 providers used with `--no-consistent-writes`, use advisory write/read ownership verification; all writers must follow the same protocol.

`.rcw-sync` combines generation and synchronization status with the optional lock:

- `generation` is a positive integer starting at `1`.
- `syncing` is set to `true` locally and remotely, together with an incremented generation, before each outgoing payload batch. It returns to `false` only after every payload operation succeeds.
- `lock`, while held, contains an opaque owner token and an RFC3339Nano timestamp. Unlocking removes this field while preserving the rest of the state.

With `--sync-remote`, startup reconciles generations as follows:

- If both files were absent, lock acquisition creates remote generation `1`, then the remote is fully synced to the local source and local generation `1` is created.
- If only the remote file exists, or its generation is higher, the remote is fully synced to the local source.
- Equal generations skip the startup sync.
- A missing remote state when local state exists, or a completed local generation higher than remote, is an error. A one-generation local advance marked incomplete is an unpublished metadata update and is rolled back safely.
- Without `--fail-on-incomplete-sync`, an old `syncing` value is retained until a successful payload sync supersedes or completes it. With the option, startup refuses incomplete local or remote state before modifying the remote.

The startup remote-to-local operation is a full `rclone sync`: local files that are not present remotely are deleted when reconciliation is required. Sync-state paths inside a payload root are reserved and excluded from payload transfers. When local and remote state paths differ, both relative names are excluded. A path resolved outside its root needs no exclusion. `..` components are supported after resolution; absolute state paths are rejected.

The executable requires Linux and an `rclone` executable on `PATH`. Rclone configuration is inherited from the process environment and rclone's standard config locations. Changed-path batches use `--files-from0`, so filenames containing newlines are handled safely.

## Releasing

Run the minor-version release script from a clean worktree:

```sh
./publish-minor
```

The script pushes the current branch, increments the latest stable version tag from `vMAJOR.MINOR.PATCH` to `vMAJOR.(MINOR+1).0`, and pushes the tag to `origin`. Pushing the tag runs the tests and publishes both Linux binaries with SHA-256 checksums.
