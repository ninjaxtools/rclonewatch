# rclonewatch

`rclonewatch` watches a local directory recursively with Linux inotify and syncs changed paths to an rclone destination. Changes are deduplicated between runs. Events received while rclone is running are retained for the next run.

```sh
go build -o rclonewatch .
./rclonewatch --interval 5m --use-lock 2m --sync-remote --logs /srv/data remote:backup/data
```

Arguments:

- `SOURCE_FOLDER` is an existing local directory.
- `RCLONE_DESTINATION` is any destination accepted by rclone.
- `--interval DURATION` waits that long after each successful sync before starting another. Failed syncs retry with exponential backoff from 1 second to 1 minute. With no interval, changes are synced only during shutdown.
- `--use-lock TIMEOUT` coordinates access through `.rcw-lock` at the destination root. The lock contains an RFC3339Nano UTC timestamp and is refreshed at half the timeout.
- `--lock-wait 0|DURATION|inf` controls how long startup follows an active lock. By default, startup waits for the observed lock to expire but fails if it is refreshed. An explicit `0` exits immediately; durations use `s`, `m`, and `h`; `inf` waits indefinitely.
- `--no-consistent-writes` disables conditional lock writes for direct S3-compatible destinations. S3 conditional writes are enabled by default and require rclone v1.73.0 or newer.
- `--sync-remote` enables generation-based remote-to-local startup reconciliation. It requires `--use-lock`.
- `--logs` writes sync status, changed paths, and rclone output to stdout.

Send `SIGINT` or `SIGTERM` to stop watching, finish one final sync, and exit. The exit status is `0` only if no changed paths remain unsynced. A failed final sync, lock/generation failure, or watcher failure exits with status `1`; invalid command-line usage exits with status `2`.

When locking is enabled without `--lock-wait`, startup waits until the initially observed lock expires, then checks it again and fails if it was refreshed. A finite wait follows refreshed timestamps until its overall deadline; `--lock-wait inf` follows them indefinitely; and `--lock-wait 0` fails immediately. An expired or absent lock is replaced and verified before watching starts. Refresh failures stop the process without syncing further, and shutdown removes the lock only when its contents still match the timestamp last written by this process.

Direct S3 remotes use conditional `If-None-Match` and `If-Match` writes by default. Other backends, and S3 providers used with `--no-consistent-writes`, use advisory write/read ownership verification; all writers must follow the same protocol.

With `--sync-remote`, `.rcw-generation` contains a positive decimal generation starting at `1` on both sides:

- If neither generation file exists, the remote is fully synced to the local source before generation `1` is created on both sides.
- If only the remote file exists, or its generation is higher, the remote is fully synced to the local source.
- Equal generations skip the startup sync.
- A missing remote generation when a local one exists, or a local generation higher than the remote, is an error.
- Before each outgoing change batch, the local generation is incremented and copied to the remote before payload changes begin.

The startup remote-to-local operation is a full `rclone sync`: local files that are not present remotely are deleted when reconciliation is required. The root names `.rcw-lock` and `.rcw-generation` are reserved protocol metadata when their respective features are enabled. `.rcw-lock` is excluded from full syncs.

The executable requires Linux and an `rclone` executable on `PATH`. Rclone configuration is inherited from the process environment and rclone's standard config locations. Changed-path batches use `--files-from0`, so filenames containing newlines are handled safely.
