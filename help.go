package main

import (
	"fmt"
	"io"
)

func printUsage(output io.Writer) {
	fmt.Fprintln(output, "Usage: rclonewatch [OPTIONS] SOURCE_FOLDER RCLONE_DESTINATION")
}

func printHelp(output io.Writer) {
	fmt.Fprint(output, `rclonewatch watches a local Linux directory recursively with inotify and
syncs deduplicated changed paths to an rclone destination.

Usage:
  rclonewatch [OPTIONS] SOURCE_FOLDER RCLONE_DESTINATION

Options:
  --interval DURATION
        Wait this long after a successful sync before syncing another batch.
        Durations use Go syntax, such as 30s, 5m, or 1h30m. Without this
        option, changes are synced only during shutdown.

  --use-lock TIMEOUT
        Coordinate writers through the remote .rcw-sync JSON file. The lock
        timestamp is refreshed every TIMEOUT/2. TIMEOUT must be positive.

  --lock-wait 0|DURATION|inf
        How long to wait when an active lock exists. By default, wait for the
        observed lock to expire but fail if it is refreshed. An explicit 0
        exits immediately. A duration waits up to that total time while
        following refreshes. inf waits indefinitely. Units are s, m, h.

  --no-consistent-writes
        Disable conditional state writes on direct S3-compatible destinations.
        By default S3 writes use If-None-Match/If-Match and require rclone
        v1.73.0 or newer. Use this only for providers without conditional
        write support.

  --sync-remote
        Reconcile remote changes into SOURCE_FOLDER at startup using
        .rcw-sync generations. Requires --use-lock. A full remote-to-local
        sync runs only when the remote generation is newer or local state is
        absent.

  --fail-on-incomplete-sync
        Exit with status 1 before lock acquisition or remote writes when the
        local or remote .rcw-sync has syncing set to true. Requires --use-lock.

  --sync-file PATH
        Use PATH, relative to each root and including the filename, for both
        local and remote sync state. Cannot be combined with the path pair.

  --sync-file-local PATH
  --sync-file-remote PATH
        Set different local and remote sync-state paths. Both must be supplied.
        Paths are relative to their respective roots; .. components may place
        state outside a root. Sync path options require --use-lock.

  --logs
        Write status, changed paths, and rclone output to stdout.

  -h, --help
        Show this help and exit.

Sync file behavior:
  .rcw-sync combines the persistent generation, incomplete-sync flag, and
  optional lock owner/timestamp. Unlocking clears the lock fields but does not
  delete the file. If local state is absent or older, --sync-remote performs a
  full remote-to-local sync. Equal generations skip it. A missing remote state
  when local state exists, or a completed local generation ahead of remote, is
  an error. Before each outgoing batch, generation is incremented and syncing
  is set true on both sides before payload changes. It is cleared only after
  the entire batch succeeds, so failures remain detectable on the next run.
  A required remote-to-local sync deletes local payload absent remotely.

  State paths inside either payload root are excluded from payload transfers.
  When local and remote paths differ, both relative names are reserved.

Shutdown and retries:
  Failed running syncs retry with exponential backoff from 1 second to 1
  minute. SIGINT or SIGTERM drains queued inotify events, performs a final
  sync, clears an owned lock, and exits. Status 0 means no changed paths are
  left unsynced; runtime failures use status 1 and usage errors status 2.

Examples:
  # Periodically sync local changes.
  rclonewatch --interval 5m /srv/data remote:backup/data

  # Coordinate writers and pull newer remote state before watching.
  rclonewatch --interval 30s --use-lock 2m --lock-wait inf \
    --sync-remote --logs /srv/data s3:bucket/data

  # Use an S3-compatible provider without conditional-write support.
  rclonewatch --use-lock 2m --no-consistent-writes \
    /srv/data s3clone:bucket/data
`)
}
