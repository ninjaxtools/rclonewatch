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
        Coordinate writers with RCLONE_DESTINATION/.rcw-lock. The lock is
        refreshed every TIMEOUT/2. TIMEOUT must be a positive duration.

  --lock-wait 0|DURATION|inf
        How long to wait when an active lock exists. By default, wait for the
        observed lock to expire but fail if it is refreshed. An explicit 0
        exits immediately. A duration waits up to that total time while
        following refreshes. inf waits indefinitely. Units are s, m, h.

  --no-consistent-writes
        Disable conditional lock writes on direct S3-compatible destinations.
        By default S3 locks use If-None-Match/If-Match and require rclone
        v1.73.0 or newer. Use this only for providers without conditional
        write support.

  --sync-remote
        Reconcile remote changes into SOURCE_FOLDER at startup using
        .rcw-generation. Requires --use-lock. A full remote-to-local sync is
        performed only when the remote generation is newer or both generation
        files are absent.

  --logs
        Write status, changed paths, and rclone output to stdout.

  -h, --help
        Show this help and exit.

Generation behavior:
  If neither side has .rcw-generation, the remote is fully synced locally and
  generation 1 is created on both sides. A missing local generation or an
  older local generation triggers a full remote-to-local sync. Equal versions
  skip it. A missing remote generation or a local version ahead of the remote
  is an error. Before each outgoing batch, the generation is incremented and
  published remotely before payload changes. A required remote-to-local sync
  deletes local payload paths that are absent remotely.

Shutdown and retries:
  Failed running syncs retry with exponential backoff from 1 second to 1
  minute. SIGINT or SIGTERM drains queued inotify events, performs a final
  sync, removes an owned lock, and exits. Status 0 means no changed paths are
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
