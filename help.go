package main

import (
	"fmt"
	"io"
)

func printUsage(output io.Writer) {
	fmt.Fprintln(output, "Usage: rclonewatch [OPTIONS] SOURCE_FOLDER RCLONE_DESTINATION [-- COMMAND [ARG...]]")
}

func printHelp(output io.Writer) {
	fmt.Fprint(output, `rclonewatch watches a local Linux directory recursively with inotify and
syncs deduplicated changed paths to an rclone destination.

Usage:
  rclonewatch [OPTIONS] SOURCE_FOLDER RCLONE_DESTINATION [-- COMMAND [ARG...]]

Options:
  --interval DURATION
        Wait this long after a successful sync before syncing another batch.
        Durations use Go syntax, such as 30s, 5m, or 1h30m. Without this
        option, changes are synced only during shutdown.

  --lock-ttl DURATION
        Set the remote lock expiry duration. The default is 2m, and the lock
        timestamp is refreshed every DURATION/2. The TTL is stored with the
        lock. DURATION must be positive.

  --lock-wait 0|DURATION|inf
        How long to wait when an active lock exists. By default, wait for the
        observed lock to expire but fail if it is refreshed. An explicit 0
        exits immediately. A duration waits up to that total time while
        following refreshes. Lock expiry is measured locally from observation
        using the stored TTL, never from the lock timestamp, to avoid clock
        skew. inf waits indefinitely. Units are s, m, h.

  --persistent-lock ID
        Hold the lock under ID and retain it when rclonewatch exits. A later
        invocation using the same ID continues that lock without waiting for
        expiry and immediately refreshes it with the configured TTL.

  --upload-only
        Only send local changes to the destination. Do not create, read, or
        update sync state, acquire a remote lock, or reconcile remote changes.
        Lock and state options cannot be used in this mode.

  --exclude GLOB
        Exclude files matching an rclone filter glob. Repeat this option for
        multiple patterns. Applies to outgoing syncs and startup reconciliation.

  --no-consistent-writes
        Disable conditional state writes on direct S3-compatible destinations.
        By default S3 writes use If-None-Match/If-Match and require rclone
        v1.73.0 or newer. Use this only for providers without conditional
        write support. Cannot be used with --upload-only.

  --force-delete-untracked-remote
        Initialize a non-empty destination that has no .rcw-state file by
        deleting its existing payload and fully syncing local to remote.
        Cannot be used with --upload-only.

  --fail-on-incomplete-sync
        Exit with status 1 before lock acquisition or remote writes when the
        local or remote .rcw-state has syncing set to true. Cannot be used with
        --upload-only.

  --state-file PATH
        Use PATH, relative to each root and including the filename, for both
        local and remote sync state. Cannot be combined with the path pair.

  --state-file-local PATH
  --state-file-remote PATH
        Set different local and remote sync-state paths. Both must be supplied.
        Paths are relative to their respective roots; .. components may place
        state outside a root. State path options cannot be used with
        --upload-only.

  --logs
        Write diagnostics, status, changed paths, and rclone output to stdout.
        Without this option, rclonewatch writes no runtime output itself.

  -h, --help
        Show this help and exit.

Wrapped command:
  Arguments after -- are run as a command once inotify is ready. rclonewatch
  passes through its standard input, output, and error, watches until it exits,
  then drains events, performs a final sync, and returns the command's status
  if syncing succeeds. SIGINT and SIGTERM are forwarded to the command, whose
  exit is awaited before final shutdown.

State file behavior:
  By default, rclonewatch coordinates writers and reconciles remote changes
  through .rcw-state. It combines the persistent generation, incomplete-sync
  flag, and optional lock owner, timestamp, and TTL. Unlocking clears the lock
  fields but does not delete the file. A destination without remote state must
  be empty unless --force-delete-untracked-remote is supplied. Initialization
  locks remote generation 0, clears its payload, fully syncs local to remote, then
  promotes both state files to generation 1. If local state is absent or older,
  startup performs a full remote-to-local sync. Equal generations skip it. A
  completed local generation ahead of remote is an error. Before each outgoing
  batch, generation is incremented and syncing is set true on both sides before
  payload changes. It is cleared only after the entire batch succeeds, so
  failures remain detectable on the next run. A required remote-to-local sync
  deletes local payload absent remotely.

  State paths inside either payload root are excluded from payload transfers.
  When local and remote paths differ, both relative names are reserved.

Shutdown and retries:
  Failed running syncs retry with exponential backoff from 1 second to 1
  minute. Failed lock refreshes retry until shortly before the current lock
  expires. If ownership is lost or the lock cannot be refreshed by then,
  active rclone work and the wrapped command are terminated without a final
  sync. SIGINT or SIGTERM drains queued inotify events, performs a final sync,
  clears an owned non-persistent lock, and exits. Runtime failures use status 1
  and usage errors status 2.

Examples:
  # Wrap a command, periodically sync, and retain a reusable lock.
  rclonewatch --interval 30s \
    --persistent-lock build-sequence --exclude '*.tmp' --logs \
    /srv/data remote:backup/data -- ./build.sh --release

  # Periodically upload local changes without remote reconciliation or state.
  rclonewatch --upload-only --interval 5m /srv/data remote:backup/data

  # Coordinate writers and pull newer remote state before watching.
  rclonewatch --interval 30s --lock-ttl 2m --lock-wait inf \
    --logs /srv/data s3:bucket/data

  # Use an S3-compatible provider without conditional-write support.
  rclonewatch --no-consistent-writes \
    /srv/data s3clone:bucket/data
`)
}
