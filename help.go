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
        Exit with status 1 before lock acquisition or remote writes when remote
        .rcw-state has syncing set to true and startup would fully sync remote
        to local. Cannot be used with --upload-only.

  --state-file PATH
        Use PATH, relative to each root and including the filename, for both
        local and remote sync state. Cannot be combined with the path pair.
        Glob characters are literal; paths containing line breaks are rejected.

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

Watching and payload transfers:
  A symlink source root is resolved before watching. Watches are installed before
  startup reconciliation, and changes are collected throughout that work.
  Inotify overflow rebuilds recursive watches and requests a full sync.
  Changed paths and subtrees are reconciled using root-relative scope filters;
  overlapping scopes are collapsed, and unrelated sibling payload is preserved.
  Directory deletion is idempotent. Missing or replaced parents expand a scope
  only to the affected ancestor. Syncs delete before copying to resolve type
  conflicts; excluded payload and metadata remain protected. Failed scopes are
  retained for retry. Full-root syncs are used for startup reconciliation,
  recovery, and explicit rescans. Unrepresentable names use the nearest safe
  ancestor scope. Transfers use --ignore-times so matching size/mtime cannot hide
  changed content; only files included within the selected scopes are recopied.

State file behavior:
  By default, rclonewatch coordinates writers and reconciles remote changes
  through .rcw-state. It combines the persistent generation, unique sync_id,
  incomplete-sync flag, and optional lock owner, timestamp, and TTL. Unlocking
  clears the lock fields but does not delete the file. A destination without
  remote state must be empty unless --force-delete-untracked-remote is supplied.
  Initialization locks remote generation 0, clears its payload, fully syncs
  local to remote, then promotes both state files to generation 1. If local state
  is absent or older, startup performs a full remote-to-local sync, even if
  syncing is true. Equal generations with different sync IDs also trigger a
  remote-to-local sync.
  Matching generations and sync IDs skip reconciliation unless either syncing
  flag or the previous local active marker is true. In those cases startup
  performs a full local-to-remote sync to recover an interrupted upload or
  session, including changes queued before any batch started. An
  incomplete local generation one ahead of remote is recovered this way too. A
  completed local generation ahead of remote is an error. Before each outgoing
  batch, generation is incremented, a fresh sync ID is written locally then
  remotely, and syncing is set true on both sides before payload changes.
  The sync ID is retained after completion. The syncing flag is cleared only
  after the entire batch succeeds, so failures remain detectable on the next
  run. A required remote-to-local sync deletes local payload absent remotely.

  The local-only active marker is set true at startup and stays true across
  interval batches. It is cleared only after the watcher drains and all pending
  changes sync successfully at shutdown, even if the wrapped command exits
  nonzero. Failed recovery, final sync, or watching leaves it true. Remote state
  is never written with this marker. Newer remote generations and conflicting
  sync IDs retain remote-to-local reconciliation priority after a broken session.

  Legacy state without sync IDs remains readable. Incomplete states at equal
  generations without IDs on either side require manual reconciliation because
  the upload identity is ambiguous. Completed legacy states gain IDs on upload.
  A missing active marker means inactive; completed legacy states can recover
  an active session. Older readers may reject local state containing active.

  State paths inside either payload root are excluded from payload transfers.
  When local and remote paths differ, both relative names are reserved.
  Local updates use synced temporary files and atomic rename. Sibling names
  matching <local-state-file>.rcw-tmp-* are reserved and excluded as well.

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
