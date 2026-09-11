package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	errLockInterrupted   = errors.New("interrupted while waiting for remote lock")
	errLockOwnershipLost = errors.New("remote state file ownership was lost")
)

const (
	lockPollInterval     = time.Second
	localStateTempPrefix = ".rcw-tmp-"
)

type syncFileData struct {
	Generation uint64        `json:"generation"`
	SyncID     string        `json:"sync_id,omitempty"`
	Syncing    bool          `json:"syncing"`
	Active     bool          `json:"active,omitempty"` // Local session marker; never published remotely.
	Lock       *syncFileLock `json:"lock,omitempty"`
}

type syncFileLock struct {
	Owner     string        `json:"owner"`
	Timestamp time.Time     `json:"timestamp"`
	TTL       time.Duration `json:"ttl,omitempty"`
}

type remoteSyncFile struct {
	data   syncFileData
	etag   string
	exists bool
}

type syncState struct {
	paths                      syncFilePaths
	source                     string
	dest                       string
	timeout                    time.Duration
	wait                       lockWait
	pollInterval               time.Duration
	consistentWrites           bool
	failOnIncomplete           bool
	logs                       bool
	logger                     *log.Logger
	runner                     commandRunner
	owner                      string
	persistent                 bool
	excludes                   []string
	forceDeleteUntrackedRemote bool
	cancelCommands             func()

	mu            sync.Mutex
	updateMu      sync.Mutex
	remote        remoteSyncFile
	lockExpiry    time.Time
	pending       *syncFileData
	pendingExpiry time.Time
	stop          chan struct{}
	done          chan struct{}
	errors        chan error
	startOnce     sync.Once
	closeOnce     sync.Once
	releaseErr    error
}

func newSyncState(paths syncFilePaths, source, destination string, timeout time.Duration, wait lockWait, consistentWrites, failOnIncomplete, logs bool, logger *log.Logger, runner commandRunner, persistentID string) (*syncState, error) {
	owner := persistentID
	if owner == "" {
		ownerBytes := make([]byte, 16)
		if _, err := rand.Read(ownerBytes); err != nil {
			return nil, fmt.Errorf("create lock owner token: %w", err)
		}
		owner = hex.EncodeToString(ownerBytes)
	}
	return &syncState{
		paths:            paths,
		source:           source,
		dest:             destination,
		timeout:          timeout,
		wait:             wait,
		pollInterval:     lockPollInterval,
		consistentWrites: consistentWrites,
		failOnIncomplete: failOnIncomplete,
		logs:             logs,
		logger:           logger,
		runner:           runner,
		owner:            owner,
		persistent:       persistentID != "",
		stop:             make(chan struct{}),
		done:             make(chan struct{}),
		errors:           make(chan error, 1),
	}, nil
}

func (s *syncState) Acquire(interrupt <-chan os.Signal) error {
	local, localExists, err := readLocalSyncFile(s.paths.local)
	if err != nil {
		return err
	}
	remote, err := s.readRemote()
	if err != nil {
		return err
	}
	if err := s.validateAcquisition(local, localExists, remote); err != nil {
		return err
	}
	if s.persistent && remote.ownedBy(s.owner) {
		candidate := remote.data
		candidate.Lock = s.newLock()
		if err := s.replaceOwnedRemote(remote, candidate); err != nil {
			return fmt.Errorf("refresh persistent lock: %w", err)
		}
		if s.logs {
			s.logger.Printf("persistent remote lock continued and refreshed")
		}
		return nil
	}

	var deadline time.Time
	if !s.wait.infinite && s.wait.duration > 0 {
		deadline = time.Now().Add(s.wait.duration)
	}
	defaultWait := !s.wait.explicit && !s.wait.infinite && s.wait.duration == 0
	canWait := defaultWait || s.wait.infinite || s.wait.duration > 0
	observedLock := ""
	var observedAt time.Time
	for {
		for remote.data.Lock != nil {
			identity := remote.lockIdentity()
			if observedLock != identity {
				if defaultWait && observedLock != "" {
					return errors.New("remote lock was refreshed while waiting")
				}
				observedLock = identity
				observedAt = time.Now()
			}
			expires := observedAt.Add(remote.lockTTL(s.timeout))
			if !time.Now().Before(expires) {
				break
			}
			if !canWait {
				return fmt.Errorf("remote lock is active with a stored TTL of %s", remote.lockTTL(s.timeout))
			}
			if !deadline.IsZero() && !time.Now().Before(deadline) {
				return fmt.Errorf("timed out after %s waiting for remote lock", s.wait.duration)
			}
			wakeAt := time.Now().Add(s.pollInterval)
			if expires.Before(wakeAt) {
				wakeAt = expires
			}
			if !deadline.IsZero() && deadline.Before(wakeAt) {
				wakeAt = deadline
			}
			if s.logs {
				s.logger.Printf("remote lock observed with TTL %s; checking again at %s", remote.lockTTL(s.timeout), wakeAt.Format(time.RFC3339Nano))
			}
			timer := time.NewTimer(max(time.Until(wakeAt), 0))
			select {
			case <-interrupt:
				if !timer.Stop() {
					<-timer.C
				}
				return errLockInterrupted
			case <-timer.C:
			}
			remote, err = s.readRemote()
			if err != nil {
				return err
			}
		}

		// Revalidate the state observed after waiting or losing an acquisition
		// race, before making any remote write based on it.
		if err := s.validateAcquisition(local, localExists, remote); err != nil {
			return err
		}
		candidate := remote.data
		candidate.Lock = s.newLock()
		writtenAt := time.Now()
		verifyDeadline := writtenAt.Add(s.timeout - s.refreshSafetyWindow())
		watchdogDone := make(chan struct{})
		watchdog := time.AfterFunc(max(time.Until(verifyDeadline), 0), func() {
			if s.cancelCommands != nil {
				s.cancelCommands()
			}
			close(watchdogDone)
		})
		writeErr := s.writeRemote(candidate, remote.etag)
		current, err := s.verifyAcquiredLock(verifyDeadline, interrupt)
		watchdogStopped := watchdog.Stop()
		if !watchdogStopped {
			<-watchdogDone
			return errors.New("could not verify acquired lock before expiry")
		}
		if err != nil {
			if writeErr != nil {
				return writeErr
			}
			return err
		}
		if sameSyncFileData(current.data, candidate) {
			s.setOwnedRemote(current, writtenAt.Add(s.timeout))
			if s.logs {
				s.logger.Printf("remote lock acquired")
			}
			return nil
		}
		if writeErr != nil {
			if s.consistentWrites && canWait {
				if current.lockIdentity() != remote.lockIdentity() {
					if defaultWait && observedLock != "" && current.lockIdentity() != observedLock {
						return errors.New("remote lock was refreshed while waiting")
					}
					remote = current
					continue
				}
			}
			return writeErr
		}
		if !current.ownedBy(s.owner) || current.data.Generation != candidate.Generation || current.data.SyncID != candidate.SyncID || current.data.Syncing != candidate.Syncing {
			if canWait && current.data.Lock != nil {
				if defaultWait && observedLock != "" && current.lockIdentity() != observedLock {
					return errors.New("remote lock was refreshed while waiting")
				}
				remote = current
				continue
			}
			return errors.New("remote state file changed while acquiring lock")
		}
		return errLockOwnershipLost
	}
}

func (s *syncState) validateAcquisition(local syncFileData, localExists bool, remote remoteSyncFile) error {
	if s.failOnIncomplete && remote.exists && remote.data.Syncing && needsSyncFromRemote(local, localExists, remote.data) {
		return errors.New("remote state indicates an incomplete sync requiring remote-to-local reconciliation")
	}
	if !remote.exists && !s.forceDeleteUntrackedRemote {
		empty, err := s.remotePayloadEmpty()
		if err != nil {
			return fmt.Errorf("inspect untracked remote destination: %w", err)
		}
		if !empty {
			return errors.New("remote destination has no state file and is not empty; use --force-delete-untracked-remote to delete its contents and initialize it")
		}
	}
	localIncompleteAdvance := localExists && remote.exists && local.Syncing && local.Generation == remote.data.Generation+1
	if remote.exists && remote.data.Generation > 0 && localExists && local.Generation > remote.data.Generation && !localIncompleteAdvance {
		return fmt.Errorf("local generation %d is ahead of remote generation %d", local.Generation, remote.data.Generation)
	}
	return nil
}

func needsSyncFromRemote(local syncFileData, localExists bool, remote syncFileData) bool {
	if remote.Generation == 0 {
		return false
	}
	return !localExists || local.Generation < remote.Generation ||
		(local.Generation == remote.Generation && local.SyncID != remote.SyncID)
}

func (s *syncState) verifyAcquiredLock(deadline time.Time, interrupt <-chan os.Signal) (remoteSyncFile, error) {
	var lastErr error
	for {
		remote, err := s.readRemote()
		if err == nil {
			return remote, nil
		}
		lastErr = err
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return remoteSyncFile{}, fmt.Errorf("verify acquired lock before expiry: %w", lastErr)
		}
		timer := time.NewTimer(min(s.pollInterval, remaining))
		select {
		case <-interrupt:
			if !timer.Stop() {
				<-timer.C
			}
			return remoteSyncFile{}, errLockInterrupted
		case <-timer.C:
		}
	}
}

func (s *syncState) InitializeRemote() error {
	remote := s.getRemote()
	if remote.data.Generation != 0 {
		return nil
	}
	if s.logs {
		s.logger.Printf("remote repository is uninitialized; syncing local to remote")
	}
	if err := s.deleteRemotePayload(); err != nil {
		return fmt.Errorf("clear untracked remote payload: %w", err)
	}
	if err := s.syncToRemote(); err != nil {
		return fmt.Errorf("initial local-to-remote sync: %w", err)
	}

	s.updateMu.Lock()
	defer s.updateMu.Unlock()
	remote = s.getRemote()
	candidate := remote.data
	candidate.Generation = 1
	candidate.SyncID = rand.Text()
	candidate.Syncing = false
	if err := s.replaceOwnedRemote(remote, candidate); err != nil {
		return fmt.Errorf("complete remote initialization: %w", err)
	}
	if err := writeLocalSyncFile(s.paths.local, syncFileData{Generation: 1, SyncID: candidate.SyncID, Active: true}); err != nil {
		return fmt.Errorf("write initial local state: %w", err)
	}
	if s.logs {
		s.logger.Printf("remote repository initialized at generation 1")
	}
	return nil
}

func (s *syncState) InitializeGeneration() error {
	local, localExists, err := readLocalSyncFile(s.paths.local)
	if err != nil {
		return err
	}
	// Keep the previous marker for the recovery decision, but persist this
	// session before doing startup work. An absent local state must stay absent
	// until initialization/download succeeds, so a restart retries that work.
	if localExists {
		active := local
		active.Active = true
		if err := writeLocalSyncFile(s.paths.local, active); err != nil {
			return fmt.Errorf("mark local session active: %w", err)
		}
	}
	remote := s.getRemote()
	if remote.data.Generation == 0 {
		return s.InitializeRemote()
	}
	// An unpublished local advance can collide with another writer's generation.
	// Only matching sync IDs establish that equal generations describe the same upload.
	differentSync := local.Generation == remote.data.Generation && local.SyncID != remote.data.SyncID
	if needsSyncFromRemote(local, localExists, remote.data) {
		if s.logs {
			switch {
			case !localExists:
				s.logger.Printf("local sync state is absent; syncing remote to local")
			case differentSync:
				s.logger.Printf("generation %d has a different sync ID; syncing remote to local", remote.data.Generation)
			default:
				s.logger.Printf("remote generation %d is newer; syncing remote to local", remote.data.Generation)
			}
		}
		if err := s.syncFromRemote(); err != nil {
			return err
		}
		return writeLocalSyncFile(s.paths.local, syncFileData{Generation: remote.data.Generation, SyncID: remote.data.SyncID, Syncing: remote.data.Syncing, Active: true})
	}
	localIncompleteAdvance := local.Syncing && local.Generation == remote.data.Generation+1
	if local.Generation > remote.data.Generation && !localIncompleteAdvance {
		return fmt.Errorf("local generation %d is ahead of remote generation %d", local.Generation, remote.data.Generation)
	}
	incomplete := local.Syncing || remote.data.Syncing
	if incomplete || local.Active {
		if incomplete && local.Generation == remote.data.Generation && local.SyncID == "" {
			return errors.New("cannot recover incomplete sync at equal generations without sync IDs; reconcile local and remote payloads manually")
		}
		if s.logs {
			s.logger.Printf("previous sync or session is incomplete; syncing local to remote")
		}
		if err := s.BeforeRemoteChange(); err != nil {
			return err
		}
		if err := s.syncToRemote(); err != nil {
			return fmt.Errorf("recover incomplete local-to-remote sync: %w", err)
		}
		return s.AfterRemoteChange()
	}
	if s.logs {
		s.logger.Printf("local and remote generation are current at %d", local.Generation)
	}
	return nil
}

func (s *syncState) BeforeRemoteChange() error {
	s.updateMu.Lock()
	defer s.updateMu.Unlock()
	remote := s.getRemote()
	if remote.data.Generation == 0 {
		return errors.New("remote repository is not initialized")
	}
	if remote.data.Generation == ^uint64(0) {
		return errors.New("generation number cannot be incremented")
	}
	next := remote.data.Generation + 1
	syncID := rand.Text()
	if err := writeLocalSyncFile(s.paths.local, syncFileData{Generation: next, SyncID: syncID, Syncing: true, Active: true}); err != nil {
		return fmt.Errorf("increment local generation: %w", err)
	}
	candidate := remote.data
	candidate.Generation = next
	candidate.SyncID = syncID
	candidate.Syncing = true
	if err := s.replaceOwnedRemote(remote, candidate); err != nil {
		return fmt.Errorf("publish generation %d: %w", next, err)
	}
	if s.logs {
		s.logger.Printf("generation advanced to %d", next)
	}
	return nil
}

func (s *syncState) AfterRemoteChange() error {
	s.updateMu.Lock()
	defer s.updateMu.Unlock()
	remote := s.getRemote()
	candidate := remote.data
	candidate.Syncing = false
	if err := s.replaceOwnedRemote(remote, candidate); err != nil {
		return fmt.Errorf("clear remote syncing flag: %w", err)
	}
	if err := writeLocalSyncFile(s.paths.local, syncFileData{Generation: candidate.Generation, SyncID: candidate.SyncID, Active: true}); err != nil {
		return fmt.Errorf("clear local syncing flag: %w", err)
	}
	if s.logs {
		s.logger.Printf("generation %d sync completed", candidate.Generation)
	}
	return nil
}

// FinishSession is called only after the watcher has drained and every pending
// batch has succeeded. Close alone must not mark a failed session as clean.
func (s *syncState) FinishSession() error {
	s.updateMu.Lock()
	defer s.updateMu.Unlock()
	local, exists, err := readLocalSyncFile(s.paths.local)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("local state file is missing at session completion")
	}
	local.Active = false
	return writeLocalSyncFile(s.paths.local, local)
}

func (s *syncState) Start() <-chan error {
	s.startOnce.Do(func() { go s.refreshLoop() })
	return s.errors
}

func (s *syncState) Close() error {
	s.closeOnce.Do(func() {
		close(s.stop)
		<-s.done
		if s.persistent {
			if s.logs {
				s.logger.Printf("persistent remote lock retained")
			}
			return
		}
		s.updateMu.Lock()
		defer s.updateMu.Unlock()
		remote := s.getRemote()
		candidate := remote.data
		candidate.Lock = nil
		s.releaseErr = s.replaceOwnedRemote(remote, candidate)
		if s.releaseErr == nil && s.logs {
			s.logger.Printf("remote lock released")
		}
	})
	return s.releaseErr
}

func (s *syncState) refreshLoop() {
	defer close(s.done)
	defer close(s.errors)
	timer := time.NewTimer(s.refreshDelay())
	defer timer.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-timer.C:
			watchdog, watchdogResult := s.startRefreshWatchdog()
			s.updateMu.Lock()
			if s.stopRefreshWatchdog(watchdog, watchdogResult) || s.refreshDeadlinePassed() {
				s.updateMu.Unlock()
				s.failRefresh(errors.New("could not refresh remote lock before expiry"))
				return
			}
			remote := s.getRemote()
			candidate := remote.data
			candidate.Lock = s.newLock()
			watchdog, watchdogResult = s.startRefreshWatchdog()
			err := s.replaceOwnedRemote(remote, candidate)
			watchdogExpired := s.stopRefreshWatchdog(watchdog, watchdogResult)
			s.updateMu.Unlock()
			if watchdogExpired {
				s.failRefresh(errors.New("could not refresh remote lock before expiry"))
				return
			}
			if err != nil {
				if !errors.Is(err, errLockOwnershipLost) {
					if delay, ok := s.refreshRetryDelay(); ok {
						if s.logs {
							s.logger.Printf("remote lock refresh failed; retrying in %s: %v", delay, err)
						}
						timer.Reset(delay)
						continue
					}
					err = fmt.Errorf("refresh remote lock before expiry: %w", err)
				}
				s.failRefresh(err)
				return
			}
			if s.logs {
				s.logger.Printf("remote lock refreshed")
			}
			timer.Reset(s.refreshDelay())
		}
	}
}

func (s *syncState) startRefreshWatchdog() (*time.Timer, <-chan bool) {
	expires := s.getLockExpiry()
	result := make(chan bool, 1)
	timer := time.AfterFunc(max(time.Until(expires.Add(-s.refreshSafetyWindow())), 0), func() {
		expired := !s.getLockExpiry().After(expires)
		if expired && s.cancelCommands != nil {
			s.cancelCommands()
		}
		result <- expired
	})
	return timer, result
}

func (*syncState) stopRefreshWatchdog(timer *time.Timer, result <-chan bool) bool {
	if timer.Stop() {
		return false
	}
	return <-result
}

func (s *syncState) failRefresh(err error) {
	if s.cancelCommands != nil {
		s.cancelCommands()
	}
	s.errors <- err
}

func (s *syncState) refreshDeadlinePassed() bool {
	expires := s.getLockExpiry()
	if expires.IsZero() {
		return true
	}
	return !time.Now().Before(expires.Add(-s.refreshSafetyWindow()))
}

func (s *syncState) refreshRetryDelay() (time.Duration, bool) {
	expires := s.getLockExpiry()
	if expires.IsZero() {
		return 0, false
	}
	remaining := time.Until(expires.Add(-s.refreshSafetyWindow()))
	if remaining <= 0 {
		return 0, false
	}
	return min(s.pollInterval, remaining), true
}

func (s *syncState) refreshSafetyWindow() time.Duration {
	return max(min(s.timeout/20, time.Second), time.Nanosecond)
}

func (s *syncState) refreshDelay() time.Duration {
	interval := max(s.timeout/2, time.Nanosecond)
	expires := s.getLockExpiry()
	if expires.IsZero() {
		return 0
	}
	return max(time.Until(expires.Add(-s.timeout+interval)), 0)
}

func (s *syncState) newLock() *syncFileLock {
	return &syncFileLock{Owner: s.owner, Timestamp: time.Now().UTC(), TTL: s.timeout}
}

func (s *syncState) replaceOwnedRemote(previous remoteSyncFile, candidate syncFileData) error {
	latest, err := s.readRemote()
	if err != nil {
		return err
	}
	latestIsPrevious := sameSyncFileData(latest.data, previous.data)
	latestIsPending := s.pending != nil && sameSyncFileData(latest.data, *s.pending)
	if !latest.ownedBy(s.owner) || (!latestIsPrevious && !latestIsPending) {
		s.pending = nil
		s.pendingExpiry = time.Time{}
		return errLockOwnershipLost
	}
	expires := s.getLockExpiry()
	if latestIsPending {
		expires = s.pendingExpiry
		if sameLock(candidate.Lock, previous.data.Lock) {
			candidate.Lock = latest.data.Lock
		}
	}
	writtenAt := time.Now()
	if candidate.Lock == nil {
		expires = time.Time{}
	} else if !sameLock(candidate.Lock, latest.data.Lock) {
		expires = writtenAt.Add(candidate.Lock.ttl(s.timeout))
	}
	pending := candidate
	s.pending = &pending
	s.pendingExpiry = expires
	if err := s.writeRemote(candidate, latest.etag); err != nil {
		return err
	}
	verified, err := s.readRemote()
	if err != nil {
		return err
	}
	if !sameSyncFileData(verified.data, candidate) {
		if sameSyncFileData(verified.data, latest.data) {
			return errors.New("remote state file changed while updating it")
		}
		s.pending = nil
		s.pendingExpiry = time.Time{}
		return errLockOwnershipLost
	}
	if candidate.Lock != nil && !verified.ownedBy(s.owner) {
		s.pending = nil
		s.pendingExpiry = time.Time{}
		return errLockOwnershipLost
	}
	s.pending = nil
	s.pendingExpiry = time.Time{}
	s.setOwnedRemote(verified, expires)
	return nil
}

func (s *syncState) syncFromRemote() error {
	return s.run(payloadSyncArgs(s.dest, s.source, s.payloadFilters(), s.excludes)...)
}

func (s *syncState) syncToRemote() error {
	return s.run(payloadSyncArgs(s.source, s.dest, s.payloadFilters(), s.excludes)...)
}

func (s *syncState) deleteRemotePayload() error {
	args := []string{"delete", s.dest}
	if s.paths.remoteFilter != "" {
		args = append(args, "--exclude", "/"+literalFilterPath(s.paths.remoteFilter))
	}
	if err := s.run(args...); err != nil {
		if isRcloneNotFound(err) {
			return nil
		}
		return err
	}
	// Inspect directories without filtering out the state file; otherwise
	// rclone can mistake its parent for an empty directory and fail to remove it.
	if err := s.run("rmdirs", s.dest, "--leave-root"); err != nil && !isRcloneNotFound(err) {
		return err
	}
	return nil
}

func (s *syncState) remotePayloadEmpty() (bool, error) {
	var stdout, stderr bytes.Buffer
	if err := s.runner.Run([]string{"lsjson", s.dest, "--recursive"}, &stdout, &stderr); err != nil {
		if isRcloneNotFound(err) {
			return true, nil
		}
		return false, commandError(err, stderr.String())
	}
	var entries []json.RawMessage
	if err := json.Unmarshal(stdout.Bytes(), &entries); err != nil {
		return false, fmt.Errorf("decode remote listing: %w", err)
	}
	return len(entries) == 0, nil
}

func (s *syncState) payloadFilters() []string {
	filters := make([]string, 0, 3)
	if s.paths.localFilter != "" {
		filters = append(filters, literalFilterPath(s.paths.localFilter+localStateTempPrefix)+"*")
		filters = append(filters, literalFilterPath(s.paths.localFilter))
	}
	if s.paths.remoteFilter != "" && s.paths.remoteFilter != s.paths.localFilter {
		filters = append(filters, literalFilterPath(s.paths.remoteFilter))
	}
	return filters
}

func (s *syncState) protects(path string) (exact, ancestor bool) {
	if s.paths.localFilter != "" && strings.HasPrefix(path, s.paths.localFilter+localStateTempPrefix) {
		return true, false
	}
	for _, filter := range []string{s.paths.localFilter, s.paths.remoteFilter} {
		if filter == "" {
			continue
		}
		if path == filter {
			return true, false
		}
		if strings.HasPrefix(filter, path+"/") {
			ancestor = true
		}
	}
	return false, ancestor
}

func (s *syncState) readRemote() (remoteSyncFile, error) {
	var stdout, stderr bytes.Buffer
	args := []string{"cat", s.paths.remote}
	if s.consistentWrites {
		args = append(args, "--dump", "headers", "--log-level", "DEBUG")
	}
	if err := s.runner.Run(args, &stdout, &stderr); err != nil {
		if isRcloneNotFound(err) {
			return remoteSyncFile{}, nil
		}
		return remoteSyncFile{}, commandError(err, stderr.String())
	}
	data, err := decodeSyncFile(stdout.Bytes())
	if err != nil {
		return remoteSyncFile{}, fmt.Errorf("invalid remote state file: %w", err)
	}
	etag := ""
	if s.consistentWrites {
		etag = responseETag(stderr.String())
		if etag == "" {
			return remoteSyncFile{}, errors.New("S3 response did not include an ETag; use --no-consistent-writes if this provider cannot supply one")
		}
	}
	return remoteSyncFile{data: data, etag: etag, exists: true}, nil
}

func (s *syncState) writeRemote(data syncFileData, previousETag string) error {
	data.Active = false
	contents, err := encodeSyncFile(data)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp("", "rclonewatch-sync-*")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if _, err := file.Write(contents); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	args := []string{"copyto", name, s.paths.remote}
	if s.consistentWrites {
		header := "If-None-Match: *"
		if previousETag != "" {
			header = fmt.Sprintf("If-Match: \"%s\"", previousETag)
		}
		args = append(args, "--header-upload", header)
	}
	return s.run(args...)
}

func (s *syncState) run(args ...string) error {
	if s.logs {
		return s.runner.Run(args, os.Stdout, os.Stdout)
	}
	var stdout, stderr bytes.Buffer
	if err := s.runner.Run(args, &stdout, &stderr); err != nil {
		return commandError(err, stderr.String())
	}
	return nil
}

func (s *syncState) getRemote() remoteSyncFile {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.remote
}

func (s *syncState) getLockExpiry() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lockExpiry
}

func (s *syncState) setOwnedRemote(remote remoteSyncFile, expires time.Time) {
	s.mu.Lock()
	s.remote = remote
	s.lockExpiry = expires
	s.mu.Unlock()
}

func (r remoteSyncFile) lockTTL(fallback time.Duration) time.Duration {
	return r.data.Lock.ttl(fallback)
}

func (l *syncFileLock) ttl(fallback time.Duration) time.Duration {
	if l == nil || l.TTL <= 0 {
		return fallback
	}
	return l.TTL
}

func (r remoteSyncFile) ownedBy(owner string) bool {
	return r.exists && r.data.Lock != nil && r.data.Lock.Owner == owner
}

func (r remoteSyncFile) lockIdentity() string {
	if r.data.Lock == nil {
		return ""
	}
	return r.data.Lock.Owner + "\x00" + r.data.Lock.Timestamp.Format(time.RFC3339Nano) + "\x00" + r.data.Lock.TTL.String()
}

func sameLock(left, right *syncFileLock) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Owner == right.Owner && left.Timestamp.Equal(right.Timestamp) && left.TTL == right.TTL
}

func sameSyncFileData(left, right syncFileData) bool {
	return left.Generation == right.Generation && left.SyncID == right.SyncID && left.Syncing == right.Syncing && sameLock(left.Lock, right.Lock)
}

func readLocalSyncFile(path string) (syncFileData, bool, error) {
	contents, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return syncFileData{}, false, nil
	}
	if err != nil {
		return syncFileData{}, false, fmt.Errorf("read local state file: %w", err)
	}
	data, err := decodeSyncFile(contents)
	if err != nil {
		return syncFileData{}, false, fmt.Errorf("invalid local state file: %w", err)
	}
	if data.Generation == 0 {
		return syncFileData{}, false, errors.New("invalid local state file: generation must be at least 1")
	}
	return data, true, nil
}

func writeLocalSyncFile(path string, data syncFileData) error {
	if data.Generation == 0 {
		return errors.New("generation must be at least 1")
	}
	// Local state always includes active, including false after a clean exit.
	contents, err := json.MarshalIndent(struct {
		syncFileData
		Active bool `json:"active"`
	}{syncFileData: data, Active: data.Active}, "", "  ")
	if err != nil {
		return err
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	file, err := os.CreateTemp(directory, filepath.Base(path)+localStateTempPrefix+"*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if _, err := file.Write(append(contents, '\n')); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), path); err != nil {
		return err
	}
	parent, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer parent.Close()
	return parent.Sync()
}

func encodeSyncFile(data syncFileData) ([]byte, error) {
	contents, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(contents, '\n'), nil
}

func decodeSyncFile(contents []byte) (syncFileData, error) {
	var data syncFileData
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&data); err != nil {
		return syncFileData{}, err
	}
	if data.Lock != nil && (data.Lock.Owner == "" || data.Lock.Timestamp.IsZero()) {
		return syncFileData{}, errors.New("lock owner and timestamp must be set")
	}
	if data.Lock != nil && data.Lock.TTL < 0 {
		return syncFileData{}, errors.New("lock TTL must not be negative")
	}
	return data, nil
}

func responseETag(output string) string {
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if key, value, found := strings.Cut(line, ":"); found && strings.EqualFold(key, "etag") {
			return strings.Trim(strings.TrimSpace(value), `"`)
		}
	}
	return ""
}

func isRcloneNotFound(err error) bool {
	var exitError *exec.ExitError
	return errors.As(err, &exitError) && (exitError.ExitCode() == 3 || exitError.ExitCode() == 4)
}

func commandError(err error, output string) error {
	if message := strings.TrimSpace(output); message != "" {
		return fmt.Errorf("%w: %s", err, message)
	}
	return err
}
