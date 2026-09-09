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

var errLockInterrupted = errors.New("interrupted while waiting for remote lock")

const lockPollInterval = time.Second

type syncFileData struct {
	Generation uint64        `json:"generation"`
	Syncing    bool          `json:"syncing"`
	Lock       *syncFileLock `json:"lock,omitempty"`
}

type syncFileLock struct {
	Owner     string    `json:"owner"`
	Timestamp time.Time `json:"timestamp"`
}

type remoteSyncFile struct {
	data   syncFileData
	etag   string
	exists bool
}

type syncState struct {
	paths            syncFilePaths
	source           string
	dest             string
	timeout          time.Duration
	wait             lockWait
	pollInterval     time.Duration
	consistentWrites bool
	failOnIncomplete bool
	logs             bool
	logger           *log.Logger
	runner           commandRunner
	owner            string

	mu         sync.Mutex
	updateMu   sync.Mutex
	remote     remoteSyncFile
	stop       chan struct{}
	done       chan struct{}
	errors     chan error
	startOnce  sync.Once
	closeOnce  sync.Once
	releaseErr error
}

func newSyncState(paths syncFilePaths, source, destination string, timeout time.Duration, wait lockWait, consistentWrites, failOnIncomplete, logs bool, logger *log.Logger, runner commandRunner) (*syncState, error) {
	ownerBytes := make([]byte, 16)
	if _, err := rand.Read(ownerBytes); err != nil {
		return nil, fmt.Errorf("create lock owner token: %w", err)
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
		owner:            hex.EncodeToString(ownerBytes),
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
	if s.failOnIncomplete && ((localExists && local.Syncing) || (remote.exists && remote.data.Syncing)) {
		return errors.New("sync file indicates an incomplete previous sync")
	}
	if localExists && !remote.exists {
		return errors.New("local sync file exists but remote sync file is missing")
	}
	localIncompleteAdvance := localExists && remote.exists && local.Syncing && local.Generation == remote.data.Generation+1
	if localExists && local.Generation > remote.data.Generation && !localIncompleteAdvance {
		return fmt.Errorf("local generation %d is ahead of remote generation %d", local.Generation, remote.data.Generation)
	}

	var deadline time.Time
	if !s.wait.infinite && s.wait.duration > 0 {
		deadline = time.Now().Add(s.wait.duration)
	}
	defaultWait := !s.wait.explicit && !s.wait.infinite && s.wait.duration == 0
	canWait := defaultWait || s.wait.infinite || s.wait.duration > 0
	observedLock := ""
	for {
		if defaultWait && observedLock != "" && remote.lockIdentity() != "" && remote.lockIdentity() != observedLock {
			return errors.New("remote lock was refreshed while waiting")
		}
		for remote.active(s.timeout) {
			if defaultWait && observedLock != "" && remote.lockIdentity() != observedLock {
				return errors.New("remote lock was refreshed while waiting")
			}
			if defaultWait && observedLock == "" {
				observedLock = remote.lockIdentity()
			}
			expires := remote.data.Lock.Timestamp.Add(s.timeout)
			if !canWait {
				return fmt.Errorf("remote lock is active until %s", expires.Format(time.RFC3339Nano))
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
				s.logger.Printf("remote lock is active until %s; checking again at %s", expires.Format(time.RFC3339Nano), wakeAt.Format(time.RFC3339Nano))
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
		if defaultWait && observedLock != "" && remote.lockIdentity() != "" && remote.lockIdentity() != observedLock {
			return errors.New("remote lock was refreshed while waiting")
		}

		candidate := remote.data
		if !remote.exists {
			candidate.Generation = 1
		}
		candidate.Lock = &syncFileLock{Owner: s.owner, Timestamp: time.Now().UTC()}
		if err := s.writeRemote(candidate, remote.etag); err != nil {
			if s.consistentWrites && canWait {
				latest, readErr := s.readRemote()
				if readErr == nil && latest.active(s.timeout) {
					if defaultWait && observedLock != "" && latest.lockIdentity() != observedLock {
						return errors.New("remote lock was refreshed while waiting")
					}
					remote = latest
					continue
				}
			}
			return err
		}
		current, err := s.readRemote()
		if err != nil {
			return err
		}
		if !current.ownedBy(s.owner) || current.data.Generation != candidate.Generation || current.data.Syncing != candidate.Syncing {
			if canWait && current.active(s.timeout) {
				if defaultWait && observedLock != "" && current.lockIdentity() != observedLock {
					return errors.New("remote lock was refreshed while waiting")
				}
				remote = current
				continue
			}
			return errors.New("remote sync file changed while acquiring lock")
		}
		if !time.Now().Before(current.data.Lock.Timestamp.Add(s.timeout)) {
			return errors.New("remote lock timeout elapsed during acquisition")
		}
		s.setRemote(current)
		if s.logs {
			s.logger.Printf("remote lock acquired")
		}
		return nil
	}
}

func (s *syncState) InitializeGeneration() error {
	local, localExists, err := readLocalSyncFile(s.paths.local)
	if err != nil {
		return err
	}
	remote := s.getRemote()
	if !localExists {
		if s.logs {
			s.logger.Printf("local sync state is absent; syncing remote to local")
		}
		if err := s.syncFromRemote(); err != nil {
			return err
		}
		return writeLocalSyncFile(s.paths.local, syncFileData{Generation: remote.data.Generation, Syncing: remote.data.Syncing})
	}
	if local.Generation < remote.data.Generation {
		if s.logs {
			s.logger.Printf("remote generation %d is newer; syncing remote to local", remote.data.Generation)
		}
		if err := s.syncFromRemote(); err != nil {
			return err
		}
		return writeLocalSyncFile(s.paths.local, syncFileData{Generation: remote.data.Generation, Syncing: remote.data.Syncing})
	}
	if local.Generation > remote.data.Generation {
		if local.Syncing && local.Generation == remote.data.Generation+1 {
			if s.logs {
				s.logger.Printf("discarding unpublished local generation %d", local.Generation)
			}
			return writeLocalSyncFile(s.paths.local, syncFileData{Generation: remote.data.Generation, Syncing: remote.data.Syncing})
		}
		return fmt.Errorf("local generation %d is ahead of remote generation %d", local.Generation, remote.data.Generation)
	}
	if local.Syncing != remote.data.Syncing {
		return writeLocalSyncFile(s.paths.local, syncFileData{Generation: remote.data.Generation, Syncing: remote.data.Syncing})
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
	if remote.data.Generation == ^uint64(0) {
		return errors.New("generation number cannot be incremented")
	}
	next := remote.data.Generation + 1
	if err := writeLocalSyncFile(s.paths.local, syncFileData{Generation: next, Syncing: true}); err != nil {
		return fmt.Errorf("increment local generation: %w", err)
	}
	candidate := remote.data
	candidate.Generation = next
	candidate.Syncing = true
	candidate.Lock = &syncFileLock{Owner: s.owner, Timestamp: time.Now().UTC()}
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
	candidate.Lock = &syncFileLock{Owner: s.owner, Timestamp: time.Now().UTC()}
	if err := s.replaceOwnedRemote(remote, candidate); err != nil {
		return fmt.Errorf("clear remote syncing flag: %w", err)
	}
	if err := writeLocalSyncFile(s.paths.local, syncFileData{Generation: candidate.Generation}); err != nil {
		return fmt.Errorf("clear local syncing flag: %w", err)
	}
	if s.logs {
		s.logger.Printf("generation %d sync completed", candidate.Generation)
	}
	return nil
}

func (s *syncState) Start() <-chan error {
	s.startOnce.Do(func() { go s.refreshLoop() })
	return s.errors
}

func (s *syncState) Close() error {
	s.closeOnce.Do(func() {
		close(s.stop)
		<-s.done
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
			s.updateMu.Lock()
			remote := s.getRemote()
			candidate := remote.data
			candidate.Lock = &syncFileLock{Owner: s.owner, Timestamp: time.Now().UTC()}
			if err := s.replaceOwnedRemote(remote, candidate); err != nil {
				s.updateMu.Unlock()
				s.errors <- err
				return
			}
			s.updateMu.Unlock()
			if s.logs {
				s.logger.Printf("remote lock refreshed")
			}
			timer.Reset(s.refreshDelay())
		}
	}
}

func (s *syncState) refreshDelay() time.Duration {
	interval := max(s.timeout/2, time.Nanosecond)
	remote := s.getRemote()
	if remote.data.Lock == nil {
		return 0
	}
	return max(time.Until(remote.data.Lock.Timestamp.Add(interval)), 0)
}

func (s *syncState) replaceOwnedRemote(previous remoteSyncFile, candidate syncFileData) error {
	latest, err := s.readRemote()
	if err != nil {
		return err
	}
	if !latest.ownedBy(s.owner) || latest.lockIdentity() != previous.lockIdentity() || latest.data.Generation != previous.data.Generation || latest.data.Syncing != previous.data.Syncing {
		return errors.New("remote sync file ownership was lost")
	}
	if err := s.writeRemote(candidate, latest.etag); err != nil {
		return err
	}
	verified, err := s.readRemote()
	if err != nil {
		return err
	}
	if verified.data.Generation != candidate.Generation || verified.data.Syncing != candidate.Syncing || !sameLock(verified.data.Lock, candidate.Lock) {
		return errors.New("remote sync file changed while updating it")
	}
	s.setRemote(verified)
	return nil
}

func (s *syncState) syncFromRemote() error {
	args := []string{"sync", s.dest, s.source, "--create-empty-src-dirs"}
	for _, filter := range s.payloadFilters() {
		args = append(args, "--exclude", "/"+filter)
	}
	return s.run(args...)
}

func (s *syncState) payloadFilters() []string {
	filters := make([]string, 0, 2)
	if s.paths.localFilter != "" {
		filters = append(filters, s.paths.localFilter)
	}
	if s.paths.remoteFilter != "" && s.paths.remoteFilter != s.paths.localFilter {
		filters = append(filters, s.paths.remoteFilter)
	}
	return filters
}

func (s *syncState) protects(path string) (exact, ancestor bool) {
	for _, filter := range s.payloadFilters() {
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
		return remoteSyncFile{}, fmt.Errorf("invalid remote sync file: %w", err)
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

func (s *syncState) setRemote(remote remoteSyncFile) {
	s.mu.Lock()
	s.remote = remote
	s.mu.Unlock()
}

func (r remoteSyncFile) active(timeout time.Duration) bool {
	return r.exists && r.data.Lock != nil && !time.Now().After(r.data.Lock.Timestamp.Add(timeout))
}

func (r remoteSyncFile) ownedBy(owner string) bool {
	return r.exists && r.data.Lock != nil && r.data.Lock.Owner == owner
}

func (r remoteSyncFile) lockIdentity() string {
	if r.data.Lock == nil {
		return ""
	}
	return r.data.Lock.Owner + "\x00" + r.data.Lock.Timestamp.Format(time.RFC3339Nano)
}

func sameLock(left, right *syncFileLock) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Owner == right.Owner && left.Timestamp.Equal(right.Timestamp)
}

func readLocalSyncFile(path string) (syncFileData, bool, error) {
	contents, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return syncFileData{}, false, nil
	}
	if err != nil {
		return syncFileData{}, false, fmt.Errorf("read local sync file: %w", err)
	}
	data, err := decodeSyncFile(contents)
	if err != nil {
		return syncFileData{}, false, fmt.Errorf("invalid local sync file: %w", err)
	}
	return data, true, nil
}

func writeLocalSyncFile(path string, data syncFileData) error {
	contents, err := encodeSyncFile(data)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, contents, 0o600)
}

func encodeSyncFile(data syncFileData) ([]byte, error) {
	if data.Generation == 0 {
		return nil, errors.New("generation must be at least 1")
	}
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
	if data.Generation == 0 {
		return syncFileData{}, errors.New("generation must be at least 1")
	}
	if data.Lock != nil && (data.Lock.Owner == "" || data.Lock.Timestamp.IsZero()) {
		return syncFileData{}, errors.New("lock owner and timestamp must be set")
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
