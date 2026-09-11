package main

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

const testRepositoryID = "11111111-1111-4111-8111-111111111111"

type memorySyncRunner struct {
	mu                  sync.Mutex
	value               string
	lastCopyArgs        []string
	copyCount           int
	copyAttempts        int
	conditionalFailures int
	refreshFailures     int
	ambiguousFailures   int
	payloadErr          error
	payloadRan          bool
	payloadSyncID       string
	requireGeneration   uint64
	requireSyncing      bool
	requireLocalState   string
	remoteFiles         bool
}

func (r *memorySyncRunner) Run(args []string, stdout, stderr io.Writer) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch args[0] {
	case "lsjson":
		if r.remoteFiles {
			_, err := io.WriteString(stdout, `[{"Path":"existing.txt","IsDir":false}]`)
			return err
		}
		_, err := io.WriteString(stdout, "[]")
		return err
	case "cat":
		if r.value == "" {
			return exec.Command("sh", "-c", "exit 4").Run()
		}
		for _, arg := range args {
			if arg == "--dump" {
				hash := md5.Sum([]byte(r.value + "\n"))
				_, _ = fmt.Fprintf(stderr, "Etag: \"%s\"\n", hex.EncodeToString(hash[:]))
				break
			}
		}
		_, err := io.WriteString(stdout, r.value+"\n")
		return err
	case "copyto":
		r.copyAttempts++
		if r.refreshFailures > 0 {
			r.refreshFailures--
			return errors.New("transient refresh failure")
		}
		if r.conditionalFailures > 0 {
			r.conditionalFailures--
			r.value = encodeTestSyncFile(syncFileData{
				RepositoryID: testRepositoryID,
				Generation:   1,
				Lock: &syncFileLock{
					Owner:     "competing-writer",
					Timestamp: time.Now().UTC(),
				},
			})
			return errors.New("conditional write failed")
		}
		contents, err := os.ReadFile(args[1])
		if err != nil {
			return err
		}
		r.value = strings.TrimSpace(string(contents))
		r.lastCopyArgs = append([]string(nil), args...)
		r.copyCount++
		if r.ambiguousFailures > 0 {
			r.ambiguousFailures--
			return errors.New("ambiguous write failure")
		}
		return nil
	default:
		data, err := decodeSyncFile([]byte(r.value))
		if err != nil {
			return fmt.Errorf("payload command observed invalid sync state: %w", err)
		}
		if r.requireGeneration != 0 && data.Generation != r.requireGeneration {
			return fmt.Errorf("payload command observed generation %d, want %d", data.Generation, r.requireGeneration)
		}
		if r.requireSyncing && !data.Syncing {
			return errors.New("payload command ran without syncing flag")
		}
		if r.requireLocalState != "" {
			local, exists, err := readLocalSyncFile(r.requireLocalState)
			if err != nil || !exists || data.SyncID == "" || local.SyncID != data.SyncID || local.Generation != data.Generation || !local.Syncing || !local.Active {
				return fmt.Errorf("payload command observed inconsistent local state: %#v, remote: %#v, error: %v", local, data, err)
			}
		}
		r.payloadRan = true
		r.payloadSyncID = data.SyncID
		return r.payloadErr
	}
}

func (r *memorySyncRunner) setData(data syncFileData) {
	if data.RepositoryID == "" {
		data.RepositoryID = testRepositoryID
	}
	r.mu.Lock()
	r.value = encodeTestSyncFile(data)
	r.mu.Unlock()
}

func (r *memorySyncRunner) data(t *testing.T) syncFileData {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	data, err := decodeSyncFile([]byte(r.value))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func (r *memorySyncRunner) copyArgs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.lastCopyArgs...)
}

func (r *memorySyncRunner) writes() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.copyCount
}

func (r *memorySyncRunner) attempts() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.copyAttempts
}

func (r *memorySyncRunner) setRefreshFailures(count int) {
	r.mu.Lock()
	r.refreshFailures = count
	r.mu.Unlock()
}

func (r *memorySyncRunner) setAmbiguousFailures(count int) {
	r.mu.Lock()
	r.ambiguousFailures = count
	r.mu.Unlock()
}

type blockingRefreshRunner struct {
	*memorySyncRunner
	block   <-chan struct{}
	started chan struct{}
	enabled bool
	once    sync.Once
}

func (r *blockingRefreshRunner) Run(args []string, stdout, stderr io.Writer) error {
	if args[0] == "copyto" && r.enabled {
		r.once.Do(func() { close(r.started) })
		<-r.block
		return errors.New("blocked refresh cancelled")
	}
	return r.memorySyncRunner.Run(args, stdout, stderr)
}

func newMemoryState(t *testing.T, runner *memorySyncRunner, timeout time.Duration, wait lockWait, consistent, failOnIncomplete bool, persistentID ...string) (*syncState, string) {
	t.Helper()
	source := t.TempDir()
	paths := syncFilePaths{
		local:        filepath.Join(source, defaultStateFile),
		remote:       "remote:destination/" + defaultStateFile,
		localFilter:  defaultStateFile,
		remoteFilter: defaultStateFile,
	}
	id := ""
	if len(persistentID) > 0 {
		id = persistentID[0]
	}
	state, err := newSyncState(paths, source, "remote:destination", timeout, wait, consistent, failOnIncomplete, false, log.New(io.Discard, "", 0), runner, id)
	if err != nil {
		t.Fatal(err)
	}
	return state, source
}

func TestSyncStateLifecycleWithLocalRclone(t *testing.T) {
	requireRclone(t)
	source := t.TempDir()
	destination := t.TempDir()
	paths, err := resolveSyncFilePaths(source, destination, defaultStateFile, defaultStateFile)
	if err != nil {
		t.Fatal(err)
	}
	state, err := newSyncState(paths, source, destination, 10*time.Second, lockWait{}, false, false, false, log.New(io.Discard, "", 0), &rcloneCommand{}, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	state.Start()
	if err := state.InitializeRemote(); err != nil {
		t.Fatal(err)
	}
	initial, _, err := readLocalSyncFile(paths.remote)
	if err != nil || initial.Lock == nil {
		t.Fatalf("initial remote state = %#v, err = %v", initial, err)
	}

	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	assertSyncState(t, paths.remote, 1, false, false)
}

func TestSyncStateRetriesLockRefresh(t *testing.T) {
	runner := &memorySyncRunner{}
	runner.setData(syncFileData{Generation: 1, SyncID: "retained"})
	state, _ := newMemoryState(t, runner, 200*time.Millisecond, lockWait{}, false, false)
	state.pollInterval = 10 * time.Millisecond
	if err := state.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	initialAttempts := runner.attempts()
	runner.setRefreshFailures(2)
	errors := state.Start()

	deadline := time.Now().Add(time.Second)
	for runner.writes() < 2 {
		select {
		case err := <-errors:
			t.Fatalf("refresh failed instead of retrying: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("lock refresh did not recover")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if attempts := runner.attempts() - initialAttempts; attempts < 3 {
		t.Fatalf("refresh attempts = %d, want at least 3", attempts)
	}
	if runner.data(t).SyncID != "retained" {
		t.Fatal("lock refresh changed the sync ID")
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSyncStateRecoversAmbiguousLockRefresh(t *testing.T) {
	runner := &memorySyncRunner{}
	runner.setData(syncFileData{Generation: 1})
	state, _ := newMemoryState(t, runner, 200*time.Millisecond, lockWait{}, false, false)
	state.pollInterval = 10 * time.Millisecond
	if err := state.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	initialAttempts := runner.attempts()
	runner.setAmbiguousFailures(1)
	errors := state.Start()

	deadline := time.Now().Add(time.Second)
	for runner.attempts()-initialAttempts < 2 {
		select {
		case err := <-errors:
			t.Fatalf("ambiguous refresh was treated as ownership loss: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("ambiguous refresh was not recovered")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSyncStatePreservesAmbiguousRefreshDuringStateUpdate(t *testing.T) {
	runner := &memorySyncRunner{}
	runner.setData(syncFileData{Generation: 1})
	state, _ := newMemoryState(t, runner, time.Hour, lockWait{}, false, false)
	if err := state.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	previous := state.getRemote()
	candidate := previous.data
	candidate.Lock = state.newLock()
	runner.setAmbiguousFailures(1)
	state.updateMu.Lock()
	err := state.replaceOwnedRemote(previous, candidate)
	state.updateMu.Unlock()
	if err == nil {
		t.Fatal("ambiguous refresh unexpectedly succeeded")
	}
	refreshed := runner.data(t)
	if sameLock(refreshed.Lock, previous.data.Lock) {
		t.Fatal("ambiguous refresh did not change the remote lock")
	}

	if err := state.BeforeRemoteChange(); err != nil {
		t.Fatal(err)
	}
	remote := runner.data(t)
	if remote.Generation != 2 || !sameLock(remote.Lock, refreshed.Lock) {
		t.Fatalf("state update restored the old lock: %#v", remote)
	}
	state.Start()
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSyncStateAbortsAtRefreshDeadline(t *testing.T) {
	runner := &memorySyncRunner{}
	runner.setData(syncFileData{Generation: 1})
	state, _ := newMemoryState(t, runner, 160*time.Millisecond, lockWait{}, false, false)
	state.pollInterval = 10 * time.Millisecond
	if err := state.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	runner.setRefreshFailures(100)
	cancelled := make(chan struct{})
	var cancelOnce sync.Once
	state.cancelCommands = func() { cancelOnce.Do(func() { close(cancelled) }) }

	select {
	case err := <-state.Start():
		if err == nil || !strings.Contains(err.Error(), "before expiry") {
			t.Fatalf("refresh error = %v, want expiry error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("refresh failure did not reach its deadline")
	}
	select {
	case <-cancelled:
	default:
		t.Fatal("commands were not cancelled")
	}
	if attempts := runner.attempts(); attempts < 3 {
		t.Fatalf("total lock write attempts = %d, want at least 3", attempts)
	}

	runner.setRefreshFailures(0)
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSyncStateAbortsBlockedRefreshAtDeadline(t *testing.T) {
	block := make(chan struct{})
	runner := &blockingRefreshRunner{
		memorySyncRunner: &memorySyncRunner{},
		block:            block,
		started:          make(chan struct{}),
	}
	runner.setData(syncFileData{Generation: 1})
	state, _ := newMemoryState(t, runner.memorySyncRunner, 160*time.Millisecond, lockWait{}, false, false)
	state.runner = runner
	if err := state.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	runner.enabled = true
	cancelled := make(chan struct{})
	var cancelOnce sync.Once
	state.cancelCommands = func() {
		cancelOnce.Do(func() {
			close(cancelled)
			close(block)
		})
	}
	errors := state.Start()

	select {
	case <-runner.started:
	case <-time.After(time.Second):
		t.Fatal("refresh did not start")
	}
	select {
	case err := <-errors:
		if err == nil || !strings.Contains(err.Error(), "before expiry") {
			t.Fatalf("refresh error = %v, want expiry error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked refresh was not cancelled at its deadline")
	}
	select {
	case <-cancelled:
	default:
		t.Fatal("commands were not cancelled")
	}
	runner.enabled = false
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSyncStateWatchdogCoversBlockedStateUpdate(t *testing.T) {
	block := make(chan struct{})
	runner := &blockingRefreshRunner{
		memorySyncRunner: &memorySyncRunner{},
		block:            block,
		started:          make(chan struct{}),
	}
	runner.setData(syncFileData{Generation: 1})
	state, _ := newMemoryState(t, runner.memorySyncRunner, 160*time.Millisecond, lockWait{}, false, false)
	state.runner = runner
	if err := state.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	cancelled := make(chan struct{})
	var cancelOnce sync.Once
	state.cancelCommands = func() {
		cancelOnce.Do(func() {
			close(cancelled)
			close(block)
		})
	}
	errors := state.Start()
	runner.enabled = true
	updateDone := make(chan error, 1)
	go func() { updateDone <- state.BeforeRemoteChange() }()

	select {
	case <-runner.started:
	case <-time.After(time.Second):
		t.Fatal("state update did not start")
	}
	select {
	case err := <-errors:
		if err == nil || !strings.Contains(err.Error(), "before expiry") {
			t.Fatalf("refresh error = %v, want expiry error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked state update was not cancelled at the refresh deadline")
	}
	if err := <-updateDone; err == nil {
		t.Fatal("cancelled state update succeeded")
	}
	select {
	case <-cancelled:
	default:
		t.Fatal("commands were not cancelled")
	}
	runner.enabled = false
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSyncStateWaitModes(t *testing.T) {
	t.Run("explicit zero", func(t *testing.T) {
		runner := &memorySyncRunner{}
		runner.setData(activeForeignState())
		state, _ := newMemoryState(t, runner, time.Hour, lockWait{explicit: true}, false, false)
		started := time.Now()
		err := state.Acquire(make(chan os.Signal))
		if err == nil || !strings.Contains(err.Error(), "remote lock is active") {
			t.Fatalf("Acquire error = %v, want active-lock error", err)
		}
		if time.Since(started) > 100*time.Millisecond {
			t.Fatal("explicit zero lock wait did not return immediately")
		}
	})

	t.Run("default expiry", func(t *testing.T) {
		runner := &memorySyncRunner{}
		runner.setData(syncFileData{Generation: 1, Lock: &syncFileLock{
			Owner:     "foreign",
			Timestamp: time.Now().Add(24 * time.Hour).UTC(),
			TTL:       60 * time.Millisecond,
		}})
		state, _ := newMemoryState(t, runner, time.Hour, lockWait{}, false, false)
		state.pollInterval = 10 * time.Millisecond
		started := time.Now()
		if err := state.Acquire(make(chan os.Signal)); err != nil {
			t.Fatal(err)
		}
		if time.Since(started) < 40*time.Millisecond {
			t.Fatal("default lock wait acquired before the observed lock expired")
		}
		state.Start()
		if err := state.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("default rejects refresh", func(t *testing.T) {
		runner := &memorySyncRunner{}
		runner.setData(activeForeignState())
		state, _ := newMemoryState(t, runner, 200*time.Millisecond, lockWait{}, false, false)
		state.pollInterval = 10 * time.Millisecond
		go func() {
			time.Sleep(30 * time.Millisecond)
			runner.setData(activeForeignState())
		}()
		err := state.Acquire(make(chan os.Signal))
		if err == nil || !strings.Contains(err.Error(), "refreshed while waiting") {
			t.Fatalf("Acquire error = %v, want refreshed-lock error", err)
		}
	})

	t.Run("finite timeout", func(t *testing.T) {
		runner := &memorySyncRunner{}
		runner.setData(activeForeignState())
		state, _ := newMemoryState(t, runner, time.Hour, lockWait{duration: 50 * time.Millisecond}, false, false)
		err := state.Acquire(make(chan os.Signal))
		if err == nil || !strings.Contains(err.Error(), "timed out") {
			t.Fatalf("Acquire error = %v, want timeout error", err)
		}
	})

	t.Run("finite follows refresh", func(t *testing.T) {
		runner := &memorySyncRunner{}
		runner.setData(activeForeignState())
		state, _ := newMemoryState(t, runner, 100*time.Millisecond, lockWait{duration: 500 * time.Millisecond}, false, false)
		state.pollInterval = 10 * time.Millisecond
		go func() {
			time.Sleep(50 * time.Millisecond)
			runner.setData(activeForeignState())
		}()
		if err := state.Acquire(make(chan os.Signal)); err != nil {
			t.Fatal(err)
		}
		state.Start()
		if err := state.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("early release", func(t *testing.T) {
		runner := &memorySyncRunner{}
		runner.setData(activeForeignState())
		state, _ := newMemoryState(t, runner, time.Hour, lockWait{duration: time.Second}, false, false)
		state.pollInterval = 10 * time.Millisecond
		go func() {
			time.Sleep(30 * time.Millisecond)
			runner.setData(syncFileData{Generation: 1})
		}()
		if err := state.Acquire(make(chan os.Signal)); err != nil {
			t.Fatal(err)
		}
		state.Start()
		if err := state.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestPersistentLock(t *testing.T) {
	t.Run("refreshes continued legacy lock", func(t *testing.T) {
		runner := &memorySyncRunner{}
		timestamp := time.Now().UTC()
		runner.setData(syncFileData{Generation: 3, Lock: &syncFileLock{Owner: "sequence", Timestamp: timestamp}})
		state, _ := newMemoryState(t, runner, time.Hour, lockWait{explicit: true}, false, false, "sequence")
		if err := state.Acquire(make(chan os.Signal)); err != nil {
			t.Fatal(err)
		}
		if got := runner.writes(); got != 1 {
			t.Fatalf("remote writes during continuation = %d, want 1", got)
		}
		state.Start()
		if err := state.Close(); err != nil {
			t.Fatal(err)
		}
		remote := runner.data(t)
		if remote.Lock == nil || remote.Lock.Owner != "sequence" || !remote.Lock.Timestamp.After(timestamp) || remote.Lock.TTL != time.Hour {
			t.Fatalf("refreshed retained lock = %#v", remote.Lock)
		}
	})

	t.Run("updates stored TTL", func(t *testing.T) {
		runner := &memorySyncRunner{}
		timestamp := time.Now().UTC()
		runner.setData(syncFileData{Generation: 2, Lock: &syncFileLock{Owner: "sequence", Timestamp: timestamp, TTL: 2 * time.Hour}})
		state, _ := newMemoryState(t, runner, time.Hour, lockWait{explicit: true}, false, false, "sequence")
		if err := state.Acquire(make(chan os.Signal)); err != nil {
			t.Fatal(err)
		}
		if got := runner.writes(); got != 1 {
			t.Fatalf("remote writes during refresh = %d, want 1", got)
		}
		remote := runner.data(t)
		if remote.Lock == nil || remote.Lock.Owner != "sequence" || !remote.Lock.Timestamp.After(timestamp) || remote.Lock.TTL != time.Hour {
			t.Fatalf("refreshed lock = %#v", remote.Lock)
		}
		state.Start()
		if err := state.Close(); err != nil {
			t.Fatal(err)
		}
		if runner.data(t).Lock == nil {
			t.Fatal("persistent lock was removed on close")
		}
	})

	t.Run("writes ID into new lock", func(t *testing.T) {
		runner := &memorySyncRunner{}
		state, _ := newMemoryState(t, runner, time.Hour, lockWait{}, false, false, "sequence")
		if err := state.Acquire(make(chan os.Signal)); err != nil {
			t.Fatal(err)
		}
		if lock := runner.data(t).Lock; lock == nil || lock.Owner != "sequence" {
			t.Fatalf("new lock = %#v, want owner sequence", lock)
		}
		state.Start()
		if err := state.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("detects another continuation", func(t *testing.T) {
		runner := &memorySyncRunner{}
		runner.setData(syncFileData{Generation: 3})
		first, _ := newMemoryState(t, runner, time.Hour, lockWait{}, false, false, "sequence")
		if err := first.Acquire(make(chan os.Signal)); err != nil {
			t.Fatal(err)
		}
		second, _ := newMemoryState(t, runner, time.Hour, lockWait{}, false, false, "sequence")
		if err := second.Acquire(make(chan os.Signal)); err != nil {
			t.Fatal(err)
		}
		if err := first.BeforeRemoteChange(); !errors.Is(err, errLockOwnershipLost) {
			t.Fatalf("state update error = %v, want ownership loss", err)
		}
		first.Start()
		second.Start()
		if err := first.Close(); err != nil {
			t.Fatal(err)
		}
		if err := second.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestSyncStateConditionalWrites(t *testing.T) {
	t.Run("replace", func(t *testing.T) {
		runner := &memorySyncRunner{}
		runner.setData(syncFileData{Generation: 1})
		state, _ := newMemoryState(t, runner, time.Hour, lockWait{}, true, false)
		if err := state.Acquire(make(chan os.Signal)); err != nil {
			t.Fatal(err)
		}
		args := runner.copyArgs()
		if len(args) < 2 || args[len(args)-2] != "--header-upload" || !strings.HasPrefix(args[len(args)-1], "If-Match:") {
			t.Fatalf("conditional copy args = %#v, want If-Match upload header", args)
		}
		state.Start()
		if err := state.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("create", func(t *testing.T) {
		runner := &memorySyncRunner{}
		state, _ := newMemoryState(t, runner, time.Hour, lockWait{}, true, false)
		if err := state.Acquire(make(chan os.Signal)); err != nil {
			t.Fatal(err)
		}
		args := runner.copyArgs()
		if len(args) < 2 || args[len(args)-2] != "--header-upload" || args[len(args)-1] != "If-None-Match: *" {
			t.Fatalf("conditional copy args = %#v, want If-None-Match upload header", args)
		}
		state.Start()
		if err := state.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("acquisition race", func(t *testing.T) {
		runner := &memorySyncRunner{conditionalFailures: 1}
		state, _ := newMemoryState(t, runner, 50*time.Millisecond, lockWait{duration: 500 * time.Millisecond}, true, false)
		state.pollInterval = 10 * time.Millisecond
		if err := state.Acquire(make(chan os.Signal)); err != nil {
			t.Fatal(err)
		}
		state.Start()
		if err := state.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestFailOnIncompleteBeforeRemoteWrite(t *testing.T) {
	for _, test := range []struct {
		name        string
		local       *syncFileData
		remote      syncFileData
		wantFailure bool
	}{
		{
			name:        "remote generation ahead",
			local:       &syncFileData{Generation: 1, SyncID: "old"},
			remote:      syncFileData{Generation: 2, SyncID: "new", Syncing: true},
			wantFailure: true,
		},
		{
			name:        "local state absent",
			remote:      syncFileData{Generation: 2, SyncID: "new", Syncing: true},
			wantFailure: true,
		},
		{
			name:        "conflicting sync IDs",
			local:       &syncFileData{Generation: 2, SyncID: "local"},
			remote:      syncFileData{Generation: 2, SyncID: "remote", Syncing: true},
			wantFailure: true,
		},
		{
			name:   "matching remote incomplete",
			local:  &syncFileData{Generation: 2, SyncID: "same"},
			remote: syncFileData{Generation: 2, SyncID: "same", Syncing: true},
		},
		{
			name:   "matching local incomplete",
			local:  &syncFileData{Generation: 2, SyncID: "same", Syncing: true},
			remote: syncFileData{Generation: 2, SyncID: "same"},
		},
		{
			name:   "interrupted local session",
			local:  &syncFileData{Generation: 2, SyncID: "same", Active: true},
			remote: syncFileData{Generation: 2, SyncID: "same"},
		},
		{
			name:   "completed remote generation ahead",
			local:  &syncFileData{Generation: 1, SyncID: "old"},
			remote: syncFileData{Generation: 2, SyncID: "new"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := &memorySyncRunner{}
			runner.setData(test.remote)
			state, _ := newMemoryState(t, runner, time.Hour, lockWait{}, false, true)
			if test.local != nil {
				writeSyncState(t, state.paths.local, *test.local)
			}
			err := state.Acquire(make(chan os.Signal))
			if test.wantFailure {
				if err == nil || !strings.Contains(err.Error(), "incomplete sync requiring remote-to-local") {
					t.Fatalf("Acquire error = %v, want incomplete remote-sync error", err)
				}
				if got := runner.writes(); got != 0 {
					t.Fatalf("remote writes = %d, want 0", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("Acquire error = %v, want success", err)
			}
			state.Start()
			if err := state.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestUntrackedRemoteRequiresForce(t *testing.T) {
	runner := &memorySyncRunner{remoteFiles: true}
	state, _ := newMemoryState(t, runner, time.Hour, lockWait{}, false, false)
	err := state.Acquire(make(chan os.Signal))
	if err == nil || !strings.Contains(err.Error(), "--force-delete-untracked-remote") {
		t.Fatalf("Acquire error = %v, want force-delete error", err)
	}
	if got := runner.writes(); got != 0 {
		t.Fatalf("remote state writes = %d, want 0", got)
	}
}

func TestEmptyRemoteAcquiresInitializationLock(t *testing.T) {
	runner := &memorySyncRunner{}
	state, _ := newMemoryState(t, runner, time.Hour, lockWait{}, false, false)
	if err := state.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	remote := runner.data(t)
	if remote.RepositoryID == "" || remote.Generation != 0 || remote.Lock == nil || remote.Lock.Owner != state.owner || remote.Lock.TTL != time.Hour {
		t.Fatalf("initial remote state = %#v, want owned generation 0", remote)
	}
	state.Start()
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestInitializedRepositoryHasUUID(t *testing.T) {
	runner := &memorySyncRunner{}
	state, _ := newMemoryState(t, runner, time.Hour, lockWait{}, false, false)
	if err := state.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	if err := state.InitializeGeneration(); err != nil {
		t.Fatal(err)
	}
	local, exists, err := readLocalSyncFile(state.paths.local)
	if err != nil || !exists {
		t.Fatalf("read local state: exists=%v, error=%v", exists, err)
	}
	remote := runner.data(t)
	if local.RepositoryID != remote.RepositoryID {
		t.Fatalf("repository IDs differ: local=%q remote=%q", local.RepositoryID, remote.RepositoryID)
	}
	if matched := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(local.RepositoryID); !matched {
		t.Fatalf("repository ID = %q, want UUID v4", local.RepositoryID)
	}
}

func TestSyncStateRejectsRepositoryIDMismatch(t *testing.T) {
	runner := &memorySyncRunner{}
	runner.setData(syncFileData{RepositoryID: "22222222-2222-4222-8222-222222222222", Generation: 1})
	state, _ := newMemoryState(t, runner, time.Hour, lockWait{}, false, false)
	writeSyncState(t, state.paths.local, syncFileData{RepositoryID: testRepositoryID, Generation: 1})

	err := state.Acquire(make(chan os.Signal))
	if err == nil || !strings.Contains(err.Error(), "does not match remote repository ID") {
		t.Fatalf("Acquire error = %v, want repository ID mismatch", err)
	}
	if got := runner.writes(); got != 0 {
		t.Fatalf("remote state writes = %d, want 0", got)
	}
}

func TestStateFileRequiresRepositoryID(t *testing.T) {
	if _, err := decodeSyncFile([]byte(`{"generation":1,"syncing":false}`)); err == nil || !strings.Contains(err.Error(), "repository ID must be set") {
		t.Fatalf("decode error = %v, want required repository ID", err)
	}
}

func TestGenerationZeroRetriesInitialization(t *testing.T) {
	runner := &memorySyncRunner{}
	runner.setData(syncFileData{Generation: 0})
	state, _ := newMemoryState(t, runner, time.Hour, lockWait{}, false, false)
	if err := state.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	state.Start()
	runner.payloadErr = errors.New("initial sync failed")
	if err := state.InitializeRemote(); err == nil {
		t.Fatal("initialization failure was ignored")
	}
	runner.payloadErr = nil
	if err := state.InitializeRemote(); err != nil {
		t.Fatal(err)
	}
	if remote := runner.data(t); remote.Generation != 1 || remote.Lock == nil {
		t.Fatalf("remote state after retry = %#v, want locked generation 1", remote)
	}
	assertSyncState(t, state.paths.local, 1, false, false)
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSyncStateRecoversUnpublishedLocalGeneration(t *testing.T) {
	runner := &memorySyncRunner{}
	runner.setData(syncFileData{Generation: 4})
	state, _ := newMemoryState(t, runner, time.Hour, lockWait{}, false, false)
	if err := writeLocalSyncFile(state.paths.local, syncFileData{RepositoryID: testRepositoryID, Generation: 5, Syncing: true}); err != nil {
		t.Fatal(err)
	}
	if err := state.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	if err := state.InitializeGeneration(); err != nil {
		t.Fatal(err)
	}
	assertSyncState(t, state.paths.local, 5, false, false)
	if !runner.payloadRan {
		t.Fatal("unpublished generation did not trigger recovery sync")
	}
	if remote := runner.data(t); remote.Generation != 5 || remote.Syncing {
		t.Fatalf("remote state after recovery = %#v", remote)
	}
	state.Start()
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSyncStateMarksPayloadTransition(t *testing.T) {
	runner := &memorySyncRunner{}
	state, source := newMemoryState(t, runner, time.Hour, lockWait{}, false, false)
	if err := state.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	if err := state.InitializeGeneration(); err != nil {
		t.Fatal(err)
	}
	initialID := runner.data(t).SyncID
	if initialID == "" {
		t.Fatal("initialization did not create a sync ID")
	}
	assertSyncID(t, state.paths.local, initialID)
	runner.requireGeneration = 2
	runner.requireSyncing = true
	runner.requireLocalState = state.paths.local
	runner.payloadRan = false
	state.Start()
	if err := os.WriteFile(filepath.Join(source, "payload.txt"), []byte("payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := syncer{
		source:     source,
		dest:       state.dest,
		state:      state,
		trackState: true,
		logger:     log.New(io.Discard, "", 0),
		runner:     runner,
	}
	if err := s.Sync(map[string]change{"payload.txt": {path: "payload.txt"}}); err != nil {
		t.Fatal(err)
	}
	if !runner.payloadRan {
		t.Fatal("payload command was not run")
	}
	assertSyncState(t, state.paths.local, 2, false, false)
	remote := runner.data(t)
	if remote.Generation != 2 || remote.Syncing || remote.Lock == nil {
		t.Fatalf("remote state after payload = %#v", remote)
	}
	if remote.SyncID == "" || remote.SyncID == initialID || remote.SyncID != runner.payloadSyncID {
		t.Fatalf("sync ID after payload = %q, initial = %q, during payload = %q", remote.SyncID, initialID, runner.payloadSyncID)
	}
	assertSyncID(t, state.paths.local, remote.SyncID)
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	if runner.data(t).SyncID != remote.SyncID {
		t.Fatal("lock release changed the sync ID")
	}
}

func TestSyncStateRetainsIncompleteFlagAfterPayloadFailure(t *testing.T) {
	runner := &memorySyncRunner{}
	state, source := newMemoryState(t, runner, time.Hour, lockWait{}, false, false)
	if err := state.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	if err := state.InitializeGeneration(); err != nil {
		t.Fatal(err)
	}
	runner.payloadErr = errors.New("payload failed")
	state.Start()
	if err := os.WriteFile(filepath.Join(source, "payload.txt"), []byte("payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := syncer{source: source, dest: state.dest, state: state, trackState: true, logger: log.New(io.Discard, "", 0), runner: runner}
	if err := s.Sync(map[string]change{"payload.txt": {path: "payload.txt"}}); err == nil {
		t.Fatal("payload failure was ignored")
	}
	assertSyncState(t, state.paths.local, 2, true, false)
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	remote := runner.data(t)
	if remote.Generation != 2 || !remote.Syncing || remote.Lock != nil {
		t.Fatalf("remote state after failed payload and release = %#v", remote)
	}
}

func TestSyncStateRetriesFailedRecovery(t *testing.T) {
	runner := &memorySyncRunner{requireGeneration: 5, requireSyncing: true}
	runner.setData(syncFileData{Generation: 4, SyncID: "interrupted", Syncing: true})
	state, _ := newMemoryState(t, runner, time.Hour, lockWait{}, false, false)
	writeSyncState(t, state.paths.local, syncFileData{Generation: 4, SyncID: "interrupted", Syncing: true})
	runner.requireLocalState = state.paths.local
	if err := state.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	state.Start()
	t.Cleanup(func() {
		if err := state.Close(); err != nil {
			t.Error(err)
		}
	})
	failure := errors.New("recovery payload failed")
	runner.payloadErr = failure
	if err := state.InitializeGeneration(); !errors.Is(err, failure) {
		t.Fatalf("recovery error = %v, want %v", err, failure)
	}
	assertSyncState(t, state.paths.local, 5, true, false)
	if remote := runner.data(t); remote.Generation != 5 || !remote.Syncing || remote.Lock == nil {
		t.Fatalf("remote state after failed recovery = %#v", remote)
	}
	failedID := runner.data(t).SyncID
	if failedID == "" || failedID == "interrupted" {
		t.Fatalf("recovery did not create a fresh sync ID: %q", failedID)
	}
	assertSyncID(t, state.paths.local, failedID)
	runner.payloadErr = nil
	runner.requireGeneration = 6
	if err := state.InitializeGeneration(); err != nil {
		t.Fatal(err)
	}
	assertSyncState(t, state.paths.local, 6, false, false)
	if remote := runner.data(t); remote.Generation != 6 || remote.Syncing || remote.Lock == nil {
		t.Fatalf("remote state after recovery retry = %#v", remote)
	}
	if remote := runner.data(t); remote.SyncID == failedID || remote.SyncID != runner.payloadSyncID {
		t.Fatalf("retry did not retain a fresh sync ID: %#v", remote)
	}
	assertSyncID(t, state.paths.local, runner.data(t).SyncID)
}

func TestSyncStateRecoversIncompleteSyncWithLocalRclone(t *testing.T) {
	requireRclone(t)
	for _, test := range []struct {
		name        string
		local       syncFileData
		remote      syncFileData
		localAbsent bool
		fromRemote  bool
	}{
		{name: "both incomplete", local: syncFileData{Generation: 4, SyncID: "same", Syncing: true}, remote: syncFileData{Generation: 4, SyncID: "same", Syncing: true}},
		{name: "local incomplete", local: syncFileData{Generation: 4, SyncID: "same", Syncing: true}, remote: syncFileData{Generation: 4, SyncID: "same"}},
		{name: "remote incomplete", local: syncFileData{Generation: 4, SyncID: "same"}, remote: syncFileData{Generation: 4, SyncID: "same", Syncing: true}},
		{name: "unpublished generation", local: syncFileData{Generation: 5, SyncID: "unpublished", Syncing: true}, remote: syncFileData{Generation: 4, SyncID: "previous"}},
		{name: "remote ahead local incomplete", local: syncFileData{Generation: 3, SyncID: "old", Syncing: true}, remote: syncFileData{Generation: 4, SyncID: "new"}, fromRemote: true},
		{name: "remote ahead remote incomplete", local: syncFileData{Generation: 3, SyncID: "old"}, remote: syncFileData{Generation: 4, SyncID: "new", Syncing: true}, fromRemote: true},
		{name: "remote ahead both incomplete", local: syncFileData{Generation: 3, SyncID: "old", Syncing: true}, remote: syncFileData{Generation: 4, SyncID: "new", Syncing: true}, fromRemote: true},
		{name: "local absent remote incomplete", localAbsent: true, remote: syncFileData{Generation: 4, SyncID: "new", Syncing: true}, fromRemote: true},
		{name: "equal generation different IDs", local: syncFileData{Generation: 4, SyncID: "unpublished", Syncing: true}, remote: syncFileData{Generation: 4, SyncID: "other-writer"}, fromRemote: true},
		{name: "equal completed generation different IDs", local: syncFileData{Generation: 4, SyncID: "different"}, remote: syncFileData{Generation: 4, SyncID: "other-writer"}, fromRemote: true},
		{name: "legacy local incomplete", local: syncFileData{Generation: 4, Syncing: true}, remote: syncFileData{Generation: 4, SyncID: "other-writer"}, fromRemote: true},
		{name: "legacy remote", local: syncFileData{Generation: 4, SyncID: "unpublished", Syncing: true}, remote: syncFileData{Generation: 4}, fromRemote: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := t.TempDir()
			destination := t.TempDir()
			if !test.localAbsent {
				writeSyncState(t, filepath.Join(source, defaultStateFile), test.local)
			}
			writeSyncState(t, filepath.Join(destination, defaultStateFile), test.remote)
			writeTestFile(t, filepath.Join(source, "payload.txt"), "local payload")
			writeTestFile(t, filepath.Join(destination, "payload.txt"), "remote payload")
			writeTestFile(t, filepath.Join(source, "local-only.txt"), "local")
			writeTestFile(t, filepath.Join(destination, "remote-only.txt"), "remote")
			state := acquireLocalState(t, source, destination)
			state.Start()
			t.Cleanup(func() {
				if err := state.Close(); err != nil {
					t.Error(err)
				}
			})
			if err := state.InitializeGeneration(); err != nil {
				t.Fatal(err)
			}
			wantPayload, retained, removed := "local payload", "local-only.txt", "remote-only.txt"
			generation, syncing := test.remote.Generation+1, false
			if test.fromRemote {
				wantPayload, retained, removed = "remote payload", "remote-only.txt", "local-only.txt"
				generation, syncing = test.remote.Generation, test.remote.Syncing
			}
			for _, root := range []string{source, destination} {
				if got := readTestFile(t, filepath.Join(root, "payload.txt")); got != wantPayload {
					t.Fatalf("payload in %s = %q, want %q", root, got, wantPayload)
				}
				if _, err := os.Stat(filepath.Join(root, retained)); err != nil {
					t.Fatalf("retained file in %s: %v", root, err)
				}
				if _, err := os.Stat(filepath.Join(root, removed)); !os.IsNotExist(err) {
					t.Fatalf("removed file in %s still exists: %v", root, err)
				}
			}
			assertSyncState(t, state.paths.local, generation, syncing, false)
			assertSyncState(t, state.paths.remote, generation, syncing, true)
			remote, _, err := readLocalSyncFile(state.paths.remote)
			if err != nil {
				t.Fatal(err)
			}
			if test.fromRemote {
				if remote.SyncID != test.remote.SyncID {
					t.Fatalf("download changed remote sync ID: %q", remote.SyncID)
				}
			} else if remote.SyncID == "" || remote.SyncID == test.remote.SyncID || remote.SyncID == test.local.SyncID {
				t.Fatalf("recovery did not publish a fresh sync ID: %q", remote.SyncID)
			}
			assertSyncID(t, state.paths.local, remote.SyncID)
		})
	}
}

func TestSyncFromRemoteAppliesExcludes(t *testing.T) {
	runner := &recordingRunner{}
	state := &syncState{
		paths: syncFilePaths{
			localFilter:  ".local-state",
			remoteFilter: ".remote-state",
		},
		source:   "/source",
		dest:     "remote:destination",
		excludes: []string{"*.tmp", "/cache/**"},
		runner:   runner,
	}
	if err := state.syncFromRemote(); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"sync", "remote:destination", "/source", "--create-empty-src-dirs", "--delete-before", "--ignore-times",
		"--exclude", "/.local-state.rcw-tmp-*",
		"--exclude", "/.local-state", "--exclude", "/.remote-state",
		"--exclude", "*.tmp", "--exclude", "/cache/**",
	}
	if !reflect.DeepEqual(runner.commands[0].args, want) {
		t.Fatalf("remote sync args = %#v, want %#v", runner.commands[0].args, want)
	}
}

func TestSyncStateInitializeGeneration(t *testing.T) {
	requireRclone(t)
	t.Run("both absent initializes from local", func(t *testing.T) {
		source := t.TempDir()
		destination := t.TempDir()
		writeTestFile(t, filepath.Join(source, "local-only.txt"), "local")
		state := acquireLocalState(t, source, destination)
		if err := state.InitializeGeneration(); err != nil {
			t.Fatal(err)
		}
		if got := readTestFile(t, filepath.Join(destination, "local-only.txt")); got != "local" {
			t.Fatalf("local content synced remotely = %q, want local", got)
		}
		if got := readTestFile(t, filepath.Join(source, "local-only.txt")); got != "local" {
			t.Fatalf("local content = %q, want local", got)
		}
		assertSyncState(t, state.paths.local, 1, false, false)
		state.Start()
		if err := state.Close(); err != nil {
			t.Fatal(err)
		}
		assertSyncState(t, state.paths.remote, 1, false, false)
	})

	t.Run("forced non-empty initialization", func(t *testing.T) {
		source := t.TempDir()
		destination := t.TempDir()
		writeTestFile(t, filepath.Join(source, "local.txt"), "local")
		writeTestFile(t, filepath.Join(source, "excluded.tmp"), "excluded")
		writeTestFile(t, filepath.Join(destination, "remote-only.tmp"), "remote")
		state := newLocalState(t, source, destination)
		state.forceDeleteUntrackedRemote = true
		state.excludes = []string{"*.tmp"}
		if err := state.Acquire(make(chan os.Signal)); err != nil {
			t.Fatal(err)
		}
		state.Start()
		if err := state.InitializeRemote(); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(destination, "remote-only.tmp")); !os.IsNotExist(err) {
			t.Fatalf("untracked remote file remains: %v", err)
		}
		if _, err := os.Stat(filepath.Join(destination, "excluded.tmp")); !os.IsNotExist(err) {
			t.Fatalf("excluded local file was synced: %v", err)
		}
		if got := readTestFile(t, filepath.Join(destination, "local.txt")); got != "local" {
			t.Fatalf("initialized remote content = %q, want local", got)
		}
		assertSyncState(t, state.paths.local, 1, false, false)
		if err := state.Close(); err != nil {
			t.Fatal(err)
		}
		assertSyncState(t, state.paths.remote, 1, false, false)
	})

	t.Run("remote ahead", func(t *testing.T) {
		source := t.TempDir()
		destination := t.TempDir()
		writeSyncState(t, filepath.Join(source, defaultStateFile), syncFileData{Generation: 1})
		writeSyncState(t, filepath.Join(destination, defaultStateFile), syncFileData{Generation: 2})
		writeTestFile(t, filepath.Join(source, "local-only.txt"), "local")
		writeTestFile(t, filepath.Join(destination, "remote.txt"), "remote")
		state := acquireLocalState(t, source, destination)
		if err := state.InitializeGeneration(); err != nil {
			t.Fatal(err)
		}
		assertSyncState(t, state.paths.local, 2, false, false)
		if got := readTestFile(t, filepath.Join(source, "remote.txt")); got != "remote" {
			t.Fatalf("remote content = %q, want remote", got)
		}
		state.Start()
		if err := state.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("equal", func(t *testing.T) {
		source := t.TempDir()
		destination := t.TempDir()
		writeSyncState(t, filepath.Join(source, defaultStateFile), syncFileData{Generation: 3})
		writeSyncState(t, filepath.Join(destination, defaultStateFile), syncFileData{Generation: 3})
		writeTestFile(t, filepath.Join(source, "local-only.txt"), "local")
		writeTestFile(t, filepath.Join(destination, "remote-only.txt"), "remote")
		state := acquireLocalState(t, source, destination)
		if err := state.InitializeGeneration(); err != nil {
			t.Fatal(err)
		}
		if got := readTestFile(t, filepath.Join(source, "local-only.txt")); got != "local" {
			t.Fatalf("local content = %q, want local", got)
		}
		if _, err := os.Stat(filepath.Join(source, "remote-only.txt")); !os.IsNotExist(err) {
			t.Fatalf("equal generations unexpectedly triggered a sync: %v", err)
		}
		state.Start()
		if err := state.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestSyncStateRejectsInvalidGenerationOrdering(t *testing.T) {
	requireRclone(t)
	for _, test := range []struct {
		name       string
		local      uint64
		remote     uint64
		wantError  string
		makeRemote bool
	}{
		{name: "local ahead", local: 2, remote: 1, makeRemote: true, wantError: "ahead of remote"},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := t.TempDir()
			destination := t.TempDir()
			writeSyncState(t, filepath.Join(source, defaultStateFile), syncFileData{Generation: test.local})
			if test.makeRemote {
				writeSyncState(t, filepath.Join(destination, defaultStateFile), syncFileData{Generation: test.remote})
			}
			paths, err := resolveSyncFilePaths(source, destination, defaultStateFile, defaultStateFile)
			if err != nil {
				t.Fatal(err)
			}
			state, err := newSyncState(paths, source, destination, time.Hour, lockWait{}, false, false, false, log.New(io.Discard, "", 0), &rcloneCommand{}, "")
			if err != nil {
				t.Fatal(err)
			}
			err = state.Acquire(make(chan os.Signal))
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("Acquire error = %v, want %q", err, test.wantError)
			}
		})
	}
}

func TestSyncStateWaitCanBeInterrupted(t *testing.T) {
	runner := &memorySyncRunner{}
	runner.setData(activeForeignState())
	interrupt := make(chan os.Signal, 1)
	interrupt <- os.Interrupt
	state, _ := newMemoryState(t, runner, time.Hour, lockWait{infinite: true}, false, false)
	if err := state.Acquire(interrupt); !errors.Is(err, errLockInterrupted) {
		t.Fatalf("Acquire error = %v, want errLockInterrupted", err)
	}
}

func TestResponseETag(t *testing.T) {
	output := "2026/09/10 DEBUG : HTTP RESPONSE\nHTTP/1.1 200 OK\nEtag: \"opaque-etag\"\n"
	if got := responseETag(output); got != "opaque-etag" {
		t.Fatalf("responseETag = %q, want opaque-etag", got)
	}
}

func activeForeignState() syncFileData {
	return syncFileData{
		Generation: 1,
		Lock: &syncFileLock{
			Owner:     "foreign-" + time.Now().UTC().Format(time.RFC3339Nano),
			Timestamp: time.Now().UTC(),
			TTL:       100 * time.Millisecond,
		},
	}
}

func acquireLocalState(t *testing.T, source, destination string) *syncState {
	t.Helper()
	state := newLocalState(t, source, destination)
	if err := state.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	state.Start()
	return state
}

func newLocalState(t *testing.T, source, destination string) *syncState {
	t.Helper()
	paths, err := resolveSyncFilePaths(source, destination, defaultStateFile, defaultStateFile)
	if err != nil {
		t.Fatal(err)
	}
	state, err := newSyncState(paths, source, destination, time.Hour, lockWait{}, false, false, false, log.New(io.Discard, "", 0), &rcloneCommand{}, "")
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func requireRclone(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("rclone"); err != nil {
		t.Skip("rclone is not installed")
	}
}

func writeTestFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}

func readTestFile(t *testing.T, path string) string {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(contents)
}

func writeSyncState(t *testing.T, path string, data syncFileData) {
	t.Helper()
	if data.RepositoryID == "" {
		data.RepositoryID = testRepositoryID
	}
	if err := writeLocalSyncFile(path, data); err != nil {
		t.Fatal(err)
	}
}

func assertSyncState(t *testing.T, path string, generation uint64, syncing, locked bool) {
	t.Helper()
	data, exists, err := readLocalSyncFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !exists || data.Generation != generation || data.Syncing != syncing || (data.Lock != nil) != locked {
		t.Fatalf("sync state at %q = %#v (exists %v), want generation=%d syncing=%v locked=%v", path, data, exists, generation, syncing, locked)
	}
}

func assertSyncID(t *testing.T, path, want string) {
	t.Helper()
	data, exists, err := readLocalSyncFile(path)
	if err != nil || !exists || data.SyncID != want {
		t.Fatalf("sync ID at %q = %q (exists %v, error %v), want %q", path, data.SyncID, exists, err, want)
	}
}

func encodeTestSyncFile(data syncFileData) string {
	if data.RepositoryID == "" {
		data.RepositoryID = testRepositoryID
	}
	contents, err := encodeSyncFile(data)
	if err != nil {
		panic(err)
	}
	return strings.TrimSpace(string(contents))
}
