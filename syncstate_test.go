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
	"strings"
	"sync"
	"testing"
	"time"
)

type memorySyncRunner struct {
	mu                  sync.Mutex
	value               string
	lastCopyArgs        []string
	copyCount           int
	conditionalFailures int
	payloadErr          error
	payloadRan          bool
	requireGeneration   uint64
	requireSyncing      bool
}

func (r *memorySyncRunner) Run(args []string, stdout, stderr io.Writer) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch args[0] {
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
		if r.conditionalFailures > 0 {
			r.conditionalFailures--
			r.value = encodeTestSyncFile(syncFileData{
				Generation: 1,
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
		r.payloadRan = true
		return r.payloadErr
	}
}

func (r *memorySyncRunner) setData(data syncFileData) {
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

func newMemoryState(t *testing.T, runner *memorySyncRunner, timeout time.Duration, wait lockWait, consistent, failOnIncomplete bool) (*syncState, string) {
	t.Helper()
	source := t.TempDir()
	paths := syncFilePaths{
		local:        filepath.Join(source, defaultSyncFile),
		remote:       "remote:destination/" + defaultSyncFile,
		localFilter:  defaultSyncFile,
		remoteFilter: defaultSyncFile,
	}
	state, err := newSyncState(paths, source, "remote:destination", timeout, wait, consistent, failOnIncomplete, false, log.New(io.Discard, "", 0), runner)
	if err != nil {
		t.Fatal(err)
	}
	return state, source
}

func TestSyncStateLifecycleWithLocalRclone(t *testing.T) {
	requireRclone(t)
	source := t.TempDir()
	destination := t.TempDir()
	paths, err := resolveSyncFilePaths(source, destination, defaultSyncFile, defaultSyncFile)
	if err != nil {
		t.Fatal(err)
	}
	state, err := newSyncState(paths, source, destination, 500*time.Millisecond, lockWait{}, false, false, false, log.New(io.Discard, "", 0), rcloneCommand{})
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	initial, _, err := readLocalSyncFile(paths.remote)
	if err != nil || initial.Lock == nil {
		t.Fatalf("initial remote state = %#v, err = %v", initial, err)
	}
	errors := state.Start()

	deadline := time.Now().Add(3 * time.Second)
	for {
		current, _, readErr := readLocalSyncFile(paths.remote)
		if readErr == nil && current.Lock != nil && current.Lock.Timestamp.After(initial.Lock.Timestamp) {
			break
		}
		select {
		case refreshErr := <-errors:
			t.Fatalf("refresh failed: %v", refreshErr)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("lock timestamp was not refreshed")
		}
		time.Sleep(20 * time.Millisecond)
	}

	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	assertSyncState(t, paths.remote, 1, false, false)
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
		runner.setData(activeForeignState())
		state, _ := newMemoryState(t, runner, 60*time.Millisecond, lockWait{}, false, false)
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
	for _, location := range []string{"local", "remote"} {
		t.Run(location, func(t *testing.T) {
			runner := &memorySyncRunner{}
			runner.setData(syncFileData{Generation: 1, Syncing: location == "remote"})
			state, _ := newMemoryState(t, runner, time.Hour, lockWait{}, false, true)
			if location == "local" {
				if err := writeLocalSyncFile(state.paths.local, syncFileData{Generation: 1, Syncing: true}); err != nil {
					t.Fatal(err)
				}
			}
			err := state.Acquire(make(chan os.Signal))
			if err == nil || !strings.Contains(err.Error(), "incomplete previous sync") {
				t.Fatalf("Acquire error = %v, want incomplete-sync error", err)
			}
			if got := runner.writes(); got != 0 {
				t.Fatalf("remote writes = %d, want 0", got)
			}
		})
	}
}

func TestSyncStateRecoversUnpublishedLocalGeneration(t *testing.T) {
	runner := &memorySyncRunner{}
	runner.setData(syncFileData{Generation: 4})
	state, _ := newMemoryState(t, runner, time.Hour, lockWait{}, false, false)
	if err := writeLocalSyncFile(state.paths.local, syncFileData{Generation: 5, Syncing: true}); err != nil {
		t.Fatal(err)
	}
	if err := state.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	if err := state.InitializeGeneration(); err != nil {
		t.Fatal(err)
	}
	assertSyncState(t, state.paths.local, 4, false, false)
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
	runner.requireGeneration = 2
	runner.requireSyncing = true
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
	if err := state.Close(); err != nil {
		t.Fatal(err)
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

func TestSyncStateInitializeGeneration(t *testing.T) {
	requireRclone(t)
	t.Run("absent", func(t *testing.T) {
		source := t.TempDir()
		destination := t.TempDir()
		writeTestFile(t, filepath.Join(source, "local-only.txt"), "local")
		writeTestFile(t, filepath.Join(destination, "remote.txt"), "remote")
		state := acquireLocalState(t, source, destination)
		if err := state.InitializeGeneration(); err != nil {
			t.Fatal(err)
		}
		if got := readTestFile(t, filepath.Join(source, "remote.txt")); got != "remote" {
			t.Fatalf("remote content = %q, want remote", got)
		}
		if _, err := os.Stat(filepath.Join(source, "local-only.txt")); !os.IsNotExist(err) {
			t.Fatalf("local-only file remains after remote sync: %v", err)
		}
		assertSyncState(t, state.paths.local, 1, false, false)
		state.Start()
		if err := state.Close(); err != nil {
			t.Fatal(err)
		}
		assertSyncState(t, state.paths.remote, 1, false, false)
	})

	t.Run("remote ahead", func(t *testing.T) {
		source := t.TempDir()
		destination := t.TempDir()
		writeSyncState(t, filepath.Join(source, defaultSyncFile), syncFileData{Generation: 1})
		writeSyncState(t, filepath.Join(destination, defaultSyncFile), syncFileData{Generation: 2})
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
		writeSyncState(t, filepath.Join(source, defaultSyncFile), syncFileData{Generation: 3})
		writeSyncState(t, filepath.Join(destination, defaultSyncFile), syncFileData{Generation: 3})
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
		{name: "local only", local: 1, wantError: "remote sync file is missing"},
		{name: "local ahead", local: 2, remote: 1, makeRemote: true, wantError: "ahead of remote"},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := t.TempDir()
			destination := t.TempDir()
			writeSyncState(t, filepath.Join(source, defaultSyncFile), syncFileData{Generation: test.local})
			if test.makeRemote {
				writeSyncState(t, filepath.Join(destination, defaultSyncFile), syncFileData{Generation: test.remote})
			}
			paths, err := resolveSyncFilePaths(source, destination, defaultSyncFile, defaultSyncFile)
			if err != nil {
				t.Fatal(err)
			}
			state, err := newSyncState(paths, source, destination, time.Hour, lockWait{}, false, false, false, log.New(io.Discard, "", 0), rcloneCommand{})
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
		},
	}
}

func acquireLocalState(t *testing.T, source, destination string) *syncState {
	t.Helper()
	paths, err := resolveSyncFilePaths(source, destination, defaultSyncFile, defaultSyncFile)
	if err != nil {
		t.Fatal(err)
	}
	state, err := newSyncState(paths, source, destination, time.Hour, lockWait{}, false, false, false, log.New(io.Discard, "", 0), rcloneCommand{})
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Acquire(make(chan os.Signal)); err != nil {
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

func encodeTestSyncFile(data syncFileData) string {
	contents, err := encodeSyncFile(data)
	if err != nil {
		panic(err)
	}
	return strings.TrimSpace(string(contents))
}
