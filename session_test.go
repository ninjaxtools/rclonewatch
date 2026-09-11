package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func assertLocalActive(t *testing.T, source string, want bool) {
	t.Helper()
	data, exists, err := readLocalSyncFile(filepath.Join(source, defaultStateFile))
	if err != nil || !exists || data.Active != want {
		t.Fatalf("local active = %v (exists %v, error %v), want %v", data.Active, exists, err, want)
	}
}

func TestInterruptedSessionRecoversPendingChanges(t *testing.T) {
	requireRclone(t)
	source, destination := t.TempDir(), t.TempDir()
	writeTestFile(t, filepath.Join(source, "modified.txt"), "before")
	writeTestFile(t, filepath.Join(source, "deleted.txt"), "delete me")
	args := []string{"--logs", "--persistent-lock", "session-test", source, destination}
	process := startTestProcess(t, args...)
	t.Cleanup(func() { _ = process.cmd.Process.Kill() })
	assertLocalActive(t, source, true)
	writeTestFile(t, filepath.Join(source, "modified.txt"), "after the crash")
	writeTestFile(t, filepath.Join(source, "created.txt"), "new")
	if err := os.Remove(filepath.Join(source, "deleted.txt")); err != nil {
		t.Fatal(err)
	}
	if err := process.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = process.waitForExit(t)
	assertLocalActive(t, source, true)
	assertSyncState(t, filepath.Join(source, defaultStateFile), 1, false, false)

	process = startTestProcess(t, args...)
	assertLocalActive(t, source, true)
	if got := readTestFile(t, filepath.Join(destination, "modified.txt")); got != "after the crash" {
		t.Fatalf("recovered modification = %q", got)
	}
	if got := readTestFile(t, filepath.Join(destination, "created.txt")); got != "new" {
		t.Fatalf("recovered creation = %q", got)
	}
	if _, err := os.Stat(filepath.Join(destination, "deleted.txt")); !os.IsNotExist(err) {
		t.Fatalf("deleted file remains remotely: %v", err)
	}
	assertSyncState(t, filepath.Join(source, defaultStateFile), 2, false, false)
	if strings.Contains(readTestFile(t, filepath.Join(destination, defaultStateFile)), `"active"`) {
		t.Fatal("local active marker was published remotely")
	}
	if err := process.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := process.waitForExit(t); err != nil {
		t.Fatal(err)
	}
	assertLocalActive(t, source, false)

	// A clean subsequent session must not upload another recovery generation.
	process = startTestProcess(t, args...)
	assertSyncState(t, filepath.Join(source, defaultStateFile), 2, false, false)
	assertLocalActive(t, source, true)
	if err := process.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := process.waitForExit(t); err != nil {
		t.Fatal(err)
	}
	assertLocalActive(t, source, false)
}

func TestActiveSessionReconciliationPriority(t *testing.T) {
	requireRclone(t)
	for _, test := range []struct {
		name       string
		local      syncFileData
		remote     syncFileData
		fromRemote bool
	}{
		{name: "matching lineage", local: syncFileData{Generation: 4, SyncID: "same", Active: true}, remote: syncFileData{Generation: 4, SyncID: "same"}},
		{name: "completed legacy lineage", local: syncFileData{Generation: 4, Active: true}, remote: syncFileData{Generation: 4}},
		{name: "remote newer", local: syncFileData{Generation: 3, SyncID: "old", Active: true}, remote: syncFileData{Generation: 4, SyncID: "new"}, fromRemote: true},
		{name: "different sync IDs", local: syncFileData{Generation: 4, SyncID: "local", Active: true}, remote: syncFileData{Generation: 4, SyncID: "remote"}, fromRemote: true},
		{name: "remote incomplete and newer", local: syncFileData{Generation: 3, SyncID: "old", Active: true}, remote: syncFileData{Generation: 4, SyncID: "new", Syncing: true}, fromRemote: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			source, destination := t.TempDir(), t.TempDir()
			writeSyncState(t, filepath.Join(source, defaultStateFile), test.local)
			writeSyncState(t, filepath.Join(destination, defaultStateFile), test.remote)
			writeTestFile(t, filepath.Join(source, "payload.txt"), "local payload")
			writeTestFile(t, filepath.Join(destination, "payload.txt"), "remote payload")
			writeTestFile(t, filepath.Join(source, "excluded.tmp"), "local excluded")
			writeTestFile(t, filepath.Join(destination, "excluded.tmp"), "remote excluded")
			state := acquireLocalState(t, source, destination)
			state.excludes = []string{"*.tmp"}
			t.Cleanup(func() {
				if err := state.Close(); err != nil {
					t.Error(err)
				}
			})
			if err := state.InitializeGeneration(); err != nil {
				t.Fatal(err)
			}
			want, generation, syncing := "local payload", uint64(5), false
			if test.fromRemote {
				want, generation, syncing = "remote payload", 4, test.remote.Syncing
			}
			for _, root := range []string{source, destination} {
				if got := readTestFile(t, filepath.Join(root, "payload.txt")); got != want {
					t.Fatalf("payload in %s = %q, want %q", root, got, want)
				}
			}
			assertSyncState(t, state.paths.local, generation, syncing, false)
			assertLocalActive(t, source, true)
			if got := readTestFile(t, filepath.Join(source, "excluded.tmp")); got != "local excluded" {
				t.Fatalf("local exclusion overwritten: %q", got)
			}
			if got := readTestFile(t, filepath.Join(destination, "excluded.tmp")); got != "remote excluded" {
				t.Fatalf("remote exclusion overwritten: %q", got)
			}
			if !test.fromRemote && state.getRemote().data.SyncID == test.remote.SyncID {
				t.Fatal("session recovery did not generate a new sync ID")
			}
		})
	}
}

func TestActiveSessionRecoveryFailureRemainsActive(t *testing.T) {
	runner := &memorySyncRunner{payloadErr: errors.New("recovery failed")}
	runner.setData(syncFileData{Generation: 4, SyncID: "same"})
	state, source := newMemoryState(t, runner, time.Hour, lockWait{}, false, false)
	writeSyncState(t, state.paths.local, syncFileData{Generation: 4, SyncID: "same", Active: true})
	if err := state.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	state.Start()
	if err := state.InitializeGeneration(); err == nil {
		t.Fatal("recovery unexpectedly succeeded")
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	assertLocalActive(t, source, true)
	assertSyncState(t, state.paths.local, 5, true, false)
}

func TestFailOnIncompleteRejectsActiveSession(t *testing.T) {
	runner := &memorySyncRunner{}
	runner.setData(syncFileData{Generation: 4, SyncID: "same"})
	state, source := newMemoryState(t, runner, time.Hour, lockWait{}, false, true)
	writeSyncState(t, state.paths.local, syncFileData{Generation: 4, SyncID: "same", Active: true})
	if err := state.Acquire(make(chan os.Signal)); err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("Acquire error = %v, want incomplete-session rejection", err)
	}
	if got := runner.writes(); got != 0 {
		t.Fatalf("remote writes = %d, want 0", got)
	}
	assertLocalActive(t, source, true)
}

func TestLegacyLocalStateStartsInactive(t *testing.T) {
	runner := &memorySyncRunner{}
	runner.setData(syncFileData{Generation: 4, SyncID: "same"})
	state, source := newMemoryState(t, runner, time.Hour, lockWait{}, false, false)
	writeTestFile(t, state.paths.local, `{"generation":4,"sync_id":"same","syncing":false}`)
	if err := state.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	state.Start()
	t.Cleanup(func() {
		if err := state.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := state.InitializeGeneration(); err != nil {
		t.Fatal(err)
	}
	if runner.payloadRan {
		t.Fatal("absent legacy active marker triggered unnecessary recovery")
	}
	assertLocalActive(t, source, true)
	if err := state.FinishSession(); err != nil {
		t.Fatal(err)
	}
	assertLocalActive(t, source, false)
	assertSyncState(t, state.paths.local, 4, false, false)
}

func TestFailedFinalSyncRetainsActiveSession(t *testing.T) {
	requireRclone(t)
	source, destination := t.TempDir(), t.TempDir()
	process := startTestProcess(t, "--logs", source, destination)
	t.Cleanup(func() { _ = process.cmd.Process.Kill() })
	// A conflicting destination directory makes this payload copy fail.
	if err := os.Mkdir(filepath.Join(destination, "blocked"), 0o700); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, filepath.Join(source, "blocked"), "payload")
	if err := process.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	var exitError *exec.ExitError
	if err := process.waitForExit(t); !errors.As(err, &exitError) || exitError.ExitCode() != 1 {
		t.Fatalf("process exit = %v, want status 1", err)
	}
	assertLocalActive(t, source, true)
}

func TestWatcherFailureRetainsActiveSession(t *testing.T) {
	requireRclone(t)
	source, destination, outside := t.TempDir(), t.TempDir(), t.TempDir()
	process := startTestProcess(t, "--logs", source, destination)
	t.Cleanup(func() { _ = process.cmd.Process.Kill() })
	moved := filepath.Join(outside, "moved-source")
	if err := os.Rename(source, moved); err != nil {
		t.Fatal(err)
	}
	var exitError *exec.ExitError
	if err := process.waitForExit(t); !errors.As(err, &exitError) || exitError.ExitCode() != 1 {
		t.Fatalf("process exit = %v, want status 1", err)
	}
	assertLocalActive(t, moved, true)
}

func TestWrappedCommandFailureWithSuccessfulSyncClearsActive(t *testing.T) {
	requireRclone(t)
	source, destination := t.TempDir(), t.TempDir()
	cfg, err := parseConfig([]string{source, destination, "--", "sh", "-c", `printf payload > "$1"; exit 7`, "sh", filepath.Join(source, "created.txt")})
	if err != nil {
		t.Fatal(err)
	}
	if got := run(cfg); got != 7 {
		t.Fatalf("exit status = %d, want 7", got)
	}
	assertLocalActive(t, source, false)
	if got := readTestFile(t, filepath.Join(destination, "created.txt")); got != "payload" {
		t.Fatalf("synced payload = %q", got)
	}
}

func TestLocalStateWriteFailurePreservesPreviousState(t *testing.T) {
	if os.Getenv("RCLONEWATCH_TEST_STATE_WRITE_FAILURE") != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestLocalStateWriteFailurePreservesPreviousState$")
		cmd.Env = append(os.Environ(), "RCLONEWATCH_TEST_STATE_WRITE_FAILURE=1")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("state-write failure test: %v\n%s", err, output)
		}
		return
	}
	path := filepath.Join(t.TempDir(), defaultStateFile)
	previous := syncFileData{Generation: 1, SyncID: "previous", Active: true}
	writeSyncState(t, path, previous)
	var saved syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &saved); err != nil {
		t.Fatal(err)
	}
	limited := saved
	limited.Cur = 8
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &limited); err != nil {
		t.Fatal(err)
	}
	writeErr := writeLocalSyncFile(path, syncFileData{Generation: 2, SyncID: "next", Active: true, Syncing: true})
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &saved); err != nil {
		t.Fatal(err)
	}
	if writeErr == nil {
		t.Fatal("expected injected state-write failure")
	}
	if got, exists, err := readLocalSyncFile(path); err != nil || !exists || got != previous {
		t.Fatalf("previous state was damaged: %#v (exists %v, error %v)", got, exists, err)
	}
}

func TestTemporaryStateFilesExcludedFromRecovery(t *testing.T) {
	requireRclone(t)
	source, destination := t.TempDir(), t.TempDir()
	data := syncFileData{Generation: 1, SyncID: "same", Active: true}
	writeSyncState(t, filepath.Join(source, defaultStateFile), data)
	data.Active = false
	writeSyncState(t, filepath.Join(destination, defaultStateFile), data)
	temporary := defaultStateFile + localStateTempPrefix + "leftover"
	writeTestFile(t, filepath.Join(source, temporary), "interrupted metadata write")
	state := acquireLocalState(t, source, destination)
	t.Cleanup(func() {
		if err := state.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := state.InitializeGeneration(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(destination, temporary)); !os.IsNotExist(err) {
		t.Fatalf("temporary local metadata was uploaded: %v", err)
	}
}
