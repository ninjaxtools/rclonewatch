package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSyncStateRecoversGenerationCollisionWithLocalRclone(t *testing.T) {
	requireRclone(t)
	for _, incomplete := range []bool{false, true} {
		name := "completed remote upload"
		if incomplete {
			name = "incomplete remote upload"
		}
		t.Run(name, func(t *testing.T) {
			clientA, clientB, destination := t.TempDir(), t.TempDir(), t.TempDir()
			// A crashed after writing its next local generation, before publishing it.
			writeSyncState(t, filepath.Join(clientA, defaultStateFile), syncFileData{Generation: 5, SyncID: "unpublished-A", Syncing: true})
			writeSyncState(t, filepath.Join(clientB, defaultStateFile), syncFileData{Generation: 4, SyncID: "previous"})
			writeSyncState(t, filepath.Join(destination, defaultStateFile), syncFileData{Generation: 4, SyncID: "previous"})
			writeTestFile(t, filepath.Join(clientA, "payload.txt"), "stale client A content")
			writeTestFile(t, filepath.Join(clientB, "payload.txt"), "new changes from client B")

			// B publishes a different upload at the same generation while A is offline.
			stateB := acquireLocalState(t, clientB, destination)
			t.Cleanup(func() {
				if err := stateB.Close(); err != nil {
					t.Error(err)
				}
			})
			if err := stateB.BeforeRemoteChange(); err != nil {
				t.Fatal(err)
			}
			if err := stateB.syncToRemote(); err != nil {
				t.Fatal(err)
			}
			if !incomplete {
				if err := stateB.AfterRemoteChange(); err != nil {
					t.Fatal(err)
				}
			}
			if err := stateB.Close(); err != nil {
				t.Fatal(err)
			}
			assertSyncState(t, stateB.paths.remote, 5, incomplete, false)
			publishedID := stateB.getRemote().data.SyncID
			if publishedID == "" || publishedID == "unpublished-A" {
				t.Fatalf("B's upload did not receive a unique sync ID: %q", publishedID)
			}

			// A must download B's upload instead of treating it as its own recovery.
			stateA := acquireLocalState(t, clientA, destination)
			t.Cleanup(func() {
				if err := stateA.Close(); err != nil {
					t.Error(err)
				}
			})
			if err := stateA.InitializeGeneration(); err != nil {
				t.Fatal(err)
			}
			for _, root := range []string{clientA, destination} {
				if got := readTestFile(t, filepath.Join(root, "payload.txt")); got != "new changes from client B" {
					t.Fatalf("payload in %s = %q, want B's changes", root, got)
				}
				assertSyncID(t, filepath.Join(root, defaultStateFile), publishedID)
			}
			assertSyncState(t, stateA.paths.local, 5, incomplete, false)
			assertSyncState(t, stateA.paths.remote, 5, incomplete, true)
		})
	}
}

func TestSyncStateWritesLocalIDBeforePublication(t *testing.T) {
	runner := &memorySyncRunner{}
	runner.setData(syncFileData{Generation: 4, SyncID: "previous"})
	state, _ := newMemoryState(t, runner, time.Hour, lockWait{}, false, false)
	writeSyncState(t, state.paths.local, syncFileData{Generation: 4, SyncID: "previous"})
	if err := state.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	state.Start()
	t.Cleanup(func() {
		if err := state.Close(); err != nil {
			t.Error(err)
		}
	})
	runner.setRefreshFailures(1)
	if err := state.BeforeRemoteChange(); err == nil {
		t.Fatal("publication failure was ignored")
	}
	assertSyncState(t, state.paths.local, 5, true, false)
	local, _, err := readLocalSyncFile(state.paths.local)
	if err != nil {
		t.Fatal(err)
	}
	if local.SyncID == "" || local.SyncID == "previous" {
		t.Fatalf("unpublished advance did not retain a fresh local sync ID: %q", local.SyncID)
	}
	if remote := runner.data(t); remote.Generation != 4 || remote.SyncID != "previous" || remote.Syncing {
		t.Fatalf("failed publication changed remote state: %#v", remote)
	}
	if err := state.InitializeGeneration(); err != nil {
		t.Fatal(err)
	}
	assertSyncState(t, state.paths.local, 5, false, false)
	if remote := runner.data(t); !runner.payloadRan || remote.SyncID == "" || remote.SyncID == local.SyncID || remote.SyncID == "previous" {
		t.Fatalf("unpublished upload was not recovered with a fresh sync ID: %#v", remote)
	}
	assertSyncID(t, state.paths.local, runner.data(t).SyncID)
}

func TestSyncStateRejectsChangedRemoteSyncID(t *testing.T) {
	runner := &memorySyncRunner{}
	runner.setData(syncFileData{Generation: 4, SyncID: "original"})
	state, _ := newMemoryState(t, runner, time.Hour, lockWait{}, false, false)
	if err := state.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	state.Start()
	previous := runner.data(t)
	t.Cleanup(func() {
		runner.setData(previous)
		if err := state.Close(); err != nil {
			t.Error(err)
		}
	})
	changed := previous
	changed.SyncID = "unexpected-replacement"
	runner.setData(changed)
	if err := state.BeforeRemoteChange(); !errors.Is(err, errLockOwnershipLost) {
		t.Fatalf("changed sync ID error = %v, want ownership loss", err)
	}
	if remote := runner.data(t); remote.SyncID != changed.SyncID || remote.Generation != changed.Generation {
		t.Fatalf("state update overwrote changed sync ID: %#v", remote)
	}
}

func TestSyncStateRejectsAmbiguousLegacyRecovery(t *testing.T) {
	for _, flags := range []struct{ local, remote bool }{{true, false}, {false, true}, {true, true}} {
		runner := &memorySyncRunner{}
		runner.setData(syncFileData{Generation: 4, Syncing: flags.remote})
		state, _ := newMemoryState(t, runner, time.Hour, lockWait{}, false, false)
		writeSyncState(t, state.paths.local, syncFileData{Generation: 4, Syncing: flags.local})
		if err := state.Acquire(make(chan os.Signal)); err != nil {
			t.Fatal(err)
		}
		state.Start()
		t.Cleanup(func() {
			if err := state.Close(); err != nil {
				t.Error(err)
			}
		})
		if err := state.InitializeGeneration(); err == nil || !strings.Contains(err.Error(), "without sync IDs") {
			t.Fatalf("legacy recovery error = %v, want ambiguous sync ID error", err)
		}
		if runner.payloadRan {
			t.Fatal("ambiguous legacy state triggered a payload transfer")
		}
		assertSyncState(t, state.paths.local, 4, flags.local, false)
		if remote := runner.data(t); remote.Generation != 4 || remote.SyncID != "" || remote.Syncing != flags.remote {
			t.Fatalf("ambiguous recovery changed remote generation state: %#v", remote)
		}
	}
}
