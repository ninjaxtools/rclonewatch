package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writePayload(t *testing.T, root, relative, contents string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, path, contents)
}

func TestDirectoryReplacementRemovesOldContents(t *testing.T) {
	requireRclone(t)
	source, destination, outside := t.TempDir(), t.TempDir(), t.TempDir()
	writePayload(t, source, "d/old.txt", "old")
	writePayload(t, destination, "d/old.txt", "old")
	w, err := newWatcher(source)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		w.Close()
		for range w.Events() {
		}
	})
	if err := os.Rename(filepath.Join(source, "d"), filepath.Join(outside, "old")); err != nil {
		t.Fatal(err)
	}
	writePayload(t, outside, "new/new.txt", "new")
	if err := os.Rename(filepath.Join(outside, "new"), filepath.Join(source, "d")); err != nil {
		t.Fatal(err)
	}
	w.Close()
	batch := make(map[string]change)
	for item := range w.Events() {
		addPending(batch, item)
	}
	s := syncer{source: source, dest: destination, runner: &rcloneCommand{}}
	if err := s.Sync(batch); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(destination, "d", "old.txt")); !os.IsNotExist(err) {
		t.Fatalf("obsolete remote file remains: %v", err)
	}
	if got := readTestFile(t, filepath.Join(destination, "d", "new.txt")); got != "new" {
		t.Fatalf("replacement payload = %q", got)
	}
}

func TestDirectoryDeletionIsIdempotent(t *testing.T) {
	requireRclone(t)
	for _, existed := range []bool{false, true} {
		source, destination := t.TempDir(), t.TempDir()
		if existed {
			writePayload(t, destination, "temporary/file", "old")
		}
		s := syncer{source: source, dest: destination, runner: &rcloneCommand{}}
		batch := map[string]change{"temporary": {path: "temporary", isDir: true, removed: true}}
		for attempt := 0; attempt < 2; attempt++ {
			if err := s.Sync(batch); err != nil {
				t.Fatalf("existed=%v attempt=%d: %v", existed, attempt, err)
			}
		}
		if _, err := os.Stat(filepath.Join(destination, "temporary")); !os.IsNotExist(err) {
			t.Fatalf("directory remains: %v", err)
		}
	}
}

func TestSyncReplacesFileAndDirectoryTypes(t *testing.T) {
	requireRclone(t)
	for _, fileToDir := range []bool{true, false} {
		for _, download := range []bool{false, true} {
			source, destination := t.TempDir(), t.TempDir()
			fileRoot, dirRoot := source, destination
			if fileToDir {
				fileRoot, dirRoot = destination, source
			}
			writePayload(t, fileRoot, "entry", "file")
			writePayload(t, dirRoot, "entry/child", "child")
			if download {
				writeSyncState(t, filepath.Join(source, defaultStateFile), syncFileData{Generation: 1, SyncID: "old"})
				writeSyncState(t, filepath.Join(destination, defaultStateFile), syncFileData{Generation: 2, SyncID: "new"})
				state := acquireLocalState(t, source, destination)
				err := state.InitializeGeneration()
				closeErr := state.Close()
				if err != nil || closeErr != nil {
					t.Fatalf("download type change: sync=%v close=%v", err, closeErr)
				}
			} else {
				batch := make(map[string]change)
				addPending(batch, change{path: "entry", isDir: !fileToDir, removed: true})
				addPending(batch, change{path: "entry", isDir: fileToDir})
				s := syncer{source: source, dest: destination, runner: &rcloneCommand{}}
				if err := s.Sync(batch); err != nil {
					t.Fatalf("upload type change: %v", err)
				}
			}
			wantDir := fileToDir != download
			for _, root := range []string{source, destination} {
				info, err := os.Stat(filepath.Join(root, "entry"))
				if err != nil || info.IsDir() != wantDir {
					t.Fatalf("type in %s: info=%v error=%v wantDir=%v", root, info, err, wantDir)
				}
			}
		}
	}
}

func TestScopedTypeConflictReconcilesDestination(t *testing.T) {
	requireRclone(t)
	source, destination := t.TempDir(), t.TempDir()
	seedUnrelatedPayload(t, source, destination)
	writePayload(t, source, "entry", "file")
	writePayload(t, destination, "entry/child", "old")
	s := syncer{source: source, dest: destination, runner: &rcloneCommand{}}
	if err := s.Sync(map[string]change{"entry": {path: "entry"}}); err != nil {
		t.Fatal(err)
	}
	if got := readTestFile(t, filepath.Join(destination, "entry")); got != "file" {
		t.Fatalf("type conflict payload = %q", got)
	}
	assertUnrelatedPayload(t, destination)
}

func TestChangedContentWithPreservedModTime(t *testing.T) {
	requireRclone(t)
	for _, full := range []bool{false, true} {
		source, destination := t.TempDir(), t.TempDir()
		file := filepath.Join(source, "file")
		writeTestFile(t, file, "old")
		s := syncer{source: source, dest: destination, runner: &rcloneCommand{}}
		batch := map[string]change{"file": {path: "file"}}
		if full {
			batch["."] = change{path: ".", isDir: true}
		}
		if err := s.Sync(batch); err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(file)
		if err != nil {
			t.Fatal(err)
		}
		writeTestFile(t, file, "new")
		if err := os.Chtimes(file, info.ModTime(), info.ModTime()); err != nil {
			t.Fatal(err)
		}
		if err := s.Sync(batch); err != nil {
			t.Fatal(err)
		}
		if got := readTestFile(t, filepath.Join(destination, "file")); got != "new" {
			t.Fatalf("full=%v: preserved mtime hid content change: %q", full, got)
		}
	}
}

func TestLiteralStatePathsWithRclone(t *testing.T) {
	requireRclone(t)
	for _, name := range []string{"[state].json", "state*.json", "state?.json", "{{state}}.json", `meta[1]/st{a,b}te\file.json`} {
		t.Run(name, func(t *testing.T) {
			source, destination := t.TempDir(), t.TempDir()
			paths, err := resolveSyncFilePaths(source, destination, name, name)
			if err != nil {
				t.Fatal(err)
			}
			state := newLocalState(t, source, destination)
			state.paths = paths
			state.timeout = time.Second
			state.pollInterval = 10 * time.Millisecond
			// These payloads would be hidden by an unescaped metadata glob.
			writePayload(t, source, "s.json", "payload")
			writePayload(t, source, "stateX.json", "payload")
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
			for _, path := range []string{"s.json", "stateX.json"} {
				if got := readTestFile(t, filepath.Join(destination, path)); got != "payload" {
					t.Fatalf("payload %s = %q", path, got)
				}
			}
			if exact, _ := state.protects(paths.localFilter); !exact {
				t.Fatal("literal state path was not protected from events")
			}
			if exact, _ := state.protects(paths.localFilter + localStateTempPrefix + "123"); !exact {
				t.Fatal("literal temporary state path was not protected")
			}
			// Both directions must preserve the metadata, including the remote lock.
			if err := state.syncFromRemote(); err != nil {
				t.Fatal(err)
			}
			assertSyncState(t, state.paths.local, 1, false, false)
			assertSyncState(t, state.paths.remote, 1, false, true)
		})
	}
}

func TestTypeConflictCannotDeleteStateAncestor(t *testing.T) {
	requireRclone(t)
	source, destination := t.TempDir(), t.TempDir()
	state := newLocalState(t, source, destination)
	paths, err := resolveSyncFilePaths(source, destination, ".local-state", "metadata/state")
	if err != nil {
		t.Fatal(err)
	}
	state.paths = paths
	writePayload(t, source, "metadata", "conflicting file")
	if err := state.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	state.Start()
	t.Cleanup(func() {
		if err := state.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := state.InitializeGeneration(); err == nil {
		t.Fatal("type conflict with protected metadata unexpectedly succeeded")
	}
	remote, err := state.readRemote()
	if err != nil || !remote.ownedBy(state.owner) {
		t.Fatalf("protected remote state was damaged: %#v %v", remote, err)
	}
}

func TestInitializeMissingPayloadRootWithExternalState(t *testing.T) {
	requireRclone(t)
	source, parent := t.TempDir(), t.TempDir()
	destination := filepath.Join(parent, "missing-payload")
	state := newLocalState(t, source, destination)
	paths, err := resolveSyncFilePaths(source, destination, ".local-state", "../remote-state")
	if err != nil {
		t.Fatal(err)
	}
	state.paths = paths
	writePayload(t, source, "file", "payload")
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
	if got := readTestFile(t, filepath.Join(destination, "file")); got != "payload" {
		t.Fatalf("initialized payload = %q", got)
	}
	assertSyncState(t, state.paths.remote, 1, false, true)
}
