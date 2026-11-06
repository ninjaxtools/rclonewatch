package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type scopedRecordingRunner struct {
	commandRunner
	recorded recordingRunner
	failure  error
}

func (r *scopedRecordingRunner) Run(args []string, stdout, stderr io.Writer) error {
	if err := r.recorded.Run(args, stdout, stderr); err != nil {
		return err
	}
	if r.failure != nil {
		return r.failure
	}
	return r.commandRunner.Run(args, stdout, stderr)
}

func assertUnrelatedPayload(t *testing.T, destination string) {
	t.Helper()
	for name, want := range map[string]string{"unrelated/file": "remote version", "remote-only": "keep"} {
		if got := readTestFile(t, filepath.Join(destination, name)); got != want {
			t.Fatalf("unrelated %s was changed: %q", name, got)
		}
	}
}

func seedUnrelatedPayload(t *testing.T, source, destination string) {
	t.Helper()
	writePayload(t, source, "unrelated/file", "local version")
	writePayload(t, destination, "unrelated/file", "remote version")
	writePayload(t, destination, "remote-only", "keep")
}

func TestSubtreeSyncLeavesUnrelatedPayloadUntouched(t *testing.T) {
	requireRclone(t)
	source, destination := t.TempDir(), t.TempDir()
	seedUnrelatedPayload(t, source, destination)
	writePayload(t, source, "changed/nested/new", "new")
	writePayload(t, destination, "changed/old", "obsolete")
	s := syncer{source: source, dest: destination, runner: &rcloneCommand{}}
	if err := s.Sync(map[string]change{"changed": {path: "changed", isDir: true}}); err != nil {
		t.Fatal(err)
	}
	if got := readTestFile(t, filepath.Join(destination, "changed/nested/new")); got != "new" {
		t.Fatalf("subtree content = %q", got)
	}
	if _, err := os.Stat(filepath.Join(destination, "changed/old")); !os.IsNotExist(err) {
		t.Fatalf("old subtree content remains: %v", err)
	}
	assertUnrelatedPayload(t, destination)
}

func TestExplicitFullSyncStillReconcilesRoot(t *testing.T) {
	requireRclone(t)
	source, destination := t.TempDir(), t.TempDir()
	seedUnrelatedPayload(t, source, destination)
	s := syncer{source: source, dest: destination, runner: &rcloneCommand{}}
	if err := s.Sync(map[string]change{".": {path: ".", isDir: true}}); err != nil {
		t.Fatal(err)
	}
	if got := readTestFile(t, filepath.Join(destination, "unrelated/file")); got != "local version" {
		t.Fatalf("full reconciliation did not update payload: %q", got)
	}
	if _, err := os.Stat(filepath.Join(destination, "remote-only")); !os.IsNotExist(err) {
		t.Fatalf("full reconciliation left remote-only payload: %v", err)
	}
}

func TestSubtreeScopesCollapseOverlappingChanges(t *testing.T) {
	source := t.TempDir()
	for _, dir := range []string{"a/b", "a-b", "ab"} {
		if err := os.MkdirAll(filepath.Join(source, dir), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	writePayload(t, source, "a-b/file", "file")
	writePayload(t, source, "ab/file", "file")
	s := syncer{source: source}
	scopes, err := s.reconciliationScopes([]string{"a/b/file", "a-b/file", "a", "a/b", "ab/file", "a"})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"a", "a-b/file", "ab/file"}; !reflect.DeepEqual(scopes, want) {
		t.Fatalf("scopes = %#v, want %#v", scopes, want)
	}
	runner := &recordingRunner{}
	s.runner = runner
	if err := s.syncScopes(scopes); err != nil {
		t.Fatal(err)
	}
	if len(runner.commands) != 1 {
		t.Fatalf("payload commands = %d, want 1", len(runner.commands))
	}
	for _, rule := range runner.commands[0].rules {
		if strings.HasPrefix(rule, "+ /a/b") {
			t.Fatalf("descendant was redundantly scheduled: %q", rule)
		}
	}
}

func TestScopedDeletionRetainsExcludedAndUnrelatedPayload(t *testing.T) {
	requireRclone(t)
	for _, excluded := range []bool{false, true} {
		source, destination := t.TempDir(), t.TempDir()
		seedUnrelatedPayload(t, source, destination)
		writePayload(t, source, "parent/sibling", "local sibling")
		writePayload(t, destination, "parent/sibling", "remote sibling")
		writePayload(t, destination, "parent/deleted/old", "obsolete")
		if excluded {
			writePayload(t, destination, "parent/deleted/preserved.keep", "protected")
		}
		s := syncer{source: source, dest: destination, runner: &rcloneCommand{}, excludes: []string{"*.keep"}}
		batch := map[string]change{"parent/deleted": {path: "parent/deleted", isDir: true, removed: true}}
		for i := 0; i < 2; i++ {
			if err := s.Sync(batch); err != nil {
				t.Fatalf("excluded=%v attempt=%d: %v", excluded, i, err)
			}
		}
		if _, err := os.Stat(filepath.Join(destination, "parent/deleted/old")); !os.IsNotExist(err) {
			t.Fatalf("deleted child remains: %v", err)
		}
		if excluded {
			if got := readTestFile(t, filepath.Join(destination, "parent/deleted/preserved.keep")); got != "protected" {
				t.Fatalf("excluded child = %q", got)
			}
		} else if _, err := os.Stat(filepath.Join(destination, "parent/deleted")); !os.IsNotExist(err) {
			t.Fatalf("empty deleted directory remains: %v", err)
		}
		if got := readTestFile(t, filepath.Join(destination, "parent/sibling")); got != "remote sibling" {
			t.Fatalf("sibling was synced: %q", got)
		}
		assertUnrelatedPayload(t, destination)
	}
}

func TestScopedDeletionDoesNotReplaceRemoteAncestorFile(t *testing.T) {
	requireRclone(t)
	source, destination := t.TempDir(), t.TempDir()
	seedUnrelatedPayload(t, source, destination)
	if err := os.Mkdir(filepath.Join(source, "parent"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(source, "new-empty"), 0o700); err != nil {
		t.Fatal(err)
	}
	writePayload(t, destination, "parent", "preserved ancestor file")
	s := syncer{source: source, dest: destination, runner: &rcloneCommand{}}
	batch := map[string]change{
		"parent/deleted": {path: "parent/deleted", isDir: true, removed: true},
		"new-empty":      {path: "new-empty", isDir: true},
	}
	if err := s.Sync(batch); err != nil {
		t.Fatal(err)
	}
	if got := readTestFile(t, filepath.Join(destination, "parent")); got != "preserved ancestor file" {
		t.Fatalf("deletion replaced unrelated ancestor: %q", got)
	}
	if info, err := os.Stat(filepath.Join(destination, "new-empty")); err != nil || !info.IsDir() {
		t.Fatalf("new empty directory missing: %v", err)
	}
	assertUnrelatedPayload(t, destination)
}

func TestScopedTypeReplacementsLeaveSiblingsUntouched(t *testing.T) {
	requireRclone(t)
	for _, directorySource := range []bool{false, true} {
		source, destination := t.TempDir(), t.TempDir()
		seedUnrelatedPayload(t, source, destination)
		fileRoot, directoryRoot := source, destination
		if directorySource {
			fileRoot, directoryRoot = destination, source
		}
		writePayload(t, fileRoot, "parent/entry", "file")
		writePayload(t, directoryRoot, "parent/entry/child", "child")
		writePayload(t, source, "parent/sibling", "local sibling")
		writePayload(t, destination, "parent/sibling", "remote sibling")
		batch := make(map[string]change)
		addPending(batch, change{path: "parent/entry", isDir: !directorySource, removed: true})
		addPending(batch, change{path: "parent/entry", isDir: directorySource})
		s := syncer{source: source, dest: destination, runner: &rcloneCommand{}}
		if err := s.Sync(batch); err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(filepath.Join(destination, "parent/entry"))
		if err != nil || info.IsDir() != directorySource {
			t.Fatalf("replacement type = %v, error %v", info, err)
		}
		if got := readTestFile(t, filepath.Join(destination, "parent/sibling")); got != "remote sibling" {
			t.Fatalf("sibling was synced: %q", got)
		}
		assertUnrelatedPayload(t, destination)
	}
}

func TestScopedSyncHandlesChangedAncestors(t *testing.T) {
	requireRclone(t)
	for _, direction := range []string{"local file", "remote file", "missing local parent"} {
		t.Run(direction, func(t *testing.T) {
			source, destination := t.TempDir(), t.TempDir()
			seedUnrelatedPayload(t, source, destination)
			if direction == "remote file" {
				writePayload(t, source, "parent/entry/child", "child")
				writePayload(t, destination, "parent/entry", "old file")
			} else {
				writePayload(t, destination, "parent/entry/child", "obsolete")
				writePayload(t, destination, "parent/entry/other", "obsolete")
				if direction == "local file" {
					writePayload(t, source, "parent/entry", "new file")
				}
			}
			writePayload(t, source, "parent/sibling", "local sibling")
			writePayload(t, destination, "parent/sibling", "remote sibling")
			s := syncer{source: source, dest: destination, runner: &rcloneCommand{}}
			if err := s.Sync(map[string]change{"parent/entry/child": {path: "parent/entry/child"}}); err != nil {
				t.Fatal(err)
			}
			switch direction {
			case "remote file":
				if got := readTestFile(t, filepath.Join(destination, "parent/entry/child")); got != "child" {
					t.Fatalf("child = %q", got)
				}
			case "local file":
				if got := readTestFile(t, filepath.Join(destination, "parent/entry")); got != "new file" {
					t.Fatalf("parent = %q", got)
				}
			default:
				if _, err := os.Stat(filepath.Join(destination, "parent/entry")); !os.IsNotExist(err) {
					t.Fatalf("missing parent remains: %v", err)
				}
			}
			if got := readTestFile(t, filepath.Join(destination, "parent/sibling")); got != "remote sibling" {
				t.Fatalf("sibling was synced: %q", got)
			}
			assertUnrelatedPayload(t, destination)
		})
	}
}

func TestScopedExclusionsRemainRootRelative(t *testing.T) {
	requireRclone(t)
	source, destination := t.TempDir(), t.TempDir()
	seedUnrelatedPayload(t, source, destination)
	writePayload(t, source, "changed/cache/file", "included nested cache")
	writePayload(t, source, "changed/private/file", "do not upload")
	writePayload(t, destination, "changed/private/file", "protected")
	writePayload(t, source, "changed/nested/file.tmp", "do not upload")
	writePayload(t, destination, "changed/nested/file.tmp", "protected temporary")
	writePayload(t, destination, "changed/obsolete", "remove")
	s := syncer{source: source, dest: destination, runner: &rcloneCommand{}, excludes: []string{"/cache/**", "/changed/private/**", "*.tmp"}}
	if err := s.Sync(map[string]change{"changed": {path: "changed", isDir: true}}); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{"changed/cache/file": "included nested cache", "changed/private/file": "protected", "changed/nested/file.tmp": "protected temporary"} {
		if got := readTestFile(t, filepath.Join(destination, name)); got != want {
			t.Fatalf("%s = %q, want %q", name, got, want)
		}
	}
	assertUnrelatedPayload(t, destination)
}

func TestScopedMetadataAncestorPreservesState(t *testing.T) {
	requireRclone(t)
	source, destination := t.TempDir(), t.TempDir()
	seedUnrelatedPayload(t, source, destination)
	state := &syncState{paths: syncFilePaths{localFilter: ".meta/local[1]", remoteFilter: ".meta/remote{1}"}}
	writePayload(t, source, state.paths.localFilter, "local metadata")
	writePayload(t, destination, state.paths.remoteFilter, "remote metadata")
	writePayload(t, source, state.paths.localFilter+localStateTempPrefix+"old", "temporary metadata")
	writePayload(t, source, ".meta/new", "new payload")
	writePayload(t, destination, ".meta/obsolete", "remove")
	s := syncer{source: source, dest: destination, state: state, runner: &rcloneCommand{}}
	batch := map[string]change{".meta": {path: ".meta", isDir: true}}
	if err := s.Sync(batch); err != nil {
		t.Fatal(err)
	}
	if got := readTestFile(t, filepath.Join(destination, state.paths.remoteFilter)); got != "remote metadata" {
		t.Fatalf("remote metadata = %q", got)
	}
	if _, err := os.Stat(filepath.Join(destination, state.paths.localFilter)); !os.IsNotExist(err) {
		t.Fatalf("local metadata uploaded: %v", err)
	}
	if _, err := os.Stat(filepath.Join(destination, state.paths.localFilter+localStateTempPrefix+"old")); !os.IsNotExist(err) {
		t.Fatalf("temporary metadata uploaded: %v", err)
	}
	if got := readTestFile(t, filepath.Join(destination, ".meta/new")); got != "new payload" {
		t.Fatalf("metadata sibling payload = %q", got)
	}
	if err := os.RemoveAll(filepath.Join(source, ".meta")); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(batch); err != nil {
		t.Fatal(err)
	}
	if got := readTestFile(t, filepath.Join(destination, state.paths.remoteFilter)); got != "remote metadata" {
		t.Fatalf("metadata lost on ancestor deletion: %q", got)
	}
	if _, err := os.Stat(filepath.Join(destination, ".meta/new")); !os.IsNotExist(err) {
		t.Fatalf("deleted ancestor payload remains: %v", err)
	}
	assertUnrelatedPayload(t, destination)
}

func TestScopedFailureDoesNotExpandAndRetryUsesNewerEvents(t *testing.T) {
	requireRclone(t)
	source, destination := t.TempDir(), t.TempDir()
	seedUnrelatedPayload(t, source, destination)
	writePayload(t, source, "changed/child", "new")
	writePayload(t, destination, "changed/child", "old")
	failure := errors.New("injected sync failure")
	runner := &scopedRecordingRunner{commandRunner: &rcloneCommand{}, failure: failure}
	s := syncer{source: source, dest: destination, runner: runner}
	failed := map[string]change{"changed": {path: "changed", isDir: true}}
	if err := s.Sync(failed); !errors.Is(err, failure) {
		t.Fatalf("sync error = %v, want injected failure", err)
	}
	if len(runner.recorded.commands) != 1 {
		t.Fatal("failed scoped sync expanded into another attempt")
	}
	if err := os.RemoveAll(filepath.Join(source, "changed")); err != nil {
		t.Fatal(err)
	}
	writePayload(t, source, "changed", "replacement file")
	pending := map[string]change{"changed": {path: "changed"}}
	mergeFailedBatch(pending, failed)
	if len(pending) != 1 || pending["changed"].isDir {
		t.Fatalf("failed batch overwrote newer event or requested full sync: %#v", pending)
	}
	runner.failure = nil
	if err := s.Sync(pending); err != nil {
		t.Fatal(err)
	}
	if got := readTestFile(t, filepath.Join(destination, "changed")); got != "replacement file" {
		t.Fatalf("retry payload = %q", got)
	}
	assertUnrelatedPayload(t, destination)
}

func TestScopedNamesAreLiteral(t *testing.T) {
	requireRclone(t)
	for _, name := range []string{"[a]*?{b}", " space ", "unicode\u00a0", "line\nbreak", "line␊break", "del\x7f", "literal␡", "quoted‛‛name", "．"} {
		t.Run(name, func(t *testing.T) {
			source, destination := t.TempDir(), t.TempDir()
			seedUnrelatedPayload(t, source, destination)
			writePayload(t, source, name+"/file", "new")
			s := syncer{source: source, dest: destination, runner: &rcloneCommand{}}
			if err := s.Sync(map[string]change{name: {path: name, isDir: true}}); err != nil {
				t.Fatal(err)
			}
			if got := readTestFile(t, filepath.Join(destination, name, "file")); got != "new" {
				t.Fatalf("literal scope content = %q", got)
			}
			if err := os.RemoveAll(filepath.Join(source, name)); err != nil {
				t.Fatal(err)
			}
			if err := s.Sync(map[string]change{name: {path: name, isDir: true, removed: true}}); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(filepath.Join(destination, name)); !os.IsNotExist(err) {
				t.Fatalf("literal scope was not deleted: %v", err)
			}
			assertUnrelatedPayload(t, destination)
		})
	}
}

func TestNonUTF8ChangeUsesNearestRepresentableScope(t *testing.T) {
	requireRclone(t)
	source, destination := t.TempDir(), t.TempDir()
	seedUnrelatedPayload(t, source, destination)
	name := "changed/invalid-\xff/file"
	writePayload(t, source, name, "payload")
	runner := &scopedRecordingRunner{commandRunner: &rcloneCommand{}}
	s := syncer{source: source, dest: destination, runner: runner}
	if err := s.Sync(map[string]change{name: {path: name}}); err != nil {
		t.Fatal(err)
	}
	if got := readTestFile(t, filepath.Join(destination, name)); got != "payload" {
		t.Fatalf("non-UTF-8 payload = %q", got)
	}
	if want := []string{"+ /changed", "+ /changed/**", "- /**"}; !reflect.DeepEqual(runner.recorded.commands[0].rules, want) {
		t.Fatalf("non-UTF-8 scope = %#v, want %#v", runner.recorded.commands[0].rules, want)
	}
	assertUnrelatedPayload(t, destination)
}
