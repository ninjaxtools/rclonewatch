//go:build linux

package main

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestWatcherRebuildsCoverageAfterOverflow(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	oldDir := filepath.Join(root, "old")
	if err := os.Mkdir(oldDir, 0o700); err != nil {
		t.Fatal(err)
	}
	// Construct the watcher without a read goroutine to deterministically drop
	// directory events, as would happen when the kernel queue overflows.
	w := &watcher{root: root, fd: -1, events: make(chan change, 4096), errors: make(chan error, 1), stop: make(chan struct{}), done: make(chan struct{})}
	if err := w.rebuild(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(oldDir, filepath.Join(outside, "old")); err != nil {
		t.Fatal(err)
	}
	newDir := filepath.Join(root, "new")
	if err := os.Mkdir(newDir, 0o700); err != nil {
		t.Fatal(err)
	}
	w.handle(-1, syscall.IN_Q_OVERFLOW, "")
	if _, ok := w.byPath[oldDir]; ok {
		t.Fatal("stale watch survived overflow")
	}
	if _, ok := w.byPath[newDir]; !ok {
		t.Fatal("new directory was not watched after overflow")
	}
	select {
	case item := <-w.Events():
		if item.path != "." {
			t.Fatalf("overflow event = %#v", item)
		}
	default:
		t.Fatal("overflow did not request full reconciliation")
	}
	go w.readLoop()
	t.Cleanup(func() {
		w.Close()
		for range w.Events() {
		}
	})
	writeTestFile(t, filepath.Join(newDir, "later.txt"), "later")
	deadline := time.After(3 * time.Second)
	for {
		select {
		case item := <-w.Events():
			if item.path == "new/later.txt" {
				return
			}
		case <-deadline:
			t.Fatal("changes under rebuilt watch were missed")
		}
	}
}

func TestWatcherSeesRecursiveChanges(t *testing.T) {
	root := t.TempDir()
	w, err := newWatcher(root)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	directory := filepath.Join(root, "new directory")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(directory, "file.txt")
	if err := os.WriteFile(file, []byte("contents"), 0o600); err != nil {
		t.Fatal(err)
	}

	want := map[string]bool{"new directory": true, "new directory/file.txt": false}
	deadline := time.After(5 * time.Second)
	for len(want) > 0 {
		select {
		case item := <-w.Events():
			if isDir, exists := want[item.path]; exists {
				if item.isDir != isDir {
					t.Fatalf("change %q isDir = %v, want %v", item.path, item.isDir, isDir)
				}
				delete(want, item.path)
			}
		case err := <-w.Errors():
			if err != nil {
				t.Fatal(err)
			}
		case <-deadline:
			t.Fatalf("timed out waiting for changes: %#v", want)
		}
	}
}

func TestWatcherDrainsQueuedChangesOnClose(t *testing.T) {
	root := t.TempDir()
	w, err := newWatcher(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "last change"), []byte("contents"), 0o600); err != nil {
		t.Fatal(err)
	}
	w.Close()

	found := false
	for item := range w.Events() {
		if item.path == "last change" {
			found = true
		}
	}
	if !found {
		t.Fatal("change queued immediately before Close was lost")
	}
	for err := range w.Errors() {
		if err != nil {
			t.Fatal(err)
		}
	}
}
