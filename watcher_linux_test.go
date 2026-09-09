//go:build linux

package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
