package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

type startupWriteRunner struct {
	commandRunner
	source string
	count  int
	wrote  bool
}

func (r *startupWriteRunner) Run(args []string, stdout, stderr io.Writer) error {
	if err := r.commandRunner.Run(args, stdout, stderr); err != nil {
		return err
	}
	if args[0] == "sync" && args[1] == r.source && !r.wrote {
		r.wrote = true
		for i := 0; i < r.count; i++ {
			if err := os.WriteFile(filepath.Join(r.source, fmt.Sprintf("late-%d.txt", i)), []byte("late write"), 0o600); err != nil {
				return err
			}
		}
	}
	return nil
}

func TestStartupRetainsWritesAfterInitialScan(t *testing.T) {
	requireRclone(t)
	source, destination := t.TempDir(), t.TempDir()
	state := acquireLocalState(t, source, destination)
	t.Cleanup(func() {
		if err := state.Close(); err != nil {
			t.Error(err)
		}
	})
	state.runner = &startupWriteRunner{commandRunner: &rcloneCommand{}, source: source, count: 1}
	w, err := newWatcher(source)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		w.Close()
		for range w.Events() {
		}
	})
	pending := make(map[string]change)
	if err := initializeWatching(state, w, pending); err != nil {
		t.Fatal(err)
	}
	w.Close()
	for item := range w.Events() {
		if exact, _ := state.protects(item.path); !exact {
			addPending(pending, item)
		}
	}
	s := syncer{source: source, dest: destination, state: state, trackState: true, runner: &rcloneCommand{}}
	if err := s.Sync(pending); err != nil {
		t.Fatal(err)
	}
	if got := readTestFile(t, filepath.Join(destination, "late-0.txt")); got != "late write" {
		t.Fatalf("late write = %q", got)
	}
}

func TestStartupDrainsMoreThanWatchChannelCapacity(t *testing.T) {
	memory := &memorySyncRunner{}
	state, source := newMemoryState(t, memory, time.Hour, lockWait{}, false, false)
	if err := state.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	state.Start()
	t.Cleanup(func() {
		if err := state.Close(); err != nil {
			t.Error(err)
		}
	})
	state.runner = &startupWriteRunner{commandRunner: memory, source: source, count: 5000}
	w, err := newWatcher(source)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		w.Close()
		for range w.Events() {
		}
	})
	pending := make(map[string]change)
	if err := initializeWatching(state, w, pending); err != nil {
		t.Fatal(err)
	}
	w.Close()
	for item := range w.Events() {
		if exact, _ := state.protects(item.path); !exact {
			addPending(pending, item)
		}
	}
	if _, full := pending["."]; !full {
		for i := 0; i < 5000; i++ {
			if _, exists := pending[fmt.Sprintf("late-%d.txt", i)]; !exists {
				t.Fatalf("startup change %d was lost", i)
			}
		}
	}
}

func TestSymlinkSourceIsWatched(t *testing.T) {
	requireRclone(t)
	source, parent, destination := t.TempDir(), t.TempDir(), t.TempDir()
	link := filepath.Join(parent, "source")
	if err := os.Symlink(source, link); err != nil {
		t.Fatal(err)
	}
	process := startTestProcess(t, "--logs", link, destination)
	t.Cleanup(func() { _ = process.cmd.Process.Kill() })
	writeTestFile(t, filepath.Join(link, "new.txt"), "new file")
	if err := process.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := process.waitForExit(t); err != nil {
		t.Fatal(err)
	}
	if got := readTestFile(t, filepath.Join(destination, "new.txt")); got != "new file" {
		t.Fatalf("symlink source payload = %q", got)
	}
}
