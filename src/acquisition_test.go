package main

import (
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

type changedWhileWaitingRunner struct {
	*memorySyncRunner
	reads       int
	replacement *syncFileData
}

func (r *changedWhileWaitingRunner) Run(args []string, stdout, stderr io.Writer) error {
	if args[0] == "cat" {
		r.reads++
		if r.reads == 2 {
			if r.replacement != nil {
				r.setData(*r.replacement)
			} else {
				r.mu.Lock()
				r.value = ""
				r.remoteFiles = true
				r.mu.Unlock()
			}
		}
	}
	return r.memorySyncRunner.Run(args, stdout, stderr)
}

func TestAcquisitionRevalidatesAfterWaiting(t *testing.T) {
	for _, test := range []struct {
		name        string
		replacement *syncFileData
		wantError   string
	}{
		{"incomplete upload", &syncFileData{Generation: 2, SyncID: "failed", Syncing: true}, "incomplete"},
		{"untracked payload", nil, "--force-delete-untracked-remote"},
	} {
		t.Run(test.name, func(t *testing.T) {
			memory := &memorySyncRunner{}
			memory.setData(syncFileData{Generation: 1, Lock: &syncFileLock{Owner: "other", Timestamp: time.Now(), TTL: time.Second}})
			state, _ := newMemoryState(t, memory, time.Second, lockWait{infinite: true, explicit: true}, false, true)
			state.pollInterval = time.Millisecond
			state.runner = &changedWhileWaitingRunner{memorySyncRunner: memory, replacement: test.replacement}
			if err := state.Acquire(make(chan os.Signal)); err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("Acquire = %v, want %q", err, test.wantError)
			}
			if got := memory.writes(); got != 0 {
				t.Fatalf("unsafe remote writes = %d", got)
			}
		})
	}
}
