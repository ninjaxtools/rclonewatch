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

type memoryLockRunner struct {
	mu                  sync.Mutex
	value               string
	lastCopyArgs        []string
	conditionalFailures int
}

func (r *memoryLockRunner) Run(args []string, stdout, stderr io.Writer) error {
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
			r.value = time.Now().UTC().Format(time.RFC3339Nano)
			return errors.New("conditional write failed")
		}
		contents, err := os.ReadFile(args[1])
		if err != nil {
			return err
		}
		r.value = strings.TrimSpace(string(contents))
		r.lastCopyArgs = append([]string(nil), args...)
	case "deletefile":
		r.value = ""
	}
	return nil
}

func (r *memoryLockRunner) set(value string) {
	r.mu.Lock()
	r.value = value
	r.mu.Unlock()
}

func (r *memoryLockRunner) copyArgs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.lastCopyArgs...)
}

func TestRemoteLockLifecycleWithLocalRclone(t *testing.T) {
	if _, err := exec.LookPath("rclone"); err != nil {
		t.Skip("rclone is not installed")
	}
	destination := t.TempDir()
	lock := newRemoteLock(destination, 500*time.Millisecond, lockWait{}, false, false, log.New(io.Discard, "", 0), rcloneCommand{})
	if err := lock.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	initial, err := os.ReadFile(filepath.Join(destination, ".rcw-lock"))
	if err != nil {
		t.Fatal(err)
	}
	errors := lock.Start()

	deadline := time.Now().Add(3 * time.Second)
	for {
		current, err := os.ReadFile(filepath.Join(destination, ".rcw-lock"))
		if err == nil && string(current) != string(initial) {
			break
		}
		select {
		case err := <-errors:
			t.Fatalf("refresh failed: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("lock timestamp was not refreshed")
		}
		time.Sleep(20 * time.Millisecond)
	}

	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(destination, ".rcw-lock")); !os.IsNotExist(err) {
		t.Fatalf("lock file remains after Close: %v", err)
	}
}

func TestRemoteLockWaitsThroughRefresh(t *testing.T) {
	runner := &memoryLockRunner{value: time.Now().UTC().Format(time.RFC3339Nano)}
	go func() {
		time.Sleep(50 * time.Millisecond)
		runner.set(time.Now().UTC().Format(time.RFC3339Nano))
	}()

	lock := newRemoteLock("remote:destination", 200*time.Millisecond, lockWait{duration: 500 * time.Millisecond}, false, false, log.New(io.Discard, "", 0), runner)
	if err := lock.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	lock.Start()
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteLockExplicitZeroWaitIsImmediate(t *testing.T) {
	runner := &memoryLockRunner{value: time.Now().UTC().Format(time.RFC3339Nano)}
	lock := newRemoteLock("remote:destination", time.Hour, lockWait{explicit: true}, false, false, log.New(io.Discard, "", 0), runner)
	started := time.Now()
	err := lock.Acquire(make(chan os.Signal))
	if err == nil || !strings.Contains(err.Error(), "remote lock is active") {
		t.Fatalf("Acquire error = %v, want active-lock error", err)
	}
	if time.Since(started) > 100*time.Millisecond {
		t.Fatal("explicit zero lock wait did not return immediately")
	}
}

func TestRemoteLockDefaultWaitsForExpiry(t *testing.T) {
	runner := &memoryLockRunner{value: time.Now().UTC().Format(time.RFC3339Nano)}
	lock := newRemoteLock("remote:destination", 60*time.Millisecond, lockWait{}, false, false, log.New(io.Discard, "", 0), runner)
	lock.pollInterval = 10 * time.Millisecond
	started := time.Now()
	if err := lock.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) < 40*time.Millisecond {
		t.Fatal("default lock wait acquired before the observed lock expired")
	}
	lock.Start()
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteLockDefaultRejectsRefresh(t *testing.T) {
	runner := &memoryLockRunner{value: time.Now().UTC().Format(time.RFC3339Nano)}
	lock := newRemoteLock("remote:destination", 200*time.Millisecond, lockWait{}, false, false, log.New(io.Discard, "", 0), runner)
	lock.pollInterval = 10 * time.Millisecond
	go func() {
		time.Sleep(30 * time.Millisecond)
		runner.set(time.Now().UTC().Format(time.RFC3339Nano))
	}()
	err := lock.Acquire(make(chan os.Signal))
	if err == nil || !strings.Contains(err.Error(), "refreshed while waiting") {
		t.Fatalf("Acquire error = %v, want refreshed-lock error", err)
	}
}

func TestRemoteLockWaitTimesOut(t *testing.T) {
	runner := &memoryLockRunner{value: time.Now().UTC().Format(time.RFC3339Nano)}
	lock := newRemoteLock("remote:destination", time.Hour, lockWait{duration: 50 * time.Millisecond}, false, false, log.New(io.Discard, "", 0), runner)
	err := lock.Acquire(make(chan os.Signal))
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("Acquire error = %v, want timeout error", err)
	}
}

func TestRemoteLockWaitDetectsEarlyRelease(t *testing.T) {
	runner := &memoryLockRunner{value: time.Now().UTC().Format(time.RFC3339Nano)}
	lock := newRemoteLock("remote:destination", time.Hour, lockWait{duration: time.Second}, false, false, log.New(io.Discard, "", 0), runner)
	lock.pollInterval = 20 * time.Millisecond
	go func() {
		time.Sleep(30 * time.Millisecond)
		runner.set("")
	}()
	if err := lock.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	lock.Start()
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteLockUsesConditionalWrite(t *testing.T) {
	runner := &memoryLockRunner{value: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)}
	lock := newRemoteLock("remote:destination", time.Second, lockWait{}, true, false, log.New(io.Discard, "", 0), runner)
	if err := lock.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	args := runner.copyArgs()
	if len(args) < 2 || args[len(args)-2] != "--header-upload" || !strings.HasPrefix(args[len(args)-1], "If-Match:") {
		t.Fatalf("conditional copy args = %#v, want If-Match upload header", args)
	}
	lock.Start()
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteLockUsesConditionalCreate(t *testing.T) {
	runner := &memoryLockRunner{}
	lock := newRemoteLock("remote:destination", time.Second, lockWait{}, true, false, log.New(io.Discard, "", 0), runner)
	if err := lock.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	args := runner.copyArgs()
	if len(args) < 2 || args[len(args)-2] != "--header-upload" || args[len(args)-1] != "If-None-Match: *" {
		t.Fatalf("conditional copy args = %#v, want If-None-Match upload header", args)
	}
	lock.Start()
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteLockWaitsAfterConditionalRace(t *testing.T) {
	runner := &memoryLockRunner{conditionalFailures: 1}
	lock := newRemoteLock("remote:destination", 50*time.Millisecond, lockWait{duration: 500 * time.Millisecond}, true, false, log.New(io.Discard, "", 0), runner)
	lock.pollInterval = 10 * time.Millisecond
	if err := lock.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	lock.Start()
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestResponseETag(t *testing.T) {
	output := "2026/09/10 DEBUG : HTTP RESPONSE\nHTTP/1.1 200 OK\nEtag: \"opaque-etag\"\n"
	if got := responseETag(output); got != "opaque-etag" {
		t.Fatalf("responseETag = %q, want opaque-etag", got)
	}
}

func TestRemoteLockWaitCanBeInterrupted(t *testing.T) {
	runner := &memoryLockRunner{value: time.Now().UTC().Format(time.RFC3339Nano)}
	interrupt := make(chan os.Signal, 1)
	interrupt <- os.Interrupt
	lock := newRemoteLock("remote:destination", time.Hour, lockWait{infinite: true}, false, false, log.New(io.Discard, "", 0), runner)
	if err := lock.Acquire(interrupt); !errors.Is(err, errLockInterrupted) {
		t.Fatalf("Acquire error = %v, want errLockInterrupted", err)
	}
}

func TestRemoteLockOverwritesExpiredLock(t *testing.T) {
	if _, err := exec.LookPath("rclone"); err != nil {
		t.Skip("rclone is not installed")
	}
	destination := t.TempDir()
	old := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	if err := os.WriteFile(filepath.Join(destination, ".rcw-lock"), []byte(old+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	lock := newRemoteLock(destination, time.Second, lockWait{}, false, false, log.New(io.Discard, "", 0), rcloneCommand{})
	if err := lock.Acquire(make(chan os.Signal)); err != nil {
		t.Fatal(err)
	}
	lock.Start()
	if lock.getValue() == old {
		t.Fatal("expired timestamp was not replaced")
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
}
