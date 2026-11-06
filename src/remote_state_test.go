package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

// S3 can report success with no output when cat targets a missing object.
type emptyRemoteStateRunner struct {
	*memorySyncRunner
	listing    string
	listingErr error
	listArgs   []string
}

func (r *emptyRemoteStateRunner) Run(args []string, stdout, stderr io.Writer) error {
	if args[0] == "cat" {
		r.mu.Lock()
		empty := r.value == ""
		r.mu.Unlock()
		if empty {
			return nil
		}
	}
	if args[0] == "lsjson" && slices.Contains(args, "--include") {
		r.listArgs = append([]string(nil), args...)
		if r.listingErr != nil {
			return r.listingErr
		}
		_, err := io.WriteString(stdout, r.listing)
		return err
	}
	return r.memorySyncRunner.Run(args, stdout, stderr)
}

func TestEmptyRemoteStateRead(t *testing.T) {
	for _, tc := range []struct {
		name, contents, listing, wantError string
		listingErr                         error
	}{
		{name: "missing", listing: "[]"},
		{name: "zero byte object", listing: `[{"Path":".rcw-state","IsDir":false}]`, wantError: "invalid remote state file: EOF"},
		{name: "directory conflict", listing: `[{"Path":".rcw-state","IsDir":true}]`, wantError: "invalid remote state file: EOF"},
		{name: "whitespace object", contents: " ", wantError: "invalid remote state file: EOF"},
		{name: "malformed state", contents: "{", wantError: "invalid remote state file"},
		{name: "listing denied", listingErr: errors.New("access denied"), wantError: "access denied"},
		{name: "invalid listing", listing: "{", wantError: "decode remote state listing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			memory := &memorySyncRunner{value: tc.contents}
			state, _ := newMemoryState(t, memory, time.Minute, lockWait{}, true, false)
			runner := &emptyRemoteStateRunner{memorySyncRunner: memory, listing: tc.listing, listingErr: tc.listingErr}
			state.runner = runner
			remote, err := state.readRemote()
			if tc.wantError == "" {
				if err != nil || remote.exists {
					t.Fatalf("missing state = %#v, %v", remote, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("got %v, want %s", err, tc.wantError)
			}
			if memory.attempts() != 0 {
				t.Fatal("reading remote state attempted a write")
			}
		})
	}
}

func TestAcquireMissingRemoteStateWithEmptyCat(t *testing.T) {
	for _, nonempty := range []bool{false, true} {
		memory := &memorySyncRunner{remoteFiles: nonempty}
		state, _ := newMemoryState(t, memory, time.Minute, lockWait{}, true, false)
		state.runner = &emptyRemoteStateRunner{memorySyncRunner: memory, listing: "[]"}
		err := state.Acquire(make(chan os.Signal))
		if nonempty {
			if err == nil || memory.attempts() != 0 {
				t.Fatalf("untracked nonempty destination was not protected: %v", err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if memory.data(t).Lock == nil {
			t.Fatal("new state has no lock")
		}
		if err := state.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRemoteStateListingPaths(t *testing.T) {
	for _, tc := range []struct{ remote, parent, name string }{
		{"remote:bucket/prefix/.rcw-state", "remote:bucket/prefix", ".rcw-state"},
		{"remote:.rcw-state", "remote:", ".rcw-state"},
		{":s3,endpoint='https://example.test/a:b':bucket/state[1]", ":s3,endpoint='https://example.test/a:b':bucket", `state\[1\]`},
		{"/tmp/state[1]", "/tmp", `state\[1\]`},
		{".rcw-state", ".", ".rcw-state"},
	} {
		memory := &memorySyncRunner{}
		state, _ := newMemoryState(t, memory, time.Minute, lockWait{}, false, false)
		runner := &emptyRemoteStateRunner{memorySyncRunner: memory, listing: "[]"}
		state.runner, state.paths.remote = runner, tc.remote
		if _, err := state.readRemote(); err != nil {
			t.Fatal(err)
		}
		want := []string{"lsjson", tc.parent, "--max-depth", "1", "--include", "/" + tc.name}
		if !reflect.DeepEqual(runner.listArgs, want) {
			t.Errorf("%s: got %v, want %v", tc.remote, runner.listArgs, want)
		}
	}
}

func TestEmptyRemoteStateWithLocalRclone(t *testing.T) {
	requireRclone(t)
	for _, name := range []string{defaultStateFile, "state[1]*?"} {
		t.Run(name, func(t *testing.T) {
			source, destination := t.TempDir(), t.TempDir()
			state := newLocalState(t, source, destination)
			state.paths.remote = filepath.Join(destination, name)
			remote, err := state.readRemote()
			if err != nil || remote.exists {
				t.Fatalf("missing state: %#v, %v", remote, err)
			}
			writeTestFile(t, state.paths.remote, "")
			if _, err := state.readRemote(); err == nil || !strings.Contains(err.Error(), "invalid remote state file: EOF") {
				t.Fatalf("accepted zero-byte state: %v", err)
			}
		})
	}
}
