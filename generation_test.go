package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

type generationOrderRunner struct {
	remoteGeneration string
	payloadRan       bool
}

func (r *generationOrderRunner) Run(args []string, stdout, _ io.Writer) error {
	switch args[0] {
	case "copyto":
		if filepath.Base(args[2]) != generationFileName {
			return fmt.Errorf("unexpected copy destination %q", args[2])
		}
		contents, err := os.ReadFile(args[1])
		if err != nil {
			return err
		}
		r.remoteGeneration = strings.TrimSpace(string(contents))
	case "cat":
		_, err := io.WriteString(stdout, r.remoteGeneration+"\n")
		return err
	default:
		if r.remoteGeneration != "2" {
			return fmt.Errorf("payload command ran at generation %q, want 2", r.remoteGeneration)
		}
		r.payloadRan = true
	}
	return nil
}

func TestGenerationInitializeAbsentSyncsRemoteAndCreatesVersionOne(t *testing.T) {
	requireRclone(t)
	source := t.TempDir()
	destination := t.TempDir()
	writeTestFile(t, filepath.Join(source, "local-only.txt"), "local")
	writeTestFile(t, filepath.Join(destination, "remote.txt"), "remote")

	generation := newGenerationManager(source, destination, false, log.New(io.Discard, "", 0), rcloneCommand{})
	if err := generation.Initialize(); err != nil {
		t.Fatal(err)
	}
	if got := readTestFile(t, filepath.Join(source, "remote.txt")); got != "remote" {
		t.Fatalf("remote content = %q, want remote", got)
	}
	if _, err := os.Stat(filepath.Join(source, "local-only.txt")); !os.IsNotExist(err) {
		t.Fatalf("local-only file remains after remote sync: %v", err)
	}
	assertGeneration(t, filepath.Join(source, generationFileName), 1)
	assertGeneration(t, filepath.Join(destination, generationFileName), 1)

	writeTestFile(t, filepath.Join(source, "changed.txt"), "changed")
	if err := generation.BeforeRemoteChange(); err != nil {
		t.Fatal(err)
	}
	assertGeneration(t, filepath.Join(source, generationFileName), 2)
	assertGeneration(t, filepath.Join(destination, generationFileName), 2)
}

func TestGenerationAdvancesBeforePayloadSync(t *testing.T) {
	source := t.TempDir()
	writeTestFile(t, filepath.Join(source, generationFileName), "1\n")
	writeTestFile(t, filepath.Join(source, "payload.txt"), "payload")
	runner := &generationOrderRunner{remoteGeneration: "1"}
	generation := &generationManager{
		localPath:  filepath.Join(source, generationFileName),
		remotePath: "remote:destination/.rcw-generation",
		current:    1,
		runner:     runner,
	}
	s := syncer{
		source:     source,
		dest:       "remote:destination",
		useLock:    true,
		generation: generation,
		logger:     log.New(io.Discard, "", 0),
		runner:     runner,
	}
	if err := s.Sync(map[string]change{"payload.txt": {path: "payload.txt"}}); err != nil {
		t.Fatal(err)
	}
	if !runner.payloadRan {
		t.Fatal("payload command was not run")
	}
	assertGeneration(t, filepath.Join(source, generationFileName), 2)
}

func TestGenerationInitializeRemoteAheadSyncsToLocal(t *testing.T) {
	requireRclone(t)
	source := t.TempDir()
	destination := t.TempDir()
	writeTestFile(t, filepath.Join(source, generationFileName), "1\n")
	writeTestFile(t, filepath.Join(source, "local-only.txt"), "local")
	writeTestFile(t, filepath.Join(destination, generationFileName), "2\n")
	writeTestFile(t, filepath.Join(destination, "remote.txt"), "remote")

	generation := newGenerationManager(source, destination, false, log.New(io.Discard, "", 0), rcloneCommand{})
	if err := generation.Initialize(); err != nil {
		t.Fatal(err)
	}
	assertGeneration(t, filepath.Join(source, generationFileName), 2)
	if got := readTestFile(t, filepath.Join(source, "remote.txt")); got != "remote" {
		t.Fatalf("remote content = %q, want remote", got)
	}
	if _, err := os.Stat(filepath.Join(source, "local-only.txt")); !os.IsNotExist(err) {
		t.Fatalf("local-only file remains after remote sync: %v", err)
	}
}

func TestGenerationInitializeRemoteOnlySyncsToLocal(t *testing.T) {
	requireRclone(t)
	source := t.TempDir()
	destination := t.TempDir()
	writeTestFile(t, filepath.Join(source, "local-only.txt"), "local")
	writeTestFile(t, filepath.Join(destination, generationFileName), "4\n")
	writeTestFile(t, filepath.Join(destination, "remote.txt"), "remote")

	generation := newGenerationManager(source, destination, false, log.New(io.Discard, "", 0), rcloneCommand{})
	if err := generation.Initialize(); err != nil {
		t.Fatal(err)
	}
	assertGeneration(t, filepath.Join(source, generationFileName), 4)
	if got := readTestFile(t, filepath.Join(source, "remote.txt")); got != "remote" {
		t.Fatalf("remote content = %q, want remote", got)
	}
	if _, err := os.Stat(filepath.Join(source, "local-only.txt")); !os.IsNotExist(err) {
		t.Fatalf("local-only file remains after remote sync: %v", err)
	}
}

func TestGenerationInitializeEqualDoesNotSync(t *testing.T) {
	requireRclone(t)
	source := t.TempDir()
	destination := t.TempDir()
	writeTestFile(t, filepath.Join(source, generationFileName), "3\n")
	writeTestFile(t, filepath.Join(source, "local-only.txt"), "local")
	writeTestFile(t, filepath.Join(destination, generationFileName), "3\n")
	writeTestFile(t, filepath.Join(destination, "remote-only.txt"), "remote")

	generation := newGenerationManager(source, destination, false, log.New(io.Discard, "", 0), rcloneCommand{})
	if err := generation.Initialize(); err != nil {
		t.Fatal(err)
	}
	if got := readTestFile(t, filepath.Join(source, "local-only.txt")); got != "local" {
		t.Fatalf("local content = %q, want local", got)
	}
	if _, err := os.Stat(filepath.Join(source, "remote-only.txt")); !os.IsNotExist(err) {
		t.Fatalf("equal generations unexpectedly triggered a sync: %v", err)
	}
}

func TestGenerationInitializeRejectsInvalidOrdering(t *testing.T) {
	requireRclone(t)
	tests := []struct {
		name        string
		local       string
		remote      string
		wantMessage string
	}{
		{name: "local only", local: "1\n", wantMessage: "remote generation is missing"},
		{name: "local ahead", local: "2\n", remote: "1\n", wantMessage: "ahead of remote"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := t.TempDir()
			destination := t.TempDir()
			writeTestFile(t, filepath.Join(source, generationFileName), test.local)
			if test.remote != "" {
				writeTestFile(t, filepath.Join(destination, generationFileName), test.remote)
			}
			generation := newGenerationManager(source, destination, false, log.New(io.Discard, "", 0), rcloneCommand{})
			err := generation.Initialize()
			if err == nil || !strings.Contains(err.Error(), test.wantMessage) {
				t.Fatalf("Initialize error = %v, want %q", err, test.wantMessage)
			}
		})
	}
}

func requireRclone(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("rclone"); err != nil {
		t.Skip("rclone is not installed")
	}
}

func writeTestFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}

func readTestFile(t *testing.T, path string) string {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(contents)
}

func assertGeneration(t *testing.T, path string, want uint64) {
	t.Helper()
	got, exists, err := readLocalGeneration(path)
	if err != nil {
		t.Fatal(err)
	}
	if !exists || got != want {
		t.Fatalf("generation at %q = %d (exists %v), want %d", path, got, exists, want)
	}
}
