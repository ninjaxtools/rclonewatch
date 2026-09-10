package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

type recordedCommand struct {
	args []string
	list []string
}

type recordingRunner struct {
	commands []recordedCommand
}

func (r *recordingRunner) Run(args []string, _, _ io.Writer) error {
	record := recordedCommand{args: append([]string(nil), args...)}
	for index, arg := range args {
		if arg != "--files-from0" || index+1 >= len(args) {
			continue
		}
		contents, err := os.ReadFile(args[index+1])
		if err != nil {
			return err
		}
		for _, path := range strings.Split(strings.TrimSuffix(string(contents), "\x00"), "\x00") {
			record.list = append(record.list, path)
		}
	}
	r.commands = append(r.commands, record)
	return nil
}

func TestRunWithListSupportsNewlines(t *testing.T) {
	runner := &recordingRunner{}
	s := syncer{runner: runner}
	path := "directory/file\nwith-newline"
	if err := s.runWithList([]string{path}, "sync", "source", "destination"); err != nil {
		t.Fatal(err)
	}
	if got := runner.commands[0].list; !reflect.DeepEqual(got, []string{path}) {
		t.Fatalf("files-from0 list = %#v, want %#v", got, []string{path})
	}
}

func TestSyncerBuildsCommandsForChanges(t *testing.T) {
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "changed file"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(source, "empty", "child"), 0o700); err != nil {
		t.Fatal(err)
	}

	runner := &recordingRunner{}
	s := syncer{
		source: source,
		dest:   "remote:backup",
		logger: log.New(io.Discard, "", 0),
		runner: runner,
	}
	batch := map[string]change{
		"changed file":        {path: "changed file"},
		"empty":               {path: "empty", isDir: true},
		"empty/child":         {path: "empty/child", isDir: true},
		"deleted file":        {path: "deleted file"},
		"deleted directory":   {path: "deleted directory", isDir: true},
		"deleted directory/a": {path: "deleted directory/a", isDir: true},
	}

	if err := s.Sync(batch); err != nil {
		t.Fatal(err)
	}
	if len(runner.commands) != 4 {
		t.Fatalf("got %d commands, want 4: %#v", len(runner.commands), runner.commands)
	}
	if got, want := runner.commands[0].args[:3], []string{"sync", source, "remote:backup"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("sync command = %#v, want prefix %#v", got, want)
	}
	if got, want := runner.commands[0].list, []string{"changed file"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("sync list = %#v, want %#v", got, want)
	}
	if got, want := runner.commands[1].args, []string{"mkdir", "remote:backup/empty/child"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("mkdir command = %#v, want %#v", got, want)
	}
	if got, want := runner.commands[2].list, []string{"deleted file"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("delete list = %#v, want %#v", got, want)
	}
	if got, want := runner.commands[3].args, []string{"purge", "remote:backup/deleted directory"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("purge command = %#v, want %#v", got, want)
	}
}

func TestSyncerWithLocalRclone(t *testing.T) {
	if _, err := exec.LookPath("rclone"); err != nil {
		t.Skip("rclone is not installed")
	}
	source := t.TempDir()
	destination := t.TempDir()
	file := filepath.Join(source, "folder", "file.txt")
	if err := os.MkdirAll(filepath.Dir(file), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte("first"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(source, "empty"), 0o700); err != nil {
		t.Fatal(err)
	}

	s := syncer{
		source: source,
		dest:   destination,
		logger: log.New(io.Discard, "", 0),
		runner: rcloneCommand{},
	}
	if err := s.Sync(map[string]change{
		"folder/file.txt": {path: "folder/file.txt"},
		"empty":           {path: "empty", isDir: true},
	}); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(filepath.Join(destination, "folder", "file.txt"))
	if err != nil || string(contents) != "first" {
		t.Fatalf("synced contents = %q, err = %v", contents, err)
	}
	if info, err := os.Stat(filepath.Join(destination, "empty")); err != nil || !info.IsDir() {
		t.Fatalf("empty directory was not synced: %v", err)
	}

	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(source, "empty")); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(map[string]change{
		"folder/file.txt": {path: "folder/file.txt"},
		"empty":           {path: "empty", isDir: true},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(destination, "folder", "file.txt")); !os.IsNotExist(err) {
		t.Fatalf("deleted file remains at destination: %v", err)
	}
	if _, err := os.Stat(filepath.Join(destination, "empty")); !os.IsNotExist(err) {
		t.Fatalf("deleted directory remains at destination: %v", err)
	}
}

func TestSyncerWithLocalRcloneExcludes(t *testing.T) {
	if _, err := exec.LookPath("rclone"); err != nil {
		t.Skip("rclone is not installed")
	}
	source := t.TempDir()
	destination := t.TempDir()
	writeTestFile(t, filepath.Join(source, "included.txt"), "included")
	writeTestFile(t, filepath.Join(source, "source.tmp"), "excluded")
	writeTestFile(t, filepath.Join(destination, "remote.tmp"), "retained")
	s := syncer{
		source:   source,
		dest:     destination,
		excludes: []string{"*.tmp"},
		logger:   log.New(io.Discard, "", 0),
		runner:   rcloneCommand{},
	}
	if err := s.Sync(map[string]change{"included.txt": {path: "included.txt"}}); err != nil {
		t.Fatal(err)
	}
	if got := readTestFile(t, filepath.Join(destination, "included.txt")); got != "included" {
		t.Fatalf("included contents = %q", got)
	}
	if _, err := os.Stat(filepath.Join(destination, "source.tmp")); !os.IsNotExist(err) {
		t.Fatalf("excluded source file was synced: %v", err)
	}
	if got := readTestFile(t, filepath.Join(destination, "remote.tmp")); got != "retained" {
		t.Fatalf("excluded remote contents = %q, want retained", got)
	}
}

func TestRemotePath(t *testing.T) {
	tests := map[string]string{
		"remote:":       "remote:path/to/file",
		"remote:backup": "remote:backup/path/to/file",
		"/tmp/backup/":  "/tmp/backup/path/to/file",
	}
	for root, want := range tests {
		if got := remotePath(root, "path/to/file"); got != want {
			t.Errorf("remotePath(%q) = %q, want %q", root, got, want)
		}
	}
}

func TestAddPendingRequestsFullSyncForTypeChange(t *testing.T) {
	pending := make(map[string]change)
	addPending(pending, change{path: "item", removed: true})
	addPending(pending, change{path: "item", isDir: true})
	if _, exists := pending["."]; !exists {
		t.Fatal("file-to-directory replacement did not request a full sync")
	}
}

func TestFullSyncExcludesSyncFiles(t *testing.T) {
	runner := &recordingRunner{}
	s := syncer{
		source: t.TempDir(),
		dest:   "remote:backup",
		state: &syncState{paths: syncFilePaths{
			localFilter:  ".local/state.json",
			remoteFilter: ".remote/state.json",
		}},
		logger: log.New(io.Discard, "", 0),
		runner: runner,
	}
	if err := s.Sync(map[string]change{".": {path: ".", isDir: true}}); err != nil {
		t.Fatal(err)
	}
	args := runner.commands[0].args
	want := []string{"--exclude", "/.local/state.json", "--exclude", "/.remote/state.json"}
	if !reflect.DeepEqual(args[len(args)-4:], want) {
		t.Fatalf("full sync args = %#v, want sync-file exclusions %#v", args, want)
	}
}

func TestSyncFileAncestorForcesFilteredFullSync(t *testing.T) {
	runner := &recordingRunner{}
	s := syncer{
		source: t.TempDir(),
		dest:   "remote:backup",
		state:  &syncState{paths: syncFilePaths{localFilter: ".metadata/state.json"}},
		logger: log.New(io.Discard, "", 0),
		runner: runner,
	}
	if err := s.Sync(map[string]change{".metadata": {path: ".metadata", isDir: true, removed: true}}); err != nil {
		t.Fatal(err)
	}
	if got := runner.commands[0].args[0]; got != "sync" {
		t.Fatalf("command = %q, want filtered full sync", got)
	}
}

func TestSyncerAppliesExcludes(t *testing.T) {
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "changed.txt"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	runner := &recordingRunner{}
	s := syncer{
		source:   source,
		dest:     "remote:backup",
		excludes: []string{"*.tmp", "/cache/**"},
		logger:   log.New(io.Discard, "", 0),
		runner:   runner,
	}
	if err := s.Sync(map[string]change{"changed.txt": {path: "changed.txt"}}); err != nil {
		t.Fatal(err)
	}
	if len(runner.commands) != 1 {
		t.Fatalf("commands = %#v, want one filtered full sync", runner.commands)
	}
	args := runner.commands[0].args
	want := []string{"--exclude", "*.tmp", "--exclude", "/cache/**"}
	if !reflect.DeepEqual(args[len(args)-len(want):], want) {
		t.Fatalf("sync args = %#v, want suffix %#v", args, want)
	}
}

func TestFinalSyncOnSIGTERM(t *testing.T) {
	if _, err := exec.LookPath("rclone"); err != nil {
		t.Skip("rclone is not installed")
	}
	source := t.TempDir()
	destination := t.TempDir()
	process := startTestProcess(t, "--use-lock", "2s", "--sync-remote", "--logs", source, destination)
	assertSyncState(t, filepath.Join(destination, defaultSyncFile), 1, false, true)

	if err := os.WriteFile(filepath.Join(source, "final.txt"), []byte("final contents"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := process.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := process.waitForExit(t); err != nil {
		t.Fatalf("process exit: %v: %s", err, process.stderr.String())
	}
	contents, err := os.ReadFile(filepath.Join(destination, "final.txt"))
	if err != nil || string(contents) != "final contents" {
		t.Fatalf("final synced contents = %q, err = %v", contents, err)
	}
	assertSyncState(t, filepath.Join(source, defaultSyncFile), 2, false, false)
	assertSyncState(t, filepath.Join(destination, defaultSyncFile), 2, false, false)
}

func TestIntervalSync(t *testing.T) {
	if _, err := exec.LookPath("rclone"); err != nil {
		t.Skip("rclone is not installed")
	}
	source := t.TempDir()
	destination := t.TempDir()
	process := startTestProcess(t, "--interval", "50ms", "--use-lock", "2s", "--sync-remote", "--logs", source, destination)
	if err := os.WriteFile(filepath.Join(source, "interval.txt"), []byte("interval contents"), 0o600); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		contents, err := os.ReadFile(filepath.Join(destination, "interval.txt"))
		if err == nil && string(contents) == "interval contents" {
			break
		}
		if time.Now().After(deadline) {
			process.cmd.Process.Kill()
			t.Fatalf("interval sync did not complete: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	for {
		local, localExists, localErr := readLocalSyncFile(filepath.Join(source, defaultSyncFile))
		remote, remoteExists, remoteErr := readLocalSyncFile(filepath.Join(destination, defaultSyncFile))
		if localErr == nil && remoteErr == nil && localExists && remoteExists && local.Generation >= 2 && local.Generation == remote.Generation && !local.Syncing && !remote.Syncing && remote.Lock != nil {
			break
		}
		if time.Now().After(deadline) {
			process.cmd.Process.Kill()
			t.Fatalf("sync state did not settle: local=%#v exists=%v err=%v remote=%#v exists=%v err=%v", local, localExists, localErr, remote, remoteExists, remoteErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := process.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := process.waitForExit(t); err != nil {
		t.Fatalf("process exit: %v: %s", err, process.stderr.String())
	}
}

func TestFailedFinalSyncExitsOne(t *testing.T) {
	source := t.TempDir()
	destination := t.TempDir()
	t.Setenv("RCLONEWATCH_TEST_MISSING_RCLONE", "1")
	process := startTestProcess(t, "--logs", source, destination)
	if err := os.WriteFile(filepath.Join(source, "unsynced.txt"), []byte("contents"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := process.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	err := process.waitForExit(t)
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) || exitError.ExitCode() != 1 {
		t.Fatalf("process exit = %v, want status 1; stderr: %s", err, process.stderr.String())
	}
}

func TestLostLockExitsWithoutSyncing(t *testing.T) {
	if _, err := exec.LookPath("rclone"); err != nil {
		t.Skip("rclone is not installed")
	}
	source := t.TempDir()
	destination := t.TempDir()
	process := startTestProcess(t, "--use-lock", "400ms", "--logs", source, destination)
	if err := os.WriteFile(filepath.Join(source, "must-not-sync.txt"), []byte("contents"), 0o600); err != nil {
		t.Fatal(err)
	}
	foreign := syncFileData{Generation: 1, Lock: &syncFileLock{Owner: "foreign", Timestamp: time.Now().UTC()}}
	contents, err := encodeSyncFile(foreign)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(destination, defaultSyncFile), contents, 0o600); err != nil {
		t.Fatal(err)
	}

	err = process.waitForExit(t)
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) || exitError.ExitCode() != 1 {
		t.Fatalf("process exit = %v, want status 1; stderr: %s", err, process.stderr.String())
	}
	if _, err := os.Stat(filepath.Join(destination, "must-not-sync.txt")); !os.IsNotExist(err) {
		t.Fatalf("file was synced after lock ownership was lost: %v", err)
	}
	got, exists, err := readLocalSyncFile(filepath.Join(destination, defaultSyncFile))
	if err != nil || !exists || got.Lock == nil || got.Lock.Owner != "foreign" {
		t.Fatalf("foreign lock was changed or removed: state=%#v exists=%v err=%v", got, exists, err)
	}
}

func TestParseConfigRejectsNonPositiveLockTimeout(t *testing.T) {
	source := t.TempDir()
	for _, timeout := range []string{"0", "-1s"} {
		if _, err := parseConfig([]string{"--use-lock", timeout, source, "remote:destination"}); err == nil {
			t.Fatalf("--use-lock %s was accepted", timeout)
		}
	}
}

func TestParseConfigRequiresLockForRemoteSync(t *testing.T) {
	if _, err := parseConfig([]string{"--sync-remote", t.TempDir(), "remote:destination"}); err == nil {
		t.Fatal("--sync-remote without --use-lock was accepted")
	}
}

func TestParseConfigRequiresLockForLockOptions(t *testing.T) {
	source := t.TempDir()
	for _, args := range [][]string{
		{"--lock-wait", "1m", source, "remote:destination"},
		{"--persistent-lock", "sequence", source, "remote:destination"},
		{"--no-consistent-writes", source, "remote:destination"},
		{"--fail-on-incomplete-sync", source, "remote:destination"},
		{"--sync-file", "state.json", source, "remote:destination"},
	} {
		if _, err := parseConfig(args); err == nil {
			t.Fatalf("options %#v were accepted without --use-lock", args)
		}
	}
	cfg, err := parseConfig([]string{"--use-lock", "2m", "--lock-wait", "inf", source, "remote:destination"})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.lockWait.infinite {
		t.Fatal("--lock-wait inf was not retained")
	}
	if _, err := parseConfig([]string{"--use-lock", "2m", "--persistent-lock", "", source, "remote:destination"}); err == nil {
		t.Fatal("empty --persistent-lock was accepted")
	}
}

func TestParseConfigWrappedCommand(t *testing.T) {
	source := t.TempDir()
	cfg, err := parseConfig([]string{"--use-lock", "2m", "--persistent-lock", "sequence", source, "remote:destination", "--", "sh", "-c", "exit 7"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.persistentLock != "sequence" {
		t.Fatalf("persistent lock = %q, want sequence", cfg.persistentLock)
	}
	want := []string{"sh", "-c", "exit 7"}
	if !reflect.DeepEqual(cfg.command, want) {
		t.Fatalf("wrapped command = %#v, want %#v", cfg.command, want)
	}
	if _, err := parseConfig([]string{source, "remote:destination", "--"}); err == nil {
		t.Fatal("empty wrapped command was accepted")
	}
}

func TestParseConfigExcludes(t *testing.T) {
	source := t.TempDir()
	cfg, err := parseConfig([]string{"--exclude", "*.tmp", "--exclude", "/cache/**", source, "remote:destination"})
	if err != nil {
		t.Fatal(err)
	}
	want := excludePatterns{"*.tmp", "/cache/**"}
	if !reflect.DeepEqual(cfg.excludes, want) {
		t.Fatalf("excludes = %#v, want %#v", cfg.excludes, want)
	}
	if _, err := parseConfig([]string{"--exclude", "", source, "remote:destination"}); err == nil {
		t.Fatal("empty --exclude was accepted")
	}
}

func TestWrappedCommandExitStatus(t *testing.T) {
	source := t.TempDir()
	destination := t.TempDir()
	if got := run(config{source: source, dest: destination, command: []string{"sh", "-c", "exit 7"}}); got != 7 {
		t.Fatalf("run exit status = %d, want 7", got)
	}
}

func TestWrappedCommandFinalSync(t *testing.T) {
	if _, err := exec.LookPath("rclone"); err != nil {
		t.Skip("rclone is not installed")
	}
	source := t.TempDir()
	destination := t.TempDir()
	created := filepath.Join(source, "created.txt")
	command := []string{"sh", "-c", fmt.Sprintf("printf contents > %s", created)}
	if got := run(config{source: source, dest: destination, command: command}); got != 0 {
		t.Fatalf("run exit status = %d, want 0", got)
	}
	if got := readTestFile(t, filepath.Join(destination, "created.txt")); got != "contents" {
		t.Fatalf("synced command output = %q, want contents", got)
	}
}

func TestWrappedCommandReceivesSIGTERM(t *testing.T) {
	source := t.TempDir()
	destination := t.TempDir()
	markers := t.TempDir()
	ready := filepath.Join(markers, "ready")
	terminated := filepath.Join(markers, "terminated")
	script := fmt.Sprintf("trap 'touch %s; exit 0' TERM; touch %s; while :; do sleep 1; done", terminated, ready)
	process := startTestProcess(t, "--logs", source, destination, "--", "sh", "-c", script)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			process.cmd.Process.Kill()
			t.Fatal("wrapped command did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := process.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := process.waitForExit(t); err != nil {
		t.Fatalf("process exit: %v: %s", err, process.stderr.String())
	}
	if _, err := os.Stat(terminated); err != nil {
		t.Fatalf("wrapped command did not handle SIGTERM: %v", err)
	}
}

func TestParseConfigSyncFileOptions(t *testing.T) {
	source := t.TempDir()
	tests := [][]string{
		{"--use-lock", "1m", "--sync-file", "one", "--sync-file-local", "two", "--sync-file-remote", "three", source, "remote:destination"},
		{"--use-lock", "1m", "--sync-file-local", "two", source, "remote:destination"},
		{"--use-lock", "1m", "--sync-file-remote", "three", source, "remote:destination"},
		{"--use-lock", "1m", "--sync-remote", "--fail-on-incomplete-sync", "--sync-file", "/absolute", source, "remote:destination"},
	}
	for _, args := range tests {
		if _, err := parseConfig(args); err == nil {
			t.Fatalf("invalid options %#v were accepted", args)
		}
	}

	cfg, err := parseConfig([]string{"--use-lock", "1m", "--sync-remote", "--fail-on-incomplete-sync", "--sync-file-local", "../local-state", "--sync-file-remote", "../remote-state", source, "remote:destination/root"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.syncPaths.local != filepath.Join(filepath.Dir(source), "local-state") || cfg.syncPaths.localFilter != "" {
		t.Fatalf("local sync path = %#v", cfg.syncPaths)
	}
	if cfg.syncPaths.remote != "remote:destination/remote-state" || cfg.syncPaths.remoteFilter != "" {
		t.Fatalf("remote sync path = %#v", cfg.syncPaths)
	}
	if _, err := parseConfig([]string{"--use-lock", "1m", "--fail-on-incomplete-sync", source, "remote:destination"}); err != nil {
		t.Fatalf("--fail-on-incomplete-sync unexpectedly required --sync-remote: %v", err)
	}
}

type testProcess struct {
	cmd    *exec.Cmd
	wait   chan error
	stderr *bytes.Buffer
}

func startTestProcess(t *testing.T, args ...string) *testProcess {
	t.Helper()
	commandArgs := []string{"-test.run=^TestRclonewatchHelper$", "--"}
	commandArgs = append(commandArgs, args...)
	cmd := exec.Command(os.Args[0], commandArgs...)
	cmd.Env = append(os.Environ(), "RCLONEWATCH_TEST_HELPER=1")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	wait := make(chan error, 1)
	go func() { wait <- cmd.Wait() }()
	ready := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if strings.Contains(scanner.Text(), "watching") {
				select {
				case <-ready:
				default:
					close(ready)
				}
			}
		}
	}()

	select {
	case <-ready:
	case err := <-wait:
		t.Fatalf("process exited before watching: %v: %s", err, stderr.String())
	case <-time.After(5 * time.Second):
		cmd.Process.Kill()
		t.Fatal("timed out waiting for watcher startup")
	}
	return &testProcess{cmd: cmd, wait: wait, stderr: &stderr}
}

func (p *testProcess) waitForExit(t *testing.T) error {
	t.Helper()
	select {
	case err := <-p.wait:
		return err
	case <-time.After(10 * time.Second):
		p.cmd.Process.Kill()
		t.Fatal("timed out waiting for final sync")
	}
	return nil
}

func TestRclonewatchHelper(t *testing.T) {
	if os.Getenv("RCLONEWATCH_TEST_HELPER") != "1" {
		return
	}
	if os.Getenv("RCLONEWATCH_TEST_MISSING_RCLONE") == "1" {
		os.Setenv("PATH", "/nonexistent")
	}
	separator := -1
	for index, arg := range os.Args {
		if arg == "--" {
			separator = index
			break
		}
	}
	if separator == -1 {
		os.Exit(2)
	}
	cfg, err := parseConfig(os.Args[separator+1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(run(cfg))
}
