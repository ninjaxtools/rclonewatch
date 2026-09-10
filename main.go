package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

const (
	initialRetryDelay = time.Second
	maxRetryDelay     = time.Minute
)

type config struct {
	interval           time.Duration
	lockTimeout        time.Duration
	lockWait           lockWait
	persistentLock     string
	excludes           excludePatterns
	logs               bool
	forceDeleteRemote  bool
	failOnIncomplete   bool
	noConsistentWrites bool
	stateFile          string
	stateFileLocal     string
	stateFileRemote    string
	syncPaths          syncFilePaths
	source             string
	dest               string
	command            []string
}

type commandRunner interface {
	Run(args []string, stdout, stderr io.Writer) error
}

type rcloneCommand struct{}

func (rcloneCommand) Run(args []string, stdout, stderr io.Writer) error {
	cmd := exec.Command("rclone", args...)
	if stdout != nil || stderr != nil {
		cmd.Stdout = stdout
		cmd.Stderr = stderr
		return cmd.Run()
	}

	var captured bytes.Buffer
	cmd.Stdout = &captured
	cmd.Stderr = &captured
	if err := cmd.Run(); err != nil {
		message := strings.TrimSpace(captured.String())
		if message != "" {
			return fmt.Errorf("%w: %s", err, message)
		}
		return err
	}
	return nil
}

type syncer struct {
	source     string
	dest       string
	logs       bool
	state      *syncState
	trackState bool
	logger     *log.Logger
	runner     commandRunner
	excludes   []string
}

func (s *syncer) Sync(batch map[string]change) error {
	if len(batch) == 0 {
		return nil
	}

	paths := make([]string, 0, len(batch))
	full := false
	for path := range batch {
		if s.state != nil {
			exact, ancestor := s.state.protects(path)
			if exact {
				continue
			}
			full = full || ancestor
		}
		paths = append(paths, path)
	}
	if len(paths) == 0 {
		return nil
	}
	sort.Strings(paths)

	if s.logs {
		s.logger.Printf("syncing %d changed path(s)", len(paths))
		for _, path := range paths {
			s.logger.Printf("sync %q", path)
		}
	}
	if s.trackState {
		if err := s.state.BeforeRemoteChange(); err != nil {
			return fmt.Errorf("advance generation: %w", err)
		}
	}
	complete := func() error {
		if !s.trackState {
			return nil
		}
		return s.state.AfterRemoteChange()
	}

	_, requestedFull := batch["."]
	if full || requestedFull || len(s.excludes) > 0 {
		args := []string{"sync", s.source, s.dest, "--create-empty-src-dirs"}
		if s.state != nil {
			for _, filter := range s.state.payloadFilters() {
				args = append(args, "--exclude", "/"+filter)
			}
		}
		for _, pattern := range s.excludes {
			args = append(args, "--exclude", pattern)
		}
		if err := s.run(args...); err != nil {
			return fmt.Errorf("full sync: %w", err)
		}
		if err := complete(); err != nil {
			return err
		}
		if s.logs {
			s.logger.Printf("sync completed: %d changed path(s)", len(paths))
		}
		return nil
	}

	var existingFiles, existingDirs, deletedFiles, deletedDirs []string
	for _, path := range paths {
		item := batch[path]
		info, err := os.Lstat(filepath.Join(s.source, filepath.FromSlash(path)))
		switch {
		case err == nil && info.IsDir():
			existingDirs = append(existingDirs, path)
		case err == nil:
			existingFiles = append(existingFiles, path)
		case errors.Is(err, os.ErrNotExist) && item.isDir:
			deletedDirs = append(deletedDirs, path)
		case errors.Is(err, os.ErrNotExist):
			deletedFiles = append(deletedFiles, path)
		default:
			return fmt.Errorf("inspect %q: %w", path, err)
		}
	}

	if len(existingFiles) > 0 {
		if err := s.runWithList(existingFiles, "sync", s.source, s.dest, "--no-traverse", "--create-empty-src-dirs"); err != nil {
			return fmt.Errorf("sync changed files: %w", err)
		}
	}
	for _, path := range minimalDirs(existingDirs) {
		if err := s.run("mkdir", remotePath(s.dest, path)); err != nil {
			return fmt.Errorf("create directory %q: %w", path, err)
		}
	}
	if len(deletedFiles) > 0 {
		if err := s.runWithList(deletedFiles, "delete", s.dest); err != nil {
			return fmt.Errorf("delete changed files: %w", err)
		}
	}
	for _, path := range topLevelDirs(deletedDirs) {
		if err := s.run("purge", remotePath(s.dest, path)); err != nil {
			return fmt.Errorf("delete directory %q: %w", path, err)
		}
	}
	if err := complete(); err != nil {
		return err
	}

	if s.logs {
		s.logger.Printf("sync completed: %d changed path(s)", len(paths))
	}
	return nil
}

func (s *syncer) runWithList(paths []string, args ...string) error {
	file, err := os.CreateTemp("", "rclonewatch-*")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)

	for _, path := range paths {
		if _, err := file.WriteString(path + "\x00"); err != nil {
			file.Close()
			return err
		}
	}
	if err := file.Close(); err != nil {
		return err
	}

	args = append(args, "--files-from0", name)
	return s.run(args...)
}

func (s *syncer) run(args ...string) error {
	var output io.Writer
	if s.logs {
		output = os.Stdout
	}
	return s.runner.Run(args, output, output)
}

func remotePath(root, relative string) string {
	if strings.HasSuffix(root, ":") {
		return root + relative
	}
	return strings.TrimRight(root, "/") + "/" + relative
}

func minimalDirs(paths []string) []string {
	// Creating a leaf directory also creates its missing parents.
	sort.Slice(paths, func(i, j int) bool {
		return len(paths[i]) > len(paths[j])
	})
	result := make([]string, 0, len(paths))
	for _, path := range paths {
		covered := false
		for _, existing := range result {
			if strings.HasPrefix(existing, path+"/") {
				covered = true
				break
			}
		}
		if !covered {
			result = append(result, path)
		}
	}
	return result
}

func topLevelDirs(paths []string) []string {
	sort.Slice(paths, func(i, j int) bool {
		return len(paths[i]) < len(paths[j])
	})
	result := make([]string, 0, len(paths))
	for _, path := range paths {
		covered := false
		for _, existing := range result {
			if strings.HasPrefix(path, existing+"/") {
				covered = true
				break
			}
		}
		if !covered {
			result = append(result, path)
		}
	}
	return result
}

type syncResult struct {
	batch map[string]change
	err   error
}

func addPending(pending map[string]change, item change) {
	previous, exists := pending[item.path]
	if exists && previous.removed && !item.removed && previous.isDir != item.isDir {
		// Filtered rclone operations cannot safely replace a file with a
		// directory (or the reverse), so reconcile the complete tree.
		pending["."] = change{path: ".", isDir: true}
	}
	pending[item.path] = item
}

func mergeFailedBatch(pending, failed map[string]change) {
	for path, item := range failed {
		newer, exists := pending[path]
		if exists {
			if item.removed && !newer.removed && item.isDir != newer.isDir {
				pending["."] = change{path: ".", isDir: true}
			}
			continue
		}
		pending[path] = item
	}
}

func commandExitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) {
		return 1
	}
	if code := exitError.ExitCode(); code >= 0 {
		return code
	}
	if status, ok := exitError.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		return 128 + int(status.Signal())
	}
	return 1
}

func run(cfg config) (exitCode int) {
	logger := log.New(io.Discard, "rclonewatch: ", log.LstdFlags)
	if cfg.logs {
		logger.SetOutput(os.Stdout)
	}
	runner := rcloneCommand{}

	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	var state *syncState
	var lockErrors <-chan error
	if cfg.lockTimeout > 0 {
		consistentWrites, err := useConsistentWrites(cfg.dest, cfg.noConsistentWrites, runner)
		if err != nil {
			logger.Printf("configure lock writes: %v", err)
			return 1
		}
		state, err = newSyncState(cfg.syncPaths, cfg.source, cfg.dest, cfg.lockTimeout, cfg.lockWait, consistentWrites, cfg.failOnIncomplete, cfg.logs, logger, runner, cfg.persistentLock)
		if err != nil {
			logger.Printf("initialize sync state: %v", err)
			return 1
		}
		state.excludes = append([]string(nil), cfg.excludes...)
		state.forceDeleteUntrackedRemote = cfg.forceDeleteRemote
		if err := state.Acquire(signals); err != nil {
			if errors.Is(err, errLockInterrupted) {
				return 0
			}
			logger.Printf("acquire lock: %v", err)
			return 1
		}
		lockErrors = state.Start()
		defer func() {
			if err := state.Close(); err != nil {
				logger.Printf("release lock: %v", err)
				exitCode = 1
			}
		}()
	}

	if state != nil {
		if err := state.InitializeGeneration(); err != nil {
			logger.Printf("initialize generation: %v", err)
			return 1
		}
	}
	if state != nil {
		select {
		case err := <-lockErrors:
			if err != nil {
				logger.Printf("lock refresh failed during startup sync: %v", err)
				return 1
			}
		default:
		}
	}

	watcher, err := newWatcher(cfg.source)
	if err != nil {
		logger.Printf("%v", err)
		return 1
	}

	s := &syncer{
		source:     cfg.source,
		dest:       cfg.dest,
		logs:       cfg.logs,
		state:      state,
		trackState: state != nil,
		logger:     logger,
		runner:     runner,
		excludes:   append([]string(nil), cfg.excludes...),
	}

	var wrapped *exec.Cmd
	var commandDone <-chan error
	if len(cfg.command) > 0 {
		wrapped = exec.Command(cfg.command[0], cfg.command[1:]...)
		wrapped.Stdin = os.Stdin
		wrapped.Stdout = os.Stdout
		wrapped.Stderr = os.Stderr
		if err := wrapped.Start(); err != nil {
			watcher.Close()
			logger.Printf("start wrapped command: %v", err)
			return 1
		}
		done := make(chan error, 1)
		commandDone = done
		go func() { done <- wrapped.Wait() }()
	}

	if cfg.logs {
		logger.Printf("watching %q for changes", cfg.source)
	}

	pending := make(map[string]change)
	results := make(chan syncResult, 1)
	var timer *time.Timer
	var timerC <-chan time.Time
	if cfg.interval > 0 {
		timer = time.NewTimer(cfg.interval)
		timerC = timer.C
		defer timer.Stop()
	}

	var active, stopping, watcherClosed, finalAttempted, syncReady bool
	var watcherErr, lockErr, lastSyncErr error
	wrappedExitCode := 0
	retryDelay := initialRetryDelay
	eventC := watcher.Events()
	errorC := watcher.Errors()

	startSync := func(final bool) {
		if active || len(pending) == 0 {
			return
		}
		batch := pending
		pending = make(map[string]change)
		active = true
		if final {
			finalAttempted = true
		}
		go func() {
			results <- syncResult{batch: batch, err: s.Sync(batch)}
		}()
	}

	for {
		if stopping && watcherClosed && !active && commandDone == nil {
			if lockErr != nil {
				logger.Printf("lock refresh failed: %v", lockErr)
				return 1
			}
			if len(pending) > 0 && !finalAttempted {
				startSync(true)
			} else {
				if watcherErr != nil {
					logger.Printf("watcher failed: %v", watcherErr)
					return 1
				}
				if len(pending) > 0 {
					logger.Printf("final sync failed: %v", lastSyncErr)
					return 1
				}
				return wrappedExitCode
			}
		}

		select {
		case item, ok := <-eventC:
			if !ok {
				watcherClosed = true
				eventC = nil
				continue
			}
			if state != nil {
				exact, _ := state.protects(item.path)
				if exact {
					continue
				}
			}
			addPending(pending, item)
			if syncReady && !active && !stopping {
				syncReady = false
				startSync(false)
			}
		case err, ok := <-errorC:
			if !ok {
				errorC = nil
				continue
			}
			if err != nil && watcherErr == nil {
				watcherErr = err
				stopping = true
				timerC = nil
				if commandDone != nil {
					_ = wrapped.Process.Signal(syscall.SIGTERM)
				}
				watcher.Close()
			}
		case err, ok := <-lockErrors:
			if !ok {
				lockErrors = nil
				continue
			}
			if err != nil && lockErr == nil {
				lockErr = err
				stopping = true
				timerC = nil
				syncReady = false
				if commandDone != nil {
					_ = wrapped.Process.Signal(syscall.SIGTERM)
				}
				watcher.Close()
			}
		case <-timerC:
			timerC = nil
			if len(pending) > 0 {
				startSync(false)
			} else {
				syncReady = true
			}
		case result := <-results:
			active = false
			lastSyncErr = result.err
			if result.err != nil {
				// Events received during the attempt are newer and win.
				mergeFailedBatch(pending, result.batch)
				if cfg.logs {
					logger.Printf("sync failed: %v", result.err)
				}
				if !stopping {
					syncReady = false
					timer.Reset(retryDelay)
					timerC = timer.C
					retryDelay *= 2
					if retryDelay > maxRetryDelay {
						retryDelay = maxRetryDelay
					}
				}
			} else {
				retryDelay = initialRetryDelay
				if !stopping && timer != nil {
					syncReady = false
					timer.Reset(cfg.interval)
					timerC = timer.C
				}
			}
		case err := <-commandDone:
			commandDone = nil
			wrappedExitCode = commandExitCode(err)
			if cfg.logs {
				logger.Printf("wrapped command exited with status %d", wrappedExitCode)
			}
			if !stopping {
				stopping = true
				timerC = nil
				syncReady = false
			}
			watcher.Close()
		case sig := <-signals:
			if commandDone != nil {
				_ = wrapped.Process.Signal(sig)
			}
			if !stopping {
				stopping = true
				timerC = nil
				syncReady = false
				if cfg.logs {
					logger.Printf("shutdown requested; waiting for final sync")
				}
				if commandDone == nil {
					watcher.Close()
				}
			}
		}
	}
}

func parseConfig(args []string) (config, error) {
	var cfg config
	for index, arg := range args {
		if arg != "--" {
			continue
		}
		if index+1 == len(args) {
			return config{}, errors.New("expected a command after --")
		}
		cfg.command = append([]string(nil), args[index+1:]...)
		args = args[:index]
		break
	}
	flags := flag.NewFlagSet("rclonewatch", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.DurationVar(&cfg.interval, "interval", 0, "time to wait after a successful sync (for example 30s or 5m)")
	flags.DurationVar(&cfg.lockTimeout, "use-lock", 0, "coordinate using the remote state file with this expiry timeout")
	flags.Var(&cfg.lockWait, "lock-wait", "maximum time to wait for an active lock: 0, a duration using s/m/h, or inf")
	flags.StringVar(&cfg.persistentLock, "persistent-lock", "", "continue and retain the lock under this ID")
	flags.Var(&cfg.excludes, "exclude", "exclude files matching this rclone glob (repeatable)")
	flags.BoolVar(&cfg.logs, "logs", false, "log sync activity and rclone output to stdout")
	flags.BoolVar(&cfg.forceDeleteRemote, "force-delete-untracked-remote", false, "delete and initialize a non-empty destination without a state file")
	flags.BoolVar(&cfg.failOnIncomplete, "fail-on-incomplete-sync", false, "exit if the state file records an incomplete sync")
	flags.BoolVar(&cfg.noConsistentWrites, "no-consistent-writes", false, "disable conditional lock writes for S3-compatible destinations")
	flags.StringVar(&cfg.stateFile, "state-file", "", "state file path relative to both source and destination")
	flags.StringVar(&cfg.stateFileLocal, "state-file-local", "", "state file path relative to the local source")
	flags.StringVar(&cfg.stateFileRemote, "state-file-remote", "", "state file path relative to the remote destination")
	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	if cfg.interval < 0 {
		return config{}, errors.New("--interval must not be negative")
	}
	lockSet := false
	lockWaitSet := false
	persistentLockSet := false
	stateFileSet := false
	stateFileLocalSet := false
	stateFileRemoteSet := false
	flags.Visit(func(item *flag.Flag) {
		if item.Name == "use-lock" {
			lockSet = true
		}
		if item.Name == "lock-wait" {
			lockWaitSet = true
		}
		if item.Name == "persistent-lock" {
			persistentLockSet = true
		}
		switch item.Name {
		case "state-file":
			stateFileSet = true
		case "state-file-local":
			stateFileLocalSet = true
		case "state-file-remote":
			stateFileRemoteSet = true
		}
	})
	if lockSet && cfg.lockTimeout <= 0 {
		return config{}, errors.New("--use-lock must be greater than zero")
	}
	if cfg.forceDeleteRemote && !lockSet {
		return config{}, errors.New("--force-delete-untracked-remote requires --use-lock with a timeout")
	}
	if lockWaitSet && !lockSet {
		return config{}, errors.New("--lock-wait requires --use-lock with a timeout")
	}
	if persistentLockSet && !lockSet {
		return config{}, errors.New("--persistent-lock requires --use-lock with a timeout")
	}
	if persistentLockSet && cfg.persistentLock == "" {
		return config{}, errors.New("--persistent-lock must not be empty")
	}
	if cfg.noConsistentWrites && !lockSet {
		return config{}, errors.New("--no-consistent-writes requires --use-lock with a timeout")
	}
	if cfg.failOnIncomplete && !lockSet {
		return config{}, errors.New("--fail-on-incomplete-sync requires --use-lock with a timeout")
	}
	if stateFileSet && (stateFileLocalSet || stateFileRemoteSet) {
		return config{}, errors.New("--state-file cannot be combined with --state-file-local or --state-file-remote")
	}
	if stateFileLocalSet != stateFileRemoteSet {
		return config{}, errors.New("--state-file-local and --state-file-remote must be specified together")
	}
	if (stateFileSet || stateFileLocalSet) && !lockSet {
		return config{}, errors.New("state file path options require --use-lock with a timeout")
	}
	if flags.NArg() != 2 {
		return config{}, errors.New("expected a source folder and rclone destination")
	}

	source, err := filepath.Abs(flags.Arg(0))
	if err != nil {
		return config{}, fmt.Errorf("resolve source folder: %w", err)
	}
	info, err := os.Stat(source)
	if err != nil {
		return config{}, fmt.Errorf("source folder: %w", err)
	}
	if !info.IsDir() {
		return config{}, fmt.Errorf("source folder %q is not a directory", source)
	}
	cfg.source = source
	cfg.dest = flags.Arg(1)
	if lockSet {
		localRelative, remoteRelative := defaultStateFile, defaultStateFile
		if stateFileSet {
			localRelative, remoteRelative = cfg.stateFile, cfg.stateFile
		} else if stateFileLocalSet {
			localRelative, remoteRelative = cfg.stateFileLocal, cfg.stateFileRemote
		}
		cfg.syncPaths, err = resolveSyncFilePaths(cfg.source, cfg.dest, localRelative, remoteRelative)
		if err != nil {
			return config{}, fmt.Errorf("resolve state file paths: %w", err)
		}
	}
	return cfg, nil
}

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			printHelp(os.Stdout)
			return
		}
		printUsage(os.Stderr)
		fmt.Fprintf(os.Stderr, "rclonewatch: %v\n", err)
		fmt.Fprintln(os.Stderr, "Try 'rclonewatch --help' for more information.")
		os.Exit(2)
	}
	os.Exit(run(cfg))
}
