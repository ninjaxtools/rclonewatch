package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const (
	initialRetryDelay = time.Second
	maxRetryDelay     = time.Minute
	defaultLockTTL    = 2 * time.Minute
)

type config struct {
	interval           time.Duration
	lockTimeout        time.Duration
	lockWait           lockWait
	persistentLock     string
	excludes           excludePatterns
	logs               bool
	uploadOnly         bool
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

type rcloneCommand struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func newRcloneCommand() *rcloneCommand {
	ctx, cancel := context.WithCancel(context.Background())
	return &rcloneCommand{ctx: ctx, cancel: cancel}
}

func (r *rcloneCommand) Cancel() {
	if r.cancel != nil {
		r.cancel()
	}
}

func (r *rcloneCommand) Run(args []string, stdout, stderr io.Writer) error {
	ctx := r.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	cmd := exec.CommandContext(ctx, "rclone", args...)
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
	for path := range batch {
		if s.state != nil {
			exact, _ := s.state.protects(path)
			if exact {
				continue
			}
		}
		paths = append(paths, path)
	}
	paths, err := s.reconciliationScopes(paths)
	if err != nil {
		return err
	}
	if len(paths) == 0 {
		return nil
	}

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

	if paths[0] == "." {
		if err := s.fullSync(); err != nil {
			return err
		}
	} else if err := s.syncScopes(paths); err != nil {
		return err
	}
	if err := complete(); err != nil {
		return err
	}

	if s.logs {
		s.logger.Printf("sync completed: %d changed path(s)", len(paths))
	}
	return nil
}

func (s *syncer) fullSync() error {
	var filters []string
	if s.state != nil {
		filters = s.state.payloadFilters()
	}
	if err := s.run(payloadSyncArgs(s.source, s.dest, filters, s.excludes)...); err != nil {
		return fmt.Errorf("full sync: %w", err)
	}
	return nil
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

type syncResult struct {
	batch map[string]change
	err   error
}

func addPending(pending map[string]change, item change) {
	pending[item.path] = item
}

func mergeFailedBatch(pending, failed map[string]change) {
	for path, item := range failed {
		if _, exists := pending[path]; !exists {
			pending[path] = item
		}
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

func signalCommandGroup(cmd *exec.Cmd, signal os.Signal) error {
	sig, ok := signal.(syscall.Signal)
	if !ok {
		return cmd.Process.Signal(signal)
	}
	return syscall.Kill(-cmd.Process.Pid, sig)
}

// Drain events while startup reconciliation runs, so large downloads cannot
// block the watcher and writes made during the startup scan remain pending.
func initializeWatching(state *syncState, watcher *watcher, pending map[string]change) error {
	done := make(chan error, 1)
	go func() { done <- state.InitializeGeneration() }()
	eventC, errorC := watcher.Events(), watcher.Errors()
	var watcherErr error
	for {
		select {
		case item, ok := <-eventC:
			if !ok {
				eventC = nil
				continue
			}
			if exact, _ := state.protects(item.path); !exact {
				addPending(pending, item)
			}
		case err, ok := <-errorC:
			if !ok {
				errorC = nil
				continue
			}
			if err != nil && watcherErr == nil {
				watcherErr = err
				if state.cancelCommands != nil {
					state.cancelCommands()
				}
			}
		case err := <-done:
			if watcherErr != nil {
				return fmt.Errorf("watch during initialization: %w", watcherErr)
			}
			return err
		}
	}
}

func run(cfg config) (exitCode int) {
	logger := log.New(io.Discard, "rclonewatch: ", log.LstdFlags)
	if cfg.logs {
		logger.SetOutput(os.Stdout)
	}
	runner := newRcloneCommand()
	defer runner.Cancel()

	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	var state *syncState
	var lockErrors <-chan error
	if !cfg.uploadOnly {
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
		state.cancelCommands = runner.Cancel
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

	watcher, err := newWatcher(cfg.source)
	if err != nil {
		logger.Printf("%v", err)
		return 1
	}
	defer func() {
		watcher.Close()
		for range watcher.Events() {
		}
	}()
	pending := make(map[string]change)
	if state != nil {
		if err := initializeWatching(state, watcher, pending); err != nil {
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
		wrapped.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
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
	abortForLockFailure := func(err error) {
		if lockErr != nil {
			return
		}
		lockErr = err
		runner.Cancel()
		stopping = true
		timerC = nil
		syncReady = false
		if commandDone != nil {
			_ = signalCommandGroup(wrapped, syscall.SIGKILL)
		}
		watcher.Close()
	}

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
		if stopping && watcherClosed && errorC == nil && !active && commandDone == nil {
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
				if state != nil {
					if err := state.FinishSession(); err != nil {
						logger.Printf("finish local session: %v", err)
						return 1
					}
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
					_ = signalCommandGroup(wrapped, syscall.SIGTERM)
				}
				watcher.Close()
			}
		case err, ok := <-lockErrors:
			if !ok {
				lockErrors = nil
				continue
			}
			if err != nil && lockErr == nil {
				abortForLockFailure(err)
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
				if errors.Is(result.err, errLockOwnershipLost) {
					abortForLockFailure(result.err)
					continue
				}
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
				_ = signalCommandGroup(wrapped, sig)
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
	flags.DurationVar(&cfg.lockTimeout, "lock-ttl", defaultLockTTL, "remote lock expiry timeout")
	flags.Var(&cfg.lockWait, "lock-wait", "maximum time to wait for an active lock: 0, a duration using s/m/h, or inf")
	flags.StringVar(&cfg.persistentLock, "persistent-lock", "", "continue and retain the lock under this ID")
	flags.Var(&cfg.excludes, "exclude", "exclude files matching this rclone glob (repeatable)")
	flags.BoolVar(&cfg.logs, "logs", false, "log sync activity and rclone output to stdout")
	flags.BoolVar(&cfg.uploadOnly, "upload-only", false, "only upload local changes without using sync state or a remote lock")
	flags.BoolVar(&cfg.forceDeleteRemote, "force-delete-untracked-remote", false, "delete and initialize a non-empty destination without a state file")
	flags.BoolVar(&cfg.failOnIncomplete, "fail-on-incomplete-sync", false, "exit if state records an incomplete upload or interrupted local session")
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
	lockTTLSet := false
	lockWaitSet := false
	persistentLockSet := false
	stateFileSet := false
	stateFileLocalSet := false
	stateFileRemoteSet := false
	flags.Visit(func(item *flag.Flag) {
		if item.Name == "lock-ttl" {
			lockTTLSet = true
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
	if cfg.lockTimeout <= 0 {
		return config{}, errors.New("--lock-ttl must be greater than zero")
	}
	if persistentLockSet && cfg.persistentLock == "" {
		return config{}, errors.New("--persistent-lock must not be empty")
	}
	if stateFileSet && (stateFileLocalSet || stateFileRemoteSet) {
		return config{}, errors.New("--state-file cannot be combined with --state-file-local or --state-file-remote")
	}
	if stateFileLocalSet != stateFileRemoteSet {
		return config{}, errors.New("--state-file-local and --state-file-remote must be specified together")
	}
	if cfg.uploadOnly {
		switch {
		case lockTTLSet:
			return config{}, errors.New("--lock-ttl cannot be used with --upload-only")
		case lockWaitSet:
			return config{}, errors.New("--lock-wait cannot be used with --upload-only")
		case persistentLockSet:
			return config{}, errors.New("--persistent-lock cannot be used with --upload-only")
		case cfg.forceDeleteRemote:
			return config{}, errors.New("--force-delete-untracked-remote cannot be used with --upload-only")
		case cfg.noConsistentWrites:
			return config{}, errors.New("--no-consistent-writes cannot be used with --upload-only")
		case cfg.failOnIncomplete:
			return config{}, errors.New("--fail-on-incomplete-sync cannot be used with --upload-only")
		case stateFileSet || stateFileLocalSet:
			return config{}, errors.New("state file path options cannot be used with --upload-only")
		}
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
	source, err = filepath.EvalSymlinks(source)
	if err != nil {
		return config{}, fmt.Errorf("resolve source folder: %w", err)
	}
	cfg.source = source
	cfg.dest = flags.Arg(1)
	if !cfg.uploadOnly {
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
