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
	// Buckets are provisioned separately. Apply this to every command so state
	// writes, payload uploads, upload-only mode, and lock release all agree.
	// Rclone ignores this S3-specific flag for other backends.
	commandArgs := append(append([]string(nil), args...), "--s3-no-check-bucket")
	cmd := exec.CommandContext(ctx, "rclone", commandArgs...)
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

	if paths[0] == "." {
		if err := s.fullSync(); err != nil {
			return err
		}
	} else if err := s.syncScopes(paths); err != nil {
		return err
	}
	if s.trackState {
		if err := s.state.AfterRemoteChange(); err != nil {
			return err
		}
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

type syncResult struct {
	batch map[string]change
	err   error
}

func addPending(pending map[string]change, item change) {
	pending[item.path] = item
}

// queueChange records a watcher event unless it concerns sync-state metadata,
// which is never part of the payload.
func queueChange(state *syncState, pending map[string]change, item change) {
	if state != nil {
		if exact, _ := state.protects(item.path); exact {
			return
		}
	}
	addPending(pending, item)
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

func signalExitCode(sig os.Signal) int {
	if number, ok := sig.(syscall.Signal); ok {
		return 128 + int(number)
	}
	return 1
}

// startExitCode follows the shell convention for a command that cannot run:
// 127 when it does not exist and 126 when it cannot be executed.
func startExitCode(err error) int {
	if errors.Is(err, exec.ErrNotFound) || errors.Is(err, os.ErrNotExist) {
		return 127
	}
	return 126
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
// A signal cancels the reconciliation, which is retried at the next start, and
// is reported as an interruptedError once the cancelled work has returned.
func initializeWatching(state *syncState, watcher *watcher, pending map[string]change, signals <-chan os.Signal) error {
	done := make(chan error, 1)
	go func() { done <- state.InitializeGeneration() }()
	eventC, errorC := watcher.Events(), watcher.Errors()
	var watcherErr, interrupted error
	cancel := func() {
		if state.cancelCommands != nil {
			state.cancelCommands()
		}
	}
	for {
		select {
		case item, ok := <-eventC:
			if !ok {
				eventC = nil
				continue
			}
			queueChange(state, pending, item)
		case err, ok := <-errorC:
			if !ok {
				errorC = nil
				continue
			}
			if err != nil && watcherErr == nil {
				watcherErr = err
				cancel()
			}
		case sig := <-signals:
			if interrupted == nil {
				interrupted = interruptedError{sig, "reconciling startup state"}
				cancel()
			}
		case err := <-done:
			if interrupted != nil {
				return interrupted
			}
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
		// Cancelling runner aborts in-flight work permanently; releasing a
		// lock that is still owned must remain possible afterwards.
		state.releaseRunner = newRcloneCommand()
		if err := state.Acquire(signals); err != nil {
			// An interrupted wait must not look like a successful run,
			// especially when the wrapped command never started.
			var interrupted interruptedError
			if errors.As(err, &interrupted) {
				logger.Printf("%v", err)
				return signalExitCode(interrupted.signal)
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
		if err := initializeWatching(state, watcher, pending, signals); err != nil {
			var interrupted interruptedError
			if errors.As(err, &interrupted) {
				logger.Printf("%v", err)
				return signalExitCode(interrupted.signal)
			}
			// A failed lock refresh cancels startup work; report the cause
			// rather than the resulting cancellation.
			select {
			case lockErr := <-lockErrors:
				if lockErr != nil {
					logger.Printf("lock refresh failed during startup sync: %v", lockErr)
					return 1
				}
			default:
			}
			logger.Printf("initialize generation: %v", err)
			return 1
		}
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

	var active, stopping, watcherClosed, finalAttempted, syncReady, abandoned bool
	var watcherErr, lockErr, lastSyncErr error
	wrappedExitCode := 0
	signalsSeen := 0

	var wrapped *exec.Cmd
	var commandDone <-chan int
	if len(cfg.command) > 0 {
		wrapped = exec.Command(cfg.command[0], cfg.command[1:]...)
		wrapped.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		tty, foreground := foregroundTerminal()
		if foreground {
			tty.configure(wrapped)
		}
		wrapped.Stdin = os.Stdin
		wrapped.Stdout = os.Stdout
		wrapped.Stderr = os.Stderr
		if err := wrapped.Start(); err != nil {
			// Shut down as if the command had exited: pending changes are
			// still synced and the session is finished rather than left
			// active, which would force a full recovery upload next time.
			logger.Printf("start wrapped command: %v", err)
			wrappedExitCode = startExitCode(err)
			stopping = true
			watcher.Close()
		} else {
			done := make(chan int, 1)
			commandDone = done
			if foreground {
				tty.started()
				go func() { done <- tty.wait(wrapped) }()
			} else {
				go func() { done <- commandExitCode(wrapped.Wait()) }()
			}
		}
	}

	if cfg.logs && !stopping {
		logger.Printf("watching %q for changes", cfg.source)
	}

	results := make(chan syncResult, 1)
	var timer *time.Timer
	var timerC <-chan time.Time
	if cfg.interval > 0 {
		timer = time.NewTimer(cfg.interval)
		defer timer.Stop()
		if !stopping {
			timerC = timer.C
		}
	}

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
			queueChange(state, pending, item)
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
			if !abandoned {
				lastSyncErr = result.err
			}
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
					if timer == nil {
						timer = time.NewTimer(retryDelay)
						defer timer.Stop()
					} else {
						timer.Reset(retryDelay)
					}
					timerC = timer.C
					retryDelay *= 2
					if retryDelay > maxRetryDelay {
						retryDelay = maxRetryDelay
					}
				}
			} else {
				retryDelay = initialRetryDelay
				if !stopping && cfg.interval > 0 {
					syncReady = false
					timer.Reset(cfg.interval)
					timerC = timer.C
				}
			}
		case code := <-commandDone:
			commandDone = nil
			wrappedExitCode = code
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
				// The command decides how to react; shutdown proceeds once it exits.
				_ = signalCommandGroup(wrapped, sig)
			} else {
				signalsSeen++
			}
			switch {
			case !stopping:
				stopping = true
				timerC = nil
				syncReady = false
				if cfg.logs {
					logger.Printf("shutdown requested; waiting for final sync")
				}
				if commandDone == nil {
					watcher.Close()
				}
			case commandDone != nil || abandoned:
			case signalsSeen < 2:
				if cfg.logs {
					logger.Printf("shutdown already in progress; send %v again to abandon the final sync", sig)
				}
			default:
				// Only the final sync remains. A repeated signal abandons it so
				// the process is never uninterruptible; the incomplete session
				// is recovered at the next start.
				abandoned = true
				finalAttempted = true
				lastSyncErr = fmt.Errorf("abandoned after repeated %v", sig)
				runner.Cancel()
				if cfg.logs {
					logger.Printf("%v received again; abandoning the final sync", sig)
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
	flags.BoolVar(&cfg.failOnIncomplete, "fail-on-incomplete-sync", false, "exit before reconciling an incomplete remote sync to local")
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
	set := make(map[string]bool)
	flags.Visit(func(item *flag.Flag) { set[item.Name] = true })
	lockTTLSet := set["lock-ttl"]
	lockWaitSet := set["lock-wait"]
	persistentLockSet := set["persistent-lock"]
	stateFileSet := set["state-file"]
	stateFileLocalSet := set["state-file-local"]
	stateFileRemoteSet := set["state-file-remote"]
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
