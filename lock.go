package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

var errLockInterrupted = errors.New("interrupted while waiting for remote lock")

const lockPollInterval = time.Second

type remoteLock struct {
	path             string
	timeout          time.Duration
	wait             lockWait
	pollInterval     time.Duration
	consistentWrites bool
	logs             bool
	logger           *log.Logger
	runner           commandRunner

	mu         sync.Mutex
	value      string
	stop       chan struct{}
	done       chan struct{}
	errors     chan error
	startOnce  sync.Once
	closeOnce  sync.Once
	releaseErr error
}

type lockState struct {
	timestamp time.Time
	value     string
	etag      string
	exists    bool
}

func newRemoteLock(destination string, timeout time.Duration, wait lockWait, consistentWrites, logs bool, logger *log.Logger, runner commandRunner) *remoteLock {
	return &remoteLock{
		path:             remotePath(destination, ".rcw-lock"),
		timeout:          timeout,
		wait:             wait,
		pollInterval:     lockPollInterval,
		consistentWrites: consistentWrites,
		logs:             logs,
		logger:           logger,
		runner:           runner,
		stop:             make(chan struct{}),
		done:             make(chan struct{}),
		errors:           make(chan error, 1),
	}
}

func (l *remoteLock) Acquire(interrupt <-chan os.Signal) error {
	state, err := l.read()
	if err != nil {
		return err
	}
	var deadline time.Time
	if !l.wait.infinite && l.wait.duration > 0 {
		deadline = time.Now().Add(l.wait.duration)
	}
	defaultWait := !l.wait.explicit && !l.wait.infinite && l.wait.duration == 0
	canWait := defaultWait || l.wait.infinite || l.wait.duration > 0
	observedValue := ""
	for {
		if defaultWait && observedValue != "" && state.exists && state.value != observedValue {
			return errors.New("remote lock was refreshed while waiting")
		}
		for state.active(l.timeout) {
			if defaultWait && observedValue != "" && state.value != observedValue {
				return errors.New("remote lock was refreshed while waiting")
			}
			expires := state.timestamp.Add(l.timeout)
			if defaultWait && observedValue == "" {
				observedValue = state.value
			}
			if !canWait {
				return fmt.Errorf("remote lock is active until %s", expires.Format(time.RFC3339Nano))
			}
			if !deadline.IsZero() && !time.Now().Before(deadline) {
				return fmt.Errorf("timed out after %s waiting for remote lock", l.wait.duration)
			}
			wakeAt := time.Now().Add(l.pollInterval)
			if expires.Before(wakeAt) {
				wakeAt = expires
			}
			if !deadline.IsZero() && deadline.Before(wakeAt) {
				wakeAt = deadline
			}
			if l.logs {
				l.logger.Printf("remote lock is active until %s; checking again at %s", expires.Format(time.RFC3339Nano), wakeAt.Format(time.RFC3339Nano))
			}
			wait := time.Until(wakeAt)
			if wait < 0 {
				wait = 0
			}
			timer := time.NewTimer(wait)
			select {
			case <-interrupt:
				if !timer.Stop() {
					<-timer.C
				}
				return errLockInterrupted
			case <-timer.C:
			}
			state, err = l.read()
			if err != nil {
				return err
			}
		}
		if defaultWait && observedValue != "" && state.exists && state.value != observedValue {
			return errors.New("remote lock was refreshed while waiting")
		}

		value := time.Now().UTC().Format(time.RFC3339Nano)
		if err := l.write(value, state.etag); err != nil {
			if l.consistentWrites && canWait {
				latest, readErr := l.read()
				if readErr == nil && latest.active(l.timeout) {
					if defaultWait && observedValue != "" && latest.value != observedValue {
						return errors.New("remote lock was refreshed while waiting")
					}
					state = latest
					continue
				}
			}
			return err
		}
		current, err := l.read()
		if err != nil {
			return err
		}
		if !current.exists || current.value != value {
			if canWait && current.active(l.timeout) {
				if defaultWait && observedValue != "" && current.value != observedValue {
					return errors.New("remote lock was refreshed while waiting")
				}
				state = current
				continue
			}
			return errors.New("remote lock changed while acquiring it")
		}
		claimedAt, _ := time.Parse(time.RFC3339Nano, value)
		if !time.Now().Before(claimedAt.Add(l.timeout)) {
			l.setValue(value)
			_ = l.release()
			return errors.New("remote lock timeout elapsed during acquisition")
		}
		l.setValue(value)
		if l.logs {
			l.logger.Printf("remote lock acquired")
		}
		return nil
	}
}

func (s lockState) active(timeout time.Duration) bool {
	return s.exists && !time.Now().After(s.timestamp.Add(timeout))
}

func (l *remoteLock) Start() <-chan error {
	l.startOnce.Do(func() { go l.refreshLoop() })
	return l.errors
}

func (l *remoteLock) Close() error {
	l.closeOnce.Do(func() {
		close(l.stop)
		<-l.done
		l.releaseErr = l.release()
	})
	return l.releaseErr
}

func (l *remoteLock) refreshLoop() {
	defer close(l.done)
	defer close(l.errors)
	timer := time.NewTimer(l.refreshDelay())
	defer timer.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-timer.C:
			if err := l.refresh(); err != nil {
				l.errors <- err
				return
			}
			timer.Reset(l.refreshDelay())
		}
	}
}

func (l *remoteLock) refreshDelay() time.Duration {
	interval := l.timeout / 2
	if interval <= 0 {
		interval = time.Nanosecond
	}
	written, err := time.Parse(time.RFC3339Nano, l.getValue())
	if err != nil {
		return 0
	}
	delay := time.Until(written.Add(interval))
	if delay < 0 {
		return 0
	}
	return delay
}

func (l *remoteLock) refresh() error {
	current, err := l.read()
	if err != nil {
		return err
	}
	if !current.exists {
		return errors.New("remote lock disappeared")
	}
	if current.value != l.getValue() {
		return errors.New("remote lock ownership was lost")
	}

	value := time.Now().UTC().Format(time.RFC3339Nano)
	if err := l.write(value, current.etag); err != nil {
		return err
	}
	current, err = l.read()
	if err != nil {
		return err
	}
	if !current.exists || current.value != value {
		return errors.New("remote lock changed while refreshing it")
	}
	written, _ := time.Parse(time.RFC3339Nano, value)
	if !time.Now().Before(written.Add(l.timeout)) {
		return errors.New("remote lock timeout elapsed while refreshing it")
	}
	l.setValue(value)
	if l.logs {
		l.logger.Printf("remote lock refreshed")
	}
	return nil
}

func (l *remoteLock) release() error {
	current, err := l.read()
	if err != nil {
		return err
	}
	if !current.exists {
		return nil
	}
	if current.value != l.getValue() {
		return errors.New("remote lock ownership was lost; lock was not removed")
	}
	if err := l.run("deletefile", l.path); err != nil {
		if isRcloneNotFound(err) {
			return nil
		}
		return err
	}
	if l.logs {
		l.logger.Printf("remote lock released")
	}
	return nil
}

func (l *remoteLock) read() (lockState, error) {
	var stdout, stderr bytes.Buffer
	args := []string{"cat", l.path}
	if l.consistentWrites {
		args = append(args, "--dump", "headers", "--log-level", "DEBUG")
	}
	err := l.runner.Run(args, &stdout, &stderr)
	if err != nil {
		if isRcloneNotFound(err) {
			return lockState{}, nil
		}
		return lockState{}, commandError(err, stderr.String())
	}
	value := strings.TrimSpace(stdout.String())
	timestamp, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return lockState{}, fmt.Errorf("invalid timestamp in remote lock %q: %w", value, err)
	}
	etag := ""
	if l.consistentWrites {
		etag = responseETag(stderr.String())
		if etag == "" {
			return lockState{}, errors.New("S3 response did not include an ETag; use --no-consistent-writes if this provider cannot supply one")
		}
	}
	return lockState{
		timestamp: timestamp,
		value:     value,
		etag:      etag,
		exists:    true,
	}, nil
}

func (l *remoteLock) write(value, previousETag string) error {
	file, err := os.CreateTemp("", "rclonewatch-lock-*")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if _, err := io.WriteString(file, value+"\n"); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	args := []string{"copyto", name, l.path}
	if l.consistentWrites {
		header := "If-None-Match: *"
		if previousETag != "" {
			header = fmt.Sprintf("If-Match: \"%s\"", previousETag)
		}
		args = append(args, "--header-upload", header)
	}
	return l.run(args...)
}

func responseETag(output string) string {
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if key, value, found := strings.Cut(line, ":"); found && strings.EqualFold(key, "etag") {
			return strings.Trim(strings.TrimSpace(value), `"`)
		}
	}
	return ""
}

func (l *remoteLock) run(args ...string) error {
	var stdout, stderr bytes.Buffer
	if err := l.runner.Run(args, &stdout, &stderr); err != nil {
		return commandError(err, stderr.String())
	}
	return nil
}

func (l *remoteLock) getValue() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.value
}

func (l *remoteLock) setValue(value string) {
	l.mu.Lock()
	l.value = value
	l.mu.Unlock()
}

func isRcloneNotFound(err error) bool {
	var exitError *exec.ExitError
	return errors.As(err, &exitError) && (exitError.ExitCode() == 3 || exitError.ExitCode() == 4)
}

func commandError(err error, output string) error {
	if message := strings.TrimSpace(output); message != "" {
		return fmt.Errorf("%w: %s", err, message)
	}
	return err
}
