//go:build linux

package main

import (
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"unsafe"
)

// The wrapped command runs in its own process group so a lock failure can
// terminate all of its descendants. A background process group that reads
// from, or changes the settings of, its controlling terminal is stopped with
// SIGTTIN/SIGTTOU, so when rclonewatch owns the foreground terminal it hands
// the terminal to the command and mirrors the command's job-control stops
// onto its own job, as a shell would.
type terminal struct {
	fd   int
	pgrp int
}

// foregroundTerminal reports stdin's terminal when this process group is in
// the foreground on it.
func foregroundTerminal() (*terminal, bool) {
	fd := int(os.Stdin.Fd())
	foreground, err := tcgetpgrp(fd)
	if err != nil || foreground != syscall.Getpgrp() {
		return nil, false
	}
	return &terminal{fd: fd, pgrp: syscall.Getpgrp()}, true
}

// configure makes cmd start as the terminal's foreground process group.
func (t *terminal) configure(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Foreground: true, Ctty: t.fd}
}

// started must be called once cmd has started. From then on rclonewatch may
// be a background process group, where changing the terminal's foreground
// group (or writing with tostop set) would otherwise stop it.
func (t *terminal) started() {
	signal.Ignore(syscall.SIGTTOU)
}

// wait reaps cmd and returns its exit status, restoring the terminal to
// rclonewatch afterwards. If the command is stopped (for example by Ctrl-Z),
// rclonewatch's own job is stopped too so the shell regains the terminal; when
// the job is continued, the command is continued as well.
func (t *terminal) wait(cmd *exec.Cmd) int {
	pid := cmd.Process.Pid
	defer cmd.Process.Release()
	for {
		var status syscall.WaitStatus
		if _, err := syscall.Wait4(pid, &status, syscall.WUNTRACED, nil); err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			t.reclaim(pid)
			return 1
		}
		if !status.Stopped() {
			t.reclaim(pid)
			return waitStatusExitCode(status)
		}
		t.reclaim(pid)
		t.suspend()
		// Give the terminal back only if the job was resumed in the foreground.
		if foreground, err := tcgetpgrp(t.fd); err == nil && foreground == t.pgrp {
			_ = tcsetpgrp(t.fd, pid)
		}
		_ = syscall.Kill(-pid, syscall.SIGCONT)
	}
}

// suspend stops rclonewatch's process group and returns once it has been
// continued. The stop signal is not delivered synchronously, so waiting for
// SIGCONT keeps the terminal from being handed back before the shell has
// observed the stop.
func (t *terminal) suspend() {
	if signal.Ignored(syscall.SIGTSTP) {
		return
	}
	continued := make(chan os.Signal, 1)
	signal.Notify(continued, syscall.SIGCONT)
	defer signal.Stop(continued)
	if err := syscall.Kill(0, syscall.SIGTSTP); err != nil {
		return
	}
	<-continued
}

// reclaim returns the terminal to rclonewatch if the command's group holds it.
func (t *terminal) reclaim(pid int) {
	if foreground, err := tcgetpgrp(t.fd); err == nil && foreground == pid {
		_ = tcsetpgrp(t.fd, t.pgrp)
	}
}

func waitStatusExitCode(status syscall.WaitStatus) int {
	switch {
	case status.Exited():
		return status.ExitStatus()
	case status.Signaled():
		return 128 + int(status.Signal())
	default:
		return 1
	}
}

func tcgetpgrp(fd int) (int, error) {
	var pgrp int32
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), syscall.TIOCGPGRP, uintptr(unsafe.Pointer(&pgrp))); errno != 0 {
		return 0, errno
	}
	return int(pgrp), nil
}

func tcsetpgrp(fd, pgrp int) error {
	value := int32(pgrp)
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), syscall.TIOCSPGRP, uintptr(unsafe.Pointer(&value))); errno != 0 {
		return errno
	}
	return nil
}
