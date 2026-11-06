//go:build linux

package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

const watchMask = syscall.IN_ATTRIB |
	syscall.IN_CLOSE_WRITE |
	syscall.IN_CREATE |
	syscall.IN_DELETE |
	syscall.IN_DELETE_SELF |
	syscall.IN_MODIFY |
	syscall.IN_MOVED_FROM |
	syscall.IN_MOVED_TO |
	syscall.IN_MOVE_SELF

type change struct {
	path    string
	isDir   bool
	removed bool
}

type watcher struct {
	root string
	// fd is the raw inotify descriptor used for watch management; file owns
	// it and performs blocking reads through the runtime poller. Both are
	// replaced only by the read loop, under mu so Close can wake it.
	fd     int
	file   *os.File
	mu     sync.Mutex
	closed bool
	events chan change
	errors chan error
	done   chan struct{}
	once   sync.Once

	byDescriptor map[int]string
	byPath       map[string]int
}

func newWatcher(root string) (*watcher, error) {
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil, fmt.Errorf("resolve watch root: %w", err)
	}
	w := &watcher{
		root:         root,
		fd:           -1,
		events:       make(chan change, 4096),
		errors:       make(chan error, 1),
		done:         make(chan struct{}),
		byDescriptor: make(map[int]string),
		byPath:       make(map[string]int),
	}
	if err := w.rebuild(); err != nil {
		return nil, err
	}
	go w.readLoop()
	return w, nil
}

// Use a new inotify instance so stale queued events and reused descriptors from
// the old tree cannot corrupt the rebuilt watch mappings. The following full
// sync covers changes made before their new watches were installed.
func (w *watcher) rebuild() error {
	fd, err := syscall.InotifyInit1(syscall.IN_CLOEXEC | syscall.IN_NONBLOCK)
	if err != nil {
		return fmt.Errorf("initialize inotify: %w", err)
	}
	replacement := &watcher{root: w.root, fd: fd, byDescriptor: make(map[int]string), byPath: make(map[string]int)}
	if err := replacement.addTree(w.root, false); err != nil {
		syscall.Close(fd)
		return err
	}
	if _, exists := replacement.byPath[w.root]; !exists {
		syscall.Close(fd)
		return errors.New("source folder is no longer a directory")
	}
	// The descriptor is non-blocking, so os.NewFile registers it with the
	// runtime poller: reads park the goroutine and honor read deadlines.
	file := os.NewFile(uintptr(fd), "inotify")
	w.mu.Lock()
	oldFile := w.file
	w.fd, w.file, w.byDescriptor, w.byPath = fd, file, replacement.byDescriptor, replacement.byPath
	if w.closed {
		file.SetReadDeadline(time.Unix(1, 0))
	}
	w.mu.Unlock()
	if oldFile != nil {
		oldFile.Close()
	}
	return nil
}

func (w *watcher) Events() <-chan change { return w.events }
func (w *watcher) Errors() <-chan error  { return w.errors }

// Close stops watching. Events already queued by the kernel are still
// delivered before the events channel closes.
func (w *watcher) Close() {
	w.once.Do(func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.closed = true
		// An expired deadline lets the reader drain readable events and then
		// return instead of blocking.
		w.file.SetReadDeadline(time.Unix(1, 0))
	})
}

func (w *watcher) addTree(root string, emit bool) error {
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if entry.IsDir() {
			if err := w.add(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
		if emit && path != w.root {
			w.send(change{path: w.relative(path), isDir: entry.IsDir()})
		}
		return nil
	})
}

func (w *watcher) add(path string) error {
	if _, exists := w.byPath[path]; exists {
		return nil
	}
	descriptor, err := syscall.InotifyAddWatch(w.fd, path, watchMask)
	if err != nil {
		return fmt.Errorf("watch %q: %w", path, err)
	}
	w.byDescriptor[descriptor] = path
	w.byPath[path] = descriptor
	return nil
}

func (w *watcher) removeTree(path string) {
	for watchedPath, descriptor := range w.byPath {
		if watchedPath == path || len(watchedPath) > len(path) && watchedPath[:len(path)+1] == path+string(os.PathSeparator) {
			syscall.InotifyRmWatch(w.fd, uint32(descriptor))
			delete(w.byPath, watchedPath)
			delete(w.byDescriptor, descriptor)
		}
	}
}

func (w *watcher) relative(path string) string {
	relative, err := filepath.Rel(w.root, path)
	if err != nil {
		return filepath.ToSlash(path)
	}
	return filepath.ToSlash(relative)
}

func (w *watcher) send(item change) {
	w.events <- item
}

func (w *watcher) fail(err error) {
	select {
	case w.errors <- err:
	default:
	}
}

func (w *watcher) readLoop() {
	defer close(w.done)
	defer close(w.events)
	defer close(w.errors)
	defer func() { w.file.Close() }()

	buffer := make([]byte, 64*1024)
	for {
		count, err := w.file.Read(buffer)
		if errors.Is(err, os.ErrDeadlineExceeded) {
			// Close was requested. The poller refuses reads after the deadline,
			// so drain already queued events directly until none remain.
			for {
				count, err := syscall.Read(w.fd, buffer)
				if errors.Is(err, syscall.EINTR) {
					continue
				}
				if errors.Is(err, syscall.EAGAIN) {
					return
				}
				if err != nil {
					w.fail(fmt.Errorf("read inotify events: %w", err))
					return
				}
				if !w.process(buffer[:count]) {
					return
				}
			}
		}
		if err != nil {
			w.fail(fmt.Errorf("read inotify events: %w", err))
			return
		}
		if !w.process(buffer[:count]) {
			return
		}
	}
}

// process handles one read of inotify events and reports whether reading
// should continue.
func (w *watcher) process(buffer []byte) bool {
	// unsafe.Sizeof includes tail padding for InotifyEvent.Name on newer Go
	// versions, while the kernel ABI header is always SizeofInotifyEvent.
	headerSize := syscall.SizeofInotifyEvent
	count := len(buffer)
	for offset := 0; offset+headerSize <= count; {
		event := (*syscall.InotifyEvent)(unsafe.Pointer(&buffer[offset]))
		size := headerSize + int(event.Len)
		if size < headerSize || offset+size > count {
			w.fail(fmt.Errorf("truncated inotify event: read=%d offset=%d event-size=%d name-size=%d", count, offset, size, event.Len))
			return false
		}
		name := string(bytes.TrimRight(buffer[offset+headerSize:offset+size], "\x00"))
		file := w.file
		w.handle(int(event.Wd), event.Mask, name)
		if w.file != file {
			// Everything remaining in this buffer belongs to the old watches.
			break
		}
		offset += size
	}
	return true
}

func (w *watcher) handle(descriptor int, mask uint32, name string) {
	if mask&syscall.IN_Q_OVERFLOW != 0 {
		if err := w.rebuild(); err != nil {
			w.fail(fmt.Errorf("rebuild watches after overflow: %w", err))
			w.Close()
			return
		}
		w.send(change{path: ".", isDir: true})
		return
	}
	base, exists := w.byDescriptor[descriptor]
	if !exists {
		return
	}
	path := base
	if name != "" {
		path = filepath.Join(base, name)
	}
	isDir := mask&syscall.IN_ISDIR != 0

	if mask&(syscall.IN_DELETE_SELF|syscall.IN_MOVE_SELF) != 0 && path == w.root {
		w.fail(errors.New("source folder was removed or moved"))
		w.Close()
		return
	}
	if isDir && mask&(syscall.IN_DELETE|syscall.IN_MOVED_FROM) != 0 {
		w.removeTree(path)
	}
	if isDir && mask&(syscall.IN_CREATE|syscall.IN_MOVED_TO) != 0 {
		if err := w.addTree(path, true); err != nil && !errors.Is(err, os.ErrNotExist) {
			w.fail(err)
			w.Close()
		}
		return
	}
	if mask&syscall.IN_IGNORED != 0 {
		delete(w.byDescriptor, descriptor)
		delete(w.byPath, base)
		return
	}
	if mask&(syscall.IN_ATTRIB|syscall.IN_CLOSE_WRITE|syscall.IN_CREATE|syscall.IN_DELETE|syscall.IN_MODIFY|syscall.IN_MOVED_FROM|syscall.IN_MOVED_TO) != 0 {
		w.send(change{
			path:    w.relative(path),
			isDir:   isDir,
			removed: mask&(syscall.IN_DELETE|syscall.IN_MOVED_FROM) != 0,
		})
	}
}
