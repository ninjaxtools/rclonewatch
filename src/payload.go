package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"unicode"
	"unicode/utf8"
)

// Rclone globs accept backslash escapes. Escape only glob metacharacters;
// ordinary regexp metacharacters are already escaped by rclone's glob parser.
func literalFilterPath(path string) string {
	var pattern strings.Builder
	for _, c := range path {
		if strings.ContainsRune(`\*?[]{}`, c) {
			pattern.WriteByte('\\')
		}
		pattern.WriteRune(c)
	}
	return pattern.String()
}

// Delete before copying so file/directory replacements can converge. Excluded
// entries remain protected. Unconditional copies are necessary even for a full
// recovery: equal size/mtime (or unavailable backend hashes) do not prove equal
// content after a known change or interrupted session.
func payloadSyncArgs(source, destination string, stateFilters, excludes []string) []string {
	args := []string{"sync", source, destination, "--create-empty-src-dirs", "--delete-before", "--ignore-times"}
	for _, filter := range stateFilters {
		args = append(args, "--exclude", "/"+filter)
	}
	for _, pattern := range excludes {
		args = append(args, "--exclude", pattern)
	}
	return args
}

// A queued child event can outlive its parent directory. Promote such a path
// to the missing/replaced ancestor, then collapse overlapping scopes. The
// current tree, not the event's old file/directory type, determines the result.
func (s *syncer) reconciliationScopes(paths []string) ([]string, error) {
	normalized := make([]string, 0, len(paths))
	for _, relative := range paths {
		if relative == "." {
			return []string{"."}, nil
		}
		if relative == "" || path.IsAbs(relative) || path.Clean(relative) != relative || relative == ".." || strings.HasPrefix(relative, "../") {
			return nil, fmt.Errorf("invalid changed path %q", relative)
		}
		// Regex filters cannot distinguish invalid UTF-8 byte sequences. Scope
		// their nearest representable ancestor rather than matching a different
		// filename or silently omitting the change.
		for !utf8.ValidString(relative) {
			relative = path.Dir(relative)
		}
		if relative == "." {
			return []string{"."}, nil
		}
		for parent := path.Dir(relative); parent != "."; parent = path.Dir(parent) {
			info, err := os.Lstat(filepath.Join(s.source, filepath.FromSlash(parent)))
			if err == nil && info.IsDir() {
				break
			}
			if err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, syscall.ENOTDIR) {
				return nil, fmt.Errorf("inspect changed ancestor %q: %w", parent, err)
			}
			relative = parent
		}
		if s.state != nil {
			if exact, _ := s.state.protects(relative); exact {
				continue
			}
		}
		normalized = append(normalized, relative)
	}
	sort.Strings(normalized)
	selected := make(map[string]bool, len(normalized))
	scopes := make([]string, 0, len(normalized))
	for _, relative := range normalized {
		covered := selected[relative]
		for parent := path.Dir(relative); parent != "." && !covered; parent = path.Dir(parent) {
			covered = selected[parent]
		}
		if !covered {
			selected[relative] = true
			scopes = append(scopes, relative)
		}
	}
	return scopes, nil
}

// Filter files are line-oriented and trim whitespace. Hex escapes also keep
// literal braces/stars out of rclone's directory-glob inference, allowing it
// to prune unrelated directories rather than walking the complete tree.
func scopeFilterPath(relative string) string {
	var pattern strings.Builder
	for _, c := range standardLocalPath(relative) {
		switch {
		case c < 128 && (unicode.IsSpace(c) || strings.ContainsRune(`\*?[]{}`, c)):
			fmt.Fprintf(&pattern, `\x%02x`, c)
		case unicode.IsSpace(c):
			pattern.WriteRune('[')
			pattern.WriteRune(c)
			pattern.WriteRune(']')
		default:
			pattern.WriteRune(c)
		}
	}
	return pattern.String()
}

// Rclone filters see Standard-encoded names, not the physical names emitted by
// inotify. With Linux's default local encoding (Slash,Dot), the additional
// Standard escapes are control characters and DEL. Literal control pictures
// must be quoted to distinguish them from encoded control characters.
func standardLocalPath(relative string) string {
	parts := strings.Split(relative, "/")
	for i, name := range parts {
		switch name {
		case "．", "．．", "‛．", "‛．‛．":
			// These special dot encodings are shared by local and Standard.
			continue
		}
		var encoded strings.Builder
		quotes := 0
		for _, c := range name {
			if c == '‛' {
				quotes++
				continue
			}
			if c != '␀' && c != '／' {
				// Local decoding preserves an unmatched quote literally; Standard
				// encoding doubles it. NUL and slash already share quote semantics.
				quotes += quotes % 2
			}
			encoded.WriteString(strings.Repeat("‛", quotes))
			quotes = 0
			switch {
			case c > 0 && c < 32:
				encoded.WriteRune('␀' + c)
			case c == 127:
				encoded.WriteRune('␡')
			case c > '␀' && c <= '␟', c == '␡':
				encoded.WriteRune('‛')
				encoded.WriteRune(c)
			default:
				encoded.WriteRune(c)
			}
		}
		encoded.WriteString(strings.Repeat("‛", quotes+quotes%2))
		parts[i] = encoded.String()
	}
	return strings.Join(parts, "/")
}

func (s *syncer) syncScopes(scopes []string) error {
	var existing, deleted []string
	for _, relative := range scopes {
		_, err := os.Lstat(filepath.Join(s.source, filepath.FromSlash(relative)))
		switch {
		case err == nil:
			existing = append(existing, relative)
		case errors.Is(err, os.ErrNotExist), errors.Is(err, syscall.ENOTDIR):
			deleted = append(deleted, relative)
		default:
			return fmt.Errorf("inspect changed path %q: %w", relative, err)
		}
	}
	// Deletions must not create source ancestor directories or replace remote
	// ancestor files: deleting a/b when remote a is a file is already a no-op.
	if len(deleted) > 0 {
		if err := s.syncScopeGroup(deleted, false); err != nil {
			return err
		}
	}
	if len(existing) > 0 {
		return s.syncScopeGroup(existing, true)
	}
	return nil
}

func (s *syncer) syncScopeGroup(scopes []string, createDirectories bool) error {
	file, err := os.CreateTemp("", "rclonewatch-scopes-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	writer := bufio.NewWriter(file)
	// Keep both roots unchanged so anchored user exclusions and metadata
	// exclusions retain their meaning. Include only selected paths/subtrees.
	written := make(map[string]bool)
	include := func(pattern string) error {
		if written[pattern] {
			return nil
		}
		written[pattern] = true
		_, err := fmt.Fprintf(writer, "+ /%s\n", pattern)
		return err
	}
	for _, relative := range scopes {
		literal := scopeFilterPath(relative)
		if err := include(literal); err != nil {
			return err
		}
		if err := include(literal + "/**"); err != nil {
			return err
		}
		// Include ancestor files (not their whole subtrees) so a remote file
		// blocking a changed descendant can be replaced with a directory.
		for parent := path.Dir(relative); createDirectories && parent != "."; parent = path.Dir(parent) {
			if err := include(scopeFilterPath(parent)); err != nil {
				return err
			}
		}
	}
	if _, err := writer.WriteString("- /**\n"); err != nil {
		return err
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	var filters []string
	if s.state != nil {
		filters = s.state.payloadFilters()
	}
	args := payloadSyncArgs(s.source, s.dest, filters, s.excludes)
	if !createDirectories {
		args = append(args, "--create-empty-src-dirs=false")
	}
	// Rclone processes --exclude before --filter-from, so scoped includes
	// cannot override either user exclusions or protected metadata.
	args = append(args, "--filter-from", file.Name())
	if err := s.run(args...); err != nil {
		return fmt.Errorf("sync changed paths: %w", err)
	}
	return nil
}
