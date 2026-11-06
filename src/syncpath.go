package main

import (
	"errors"
	"path"
	"path/filepath"
	"strings"
)

const defaultStateFile = ".rcw-state"

type syncFilePaths struct {
	local        string
	remote       string
	localFilter  string
	remoteFilter string
}

func resolveSyncFilePaths(source, destination, localRelative, remoteRelative string) (syncFilePaths, error) {
	local, localFilter, err := resolveLocalSyncFile(source, localRelative)
	if err != nil {
		return syncFilePaths{}, err
	}
	remote, remoteFilter, err := resolveRemoteSyncFile(destination, remoteRelative)
	if err != nil {
		return syncFilePaths{}, err
	}
	return syncFilePaths{local: local, remote: remote, localFilter: localFilter, remoteFilter: remoteFilter}, nil
}

func resolveLocalSyncFile(root, relative string) (string, string, error) {
	if strings.ContainsAny(relative, "\r\n") {
		return "", "", errors.New("state file paths must not contain line breaks")
	}
	if relative == "" || filepath.IsAbs(relative) {
		return "", "", errors.New("local state file path must be a non-empty relative path")
	}
	resolved, err := filepath.Abs(filepath.Join(root, filepath.FromSlash(relative)))
	if err != nil {
		return "", "", err
	}
	if resolved == filepath.Clean(root) {
		return "", "", errors.New("local state file path resolves to the source directory")
	}
	filter := relativeLocalPath(root, resolved)
	return resolved, filter, nil
}

func resolveRemoteSyncFile(root, relative string) (string, string, error) {
	if strings.ContainsAny(relative, "\r\n") {
		return "", "", errors.New("state file paths must not contain line breaks")
	}
	if relative == "" || path.IsAbs(relative) {
		return "", "", errors.New("remote state file path must be a non-empty relative path")
	}
	remote, err := parseRemote(root)
	if err != nil {
		return "", "", err
	}
	colon := remote.colon
	if colon < 0 {
		resolved := filepath.Clean(filepath.Join(root, filepath.FromSlash(relative)))
		if resolved == filepath.Clean(root) {
			return "", "", errors.New("remote state file path resolves to the destination directory")
		}
		return resolved, relativeLocalPath(root, resolved), nil
	}

	prefix := root[:colon+1]
	remoteRoot := root[colon+1:]
	rootPath := path.Clean("/" + remoteRoot)
	resolvedPath := path.Clean(path.Join(rootPath, relative))
	if resolvedPath == rootPath {
		return "", "", errors.New("remote state file path resolves to the destination directory")
	}
	filter := relativeSlashPath(rootPath, resolvedPath)
	if strings.HasPrefix(remoteRoot, "/") {
		return prefix + resolvedPath, filter, nil
	}
	return prefix + strings.TrimPrefix(resolvedPath, "/"), filter, nil
}

func relativeLocalPath(root, target string) string {
	relative, err := filepath.Rel(root, target)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return ""
	}
	return filepath.ToSlash(relative)
}

func relativeSlashPath(root, target string) string {
	relative, err := filepath.Rel(filepath.FromSlash(root), filepath.FromSlash(target))
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return ""
	}
	return filepath.ToSlash(relative)
}

type remoteSpec struct {
	name    string
	options map[string]string
	colon   int
}

// Connection strings may contain quoted colons, commas, and slashes. Doubled
// quotes represent a literal quote, as in rclone's connection-string syntax.
func parseRemote(value string) (remoteSpec, error) {
	remote := remoteSpec{colon: -1}
	if !strings.Contains(value, ":") {
		return remote, nil
	}
	start := 0
	if strings.HasPrefix(value, ":") {
		start = 1
	}
	end := strings.IndexAny(value[start:], ":,/\\")
	if end < 0 {
		return remote, nil
	}
	end += start
	if value[end] == '/' || value[end] == '\\' {
		return remote, nil
	}
	remote.name = value[:end]
	remote.options = make(map[string]string)
	for value[end] == ',' {
		end++
		keyStart := end
		for end < len(value) && value[end] != '=' && value[end] != ',' && value[end] != ':' {
			end++
		}
		if end == keyStart || end == len(value) {
			return remote, errors.New("invalid remote connection-string option")
		}
		key, option := value[keyStart:end], "true"
		if value[end] == '=' {
			end++
			var text strings.Builder
			if end < len(value) && (value[end] == '\'' || value[end] == '"') {
				quote := value[end]
				end++
				for {
					if end == len(value) {
						return remote, errors.New("unterminated remote connection-string quote")
					}
					if value[end] == quote {
						end++
						if end == len(value) || value[end] != quote {
							break
						}
					}
					text.WriteByte(value[end])
					end++
				}
			} else {
				for end < len(value) && value[end] != ',' && value[end] != ':' {
					text.WriteByte(value[end])
					end++
				}
			}
			option = text.String()
		}
		remote.options[key] = option
		if end == len(value) || (value[end] != ',' && value[end] != ':') {
			return remote, errors.New("remote connection string needs a trailing colon")
		}
	}
	remote.colon = end
	return remote, nil
}
