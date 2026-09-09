package main

import (
	"errors"
	"path"
	"path/filepath"
	"strings"
)

const defaultSyncFile = ".rcw-sync"

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
	if relative == "" || filepath.IsAbs(relative) {
		return "", "", errors.New("local sync file path must be a non-empty relative path")
	}
	resolved, err := filepath.Abs(filepath.Join(root, filepath.FromSlash(relative)))
	if err != nil {
		return "", "", err
	}
	if resolved == filepath.Clean(root) {
		return "", "", errors.New("local sync file path resolves to the source directory")
	}
	filter := relativeLocalPath(root, resolved)
	return resolved, filter, nil
}

func resolveRemoteSyncFile(root, relative string) (string, string, error) {
	if relative == "" || path.IsAbs(relative) {
		return "", "", errors.New("remote sync file path must be a non-empty relative path")
	}
	colon := remoteColon(root)
	if colon < 0 {
		resolved := filepath.Clean(filepath.Join(root, filepath.FromSlash(relative)))
		if resolved == filepath.Clean(root) {
			return "", "", errors.New("remote sync file path resolves to the destination directory")
		}
		return resolved, relativeLocalPath(root, resolved), nil
	}

	prefix := root[:colon+1]
	remoteRoot := root[colon+1:]
	rootPath := path.Clean("/" + remoteRoot)
	resolvedPath := path.Clean(path.Join(rootPath, relative))
	if resolvedPath == rootPath {
		return "", "", errors.New("remote sync file path resolves to the destination directory")
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

func remoteColon(value string) int {
	if strings.HasPrefix(value, ":") {
		if next := strings.Index(value[1:], ":"); next >= 0 {
			return next + 1
		}
		return -1
	}
	colon := strings.Index(value, ":")
	slash := strings.IndexAny(value, `/\\`)
	if colon >= 0 && (slash < 0 || colon < slash) {
		return colon
	}
	return -1
}
