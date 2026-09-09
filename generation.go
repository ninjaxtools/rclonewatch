package main

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const generationFileName = ".rcw-generation"

type generationManager struct {
	localPath  string
	remotePath string
	source     string
	dest       string
	logs       bool
	logger     *log.Logger
	runner     commandRunner
	current    uint64
}

func newGenerationManager(source, destination string, logs bool, logger *log.Logger, runner commandRunner) *generationManager {
	return &generationManager{
		localPath:  filepath.Join(source, generationFileName),
		remotePath: remotePath(destination, generationFileName),
		source:     source,
		dest:       destination,
		logs:       logs,
		logger:     logger,
		runner:     runner,
	}
}

func (g *generationManager) Initialize() error {
	local, localExists, err := readLocalGeneration(g.localPath)
	if err != nil {
		return err
	}
	remote, remoteExists, err := g.readRemote()
	if err != nil {
		return err
	}

	switch {
	case !localExists && !remoteExists:
		if g.logs {
			g.logger.Printf("generation metadata is absent; syncing remote to local")
		}
		if err := g.syncFromRemote(); err != nil {
			return err
		}
		if err := g.writeRemoteVersion(1); err != nil {
			return fmt.Errorf("create remote generation: %w", err)
		}
		created, exists, err := g.readRemote()
		if err != nil {
			return fmt.Errorf("verify remote generation: %w", err)
		}
		if !exists || created != 1 {
			return fmt.Errorf("remote generation is %d after creating generation 1", created)
		}
		if err := writeLocalGeneration(g.localPath, 1); err != nil {
			return fmt.Errorf("create local generation: %w", err)
		}
		g.current = 1
	case localExists && !remoteExists:
		return errors.New("local generation exists but remote generation is missing")
	case remoteExists && (!localExists || local < remote):
		if g.logs {
			g.logger.Printf("remote generation %d is newer; syncing remote to local", remote)
		}
		if err := g.syncFromRemote(); err != nil {
			return err
		}
		// Rclone can consider equal-sized generation values unchanged when
		// the backend has coarse modtimes, so install the validated value.
		if err := writeLocalGeneration(g.localPath, remote); err != nil {
			return fmt.Errorf("install remote generation locally: %w", err)
		}
		synced, exists, err := readLocalGeneration(g.localPath)
		if err != nil {
			return err
		}
		if !exists || synced != remote {
			return fmt.Errorf("remote sync did not install generation %d locally", remote)
		}
		g.current = remote
	case local > remote:
		return fmt.Errorf("local generation %d is ahead of remote generation %d", local, remote)
	default:
		g.current = local
		if g.logs {
			g.logger.Printf("local and remote generation are current at %d", local)
		}
	}
	return nil
}

func (g *generationManager) BeforeRemoteChange() error {
	if g.current == 0 {
		return errors.New("generation metadata is not initialized")
	}
	if g.current == math.MaxUint64 {
		return errors.New("generation number cannot be incremented")
	}
	next := g.current + 1
	if err := writeLocalGeneration(g.localPath, next); err != nil {
		return fmt.Errorf("increment local generation: %w", err)
	}
	g.current = next
	if err := g.copyLocalToRemote(); err != nil {
		return fmt.Errorf("publish generation %d: %w", next, err)
	}
	remote, exists, err := g.readRemote()
	if err != nil {
		return fmt.Errorf("verify generation %d: %w", next, err)
	}
	if !exists || remote != next {
		return fmt.Errorf("remote generation is %d after publishing %d", remote, next)
	}
	if g.logs {
		g.logger.Printf("generation advanced to %d", next)
	}
	return nil
}

func (g *generationManager) syncFromRemote() error {
	if err := g.run("sync", g.dest, g.source, "--create-empty-src-dirs", "--exclude", "/.rcw-lock"); err != nil {
		return fmt.Errorf("sync remote to local: %w", err)
	}
	return nil
}

func (g *generationManager) readRemote() (uint64, bool, error) {
	var stdout, stderr bytes.Buffer
	err := g.runner.Run([]string{"cat", g.remotePath}, &stdout, &stderr)
	if err != nil {
		if isRcloneNotFound(err) {
			return 0, false, nil
		}
		return 0, false, commandError(err, stderr.String())
	}
	version, err := parseGeneration(stdout.String())
	if err != nil {
		return 0, false, fmt.Errorf("invalid remote generation: %w", err)
	}
	return version, true, nil
}

func (g *generationManager) writeRemoteVersion(version uint64) error {
	file, err := os.CreateTemp("", "rclonewatch-generation-*")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if _, err := fmt.Fprintf(file, "%d\n", version); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return g.copyToRemote(name)
}

func (g *generationManager) copyLocalToRemote() error {
	return g.copyToRemote(g.localPath)
}

func (g *generationManager) copyToRemote(localPath string) error {
	if err := g.run("copyto", localPath, g.remotePath); err != nil {
		return err
	}
	return nil
}

func (g *generationManager) run(args ...string) error {
	if g.logs {
		return g.runner.Run(args, os.Stdout, os.Stdout)
	}
	var stdout, stderr bytes.Buffer
	if err := g.runner.Run(args, &stdout, &stderr); err != nil {
		return commandError(err, stderr.String())
	}
	return nil
}

func readLocalGeneration(path string) (uint64, bool, error) {
	contents, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("read local generation: %w", err)
	}
	version, err := parseGeneration(string(contents))
	if err != nil {
		return 0, false, fmt.Errorf("invalid local generation: %w", err)
	}
	return version, true, nil
}

func writeLocalGeneration(path string, version uint64) error {
	return os.WriteFile(path, []byte(strconv.FormatUint(version, 10)+"\n"), 0o600)
}

func parseGeneration(value string) (uint64, error) {
	value = strings.TrimSpace(value)
	version, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %q: %w", value, err)
	}
	if version == 0 {
		return 0, errors.New("generation must be at least 1")
	}
	return version, nil
}
