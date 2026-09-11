package main

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func useConsistentWrites(destination string, disabled bool, runner commandRunner) (bool, error) {
	if disabled {
		return false, nil
	}
	backend, err := destinationBackend(destination, runner)
	if err != nil {
		return false, err
	}
	if backend != "s3" {
		return false, nil
	}
	if err := requireConditionalWriteVersion(runner); err != nil {
		return false, err
	}
	return true, nil
}

func destinationBackend(destination string, runner commandRunner) (string, error) {
	remote, err := parseRemote(destination)
	if err != nil {
		return "", err
	}
	if remote.colon < 0 {
		return "local", nil
	}
	if strings.HasPrefix(remote.name, ":") {
		return strings.TrimPrefix(remote.name, ":"), nil
	}
	if backend, ok := remote.options["type"]; ok {
		return backend, nil
	}
	if backend, ok := os.LookupEnv("RCLONE_CONFIG_" + strings.ToUpper(remote.name) + "_TYPE"); ok {
		return backend, nil
	}
	var stdout, stderr bytes.Buffer
	// Unlike backend features.Name, listremotes reports the backend type.
	// --long also works with older rclone versions used for non-S3 remotes.
	if err := runner.Run([]string{"listremotes", "--long"}, &stdout, &stderr); err != nil {
		return "", fmt.Errorf("detect destination backend: %w", commandError(err, stderr.String()))
	}
	for _, line := range strings.Split(stdout.String(), "\n") {
		name, rest, found := strings.Cut(line, ":")
		if fields := strings.Fields(rest); found && name == remote.name && len(fields) > 0 {
			return fields[0], nil
		}
	}
	return "", fmt.Errorf("cannot determine backend type for remote %q", remote.name)
}

func requireConditionalWriteVersion(runner commandRunner) error {
	var stdout, stderr bytes.Buffer
	if err := runner.Run([]string{"version"}, &stdout, &stderr); err != nil {
		return fmt.Errorf("check rclone version: %w", commandError(err, stderr.String()))
	}
	line, _, _ := strings.Cut(stdout.String(), "\n")
	fields := strings.Fields(line)
	if len(fields) < 2 || fields[0] != "rclone" {
		return fmt.Errorf("cannot parse rclone version from %q", line)
	}
	version := strings.TrimPrefix(fields[1], "v")
	version, _, _ = strings.Cut(version, "-")
	parts := strings.Split(version, ".")
	if len(parts) < 2 {
		return fmt.Errorf("cannot parse rclone version from %q", line)
	}
	major, majorErr := strconv.Atoi(parts[0])
	minor, minorErr := strconv.Atoi(parts[1])
	if majorErr != nil || minorErr != nil {
		return fmt.Errorf("cannot parse rclone version from %q", line)
	}
	if major < 1 || major == 1 && minor < 73 {
		return fmt.Errorf("S3 consistent writes require rclone v1.73.0 or newer (found %s); use --no-consistent-writes only if the provider does not support conditional writes", fields[1])
	}
	return nil
}
