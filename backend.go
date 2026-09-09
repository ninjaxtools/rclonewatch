package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

func useConsistentWrites(destination string, disabled bool, runner commandRunner) (bool, error) {
	if disabled {
		return false, nil
	}
	var stdout, stderr bytes.Buffer
	if err := runner.Run([]string{"backend", "features", destination}, &stdout, &stderr); err != nil {
		return false, fmt.Errorf("detect destination backend: %w", commandError(err, stderr.String()))
	}
	var backend struct {
		Name string
	}
	if err := json.Unmarshal(stdout.Bytes(), &backend); err != nil {
		return false, fmt.Errorf("decode destination backend features: %w", err)
	}
	if backend.Name != "s3" {
		return false, nil
	}
	if err := requireConditionalWriteVersion(runner); err != nil {
		return false, err
	}
	return true, nil
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
