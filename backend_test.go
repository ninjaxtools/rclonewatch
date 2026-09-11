package main

import (
	"fmt"
	"io"
	"path/filepath"
	"testing"
)

type backendRunner struct {
	name    string
	version string
	calls   int
}

func TestBackendDetectionWithRclone(t *testing.T) {
	requireRclone(t)
	config := filepath.Join(t.TempDir(), "rclone.conf")
	writeTestFile(t, config, "[backup]\ntype = s3\nprovider = AWS\n[s3]\ntype = local\n[space name]\ntype = s3\n")
	t.Setenv("RCLONE_CONFIG", config)
	t.Setenv("RCLONE_CONFIG_ENVBACKUP_TYPE", "s3")
	t.Setenv("RCLONE_CONFIG_OVERRIDDEN_TYPE", "local")
	for _, test := range []struct {
		destination string
		want        bool
	}{
		{"backup:bucket/path", true},
		{"space name:bucket/path", true},
		{"envbackup:bucket/path", true},
		{"ENVBACKUP:bucket/path", true},
		{"s3:path", false},
		{"backup,type=local:path", false},
		{"s3,type='s3':bucket/path", true},
		{"overridden:path", false},
		{":s3:bucket/path", true},
		{`:s3,endpoint='http://localhost:9000',provider=Minio:bucket/path`, true},
		{`backup,provider='Other',endpoint="http://localhost:9000":bucket/path`, true},
		{t.TempDir(), false},
		{"local,backup", false},
	} {
		t.Run(test.destination, func(t *testing.T) {
			got, err := useConsistentWrites(test.destination, false, &rcloneCommand{})
			if err != nil || got != test.want {
				t.Fatalf("consistent writes = %v, error %v, want %v", got, err, test.want)
			}
		})
	}
}

func (r *backendRunner) Run(args []string, stdout, _ io.Writer) error {
	r.calls++
	switch args[0] {
	case "listremotes":
		_, err := fmt.Fprintf(stdout, "remote: %s\n", r.name)
		return err
	case "version":
		_, err := fmt.Fprintf(stdout, "rclone %s\n", r.version)
		return err
	default:
		return fmt.Errorf("unexpected command %q", args[0])
	}
}

func TestUseConsistentWrites(t *testing.T) {
	tests := []struct {
		name      string
		backend   string
		version   string
		disabled  bool
		want      bool
		wantError bool
		wantCalls int
	}{
		{name: "supported s3", backend: "s3", version: "v1.73.0", want: true, wantCalls: 2},
		{name: "newer s3", backend: "s3", version: "v1.74.1-DEV", want: true, wantCalls: 2},
		{name: "old s3", backend: "s3", version: "v1.72.2", wantError: true, wantCalls: 2},
		{name: "local", backend: "local", version: "v1.60.1", wantCalls: 1},
		{name: "disabled", disabled: true, wantCalls: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := &backendRunner{name: test.backend, version: test.version}
			got, err := useConsistentWrites("remote:path", test.disabled, runner)
			if (err != nil) != test.wantError {
				t.Fatalf("error = %v, wantError %v", err, test.wantError)
			}
			if got != test.want {
				t.Fatalf("useConsistentWrites = %v, want %v", got, test.want)
			}
			if runner.calls != test.wantCalls {
				t.Fatalf("commands = %d, want %d", runner.calls, test.wantCalls)
			}
		})
	}
}
