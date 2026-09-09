package main

import (
	"fmt"
	"io"
	"testing"
)

type backendRunner struct {
	name    string
	version string
	calls   int
}

func (r *backendRunner) Run(args []string, stdout, _ io.Writer) error {
	r.calls++
	switch args[0] {
	case "backend":
		_, err := fmt.Fprintf(stdout, `{"Name":%q}`, r.name)
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
