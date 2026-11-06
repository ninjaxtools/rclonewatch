package main

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestLockWaitParsing(t *testing.T) {
	tests := []struct {
		value     string
		duration  time.Duration
		infinite  bool
		wantError bool
	}{
		{value: "0"},
		{value: "30s", duration: 30 * time.Second},
		{value: "1h30m", duration: 90 * time.Minute},
		{value: "inf", infinite: true},
		{value: "1ms", wantError: true},
		{value: "-1s", wantError: true},
		{value: "later", wantError: true},
	}
	for _, test := range tests {
		t.Run(test.value, func(t *testing.T) {
			var wait lockWait
			err := wait.Set(test.value)
			if (err != nil) != test.wantError {
				t.Fatalf("Set error = %v, wantError %v", err, test.wantError)
			}
			if wait.duration != test.duration || wait.infinite != test.infinite {
				t.Fatalf("wait = %#v, want duration %v infinite %v", wait, test.duration, test.infinite)
			}
			if err == nil && !wait.explicit {
				t.Fatal("parsed lock wait was not marked explicit")
			}
		})
	}
}

func TestExcludePatterns(t *testing.T) {
	var patterns excludePatterns
	for _, pattern := range []string{"*.tmp", "/cache/**"} {
		if err := patterns.Set(pattern); err != nil {
			t.Fatal(err)
		}
	}
	if got := patterns.String(); got != "*.tmp,/cache/**" {
		t.Fatalf("String = %q", got)
	}
	if err := patterns.Set(""); err == nil {
		t.Fatal("empty pattern was accepted")
	}
}

func TestHelpIncludesOptionsAndExamples(t *testing.T) {
	var output bytes.Buffer
	printHelp(&output)
	for _, text := range []string{"--interval", "--lock-ttl", "--lock-wait", "--persistent-lock", "--upload-only", "--exclude", "--no-consistent-writes", "--force-delete-untracked-remote", "--fail-on-incomplete-sync", "--state-file", "--state-file-local", "--state-file-remote", "--logs", "Wrapped command:", "State file behavior:", "Examples:"} {
		if !strings.Contains(output.String(), text) {
			t.Errorf("help does not contain %q", text)
		}
	}
	for _, text := range []string{"--use-lock", "--reconcile-remote-changes", "--sync-remote", "--sync-file", ".rcw-sync"} {
		if strings.Contains(output.String(), text) {
			t.Errorf("help contains renamed value %q", text)
		}
	}
}
