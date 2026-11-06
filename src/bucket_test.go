package main

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

func TestRcloneCommandsDisableBucketCreation(t *testing.T) {
	bin := t.TempDir()
	log := filepath.Join(t.TempDir(), "args")
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$RCLONEWATCH_ARG_LOG\"\n"
	if err := os.WriteFile(filepath.Join(bin, "rclone"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("RCLONEWATCH_ARG_LOG", log)
	for _, args := range [][]string{
		{"copyto", "/tmp/state", "s3:bucket/.rcw-state", "--header-upload", "If-None-Match: *"},
		{"sync", "/tmp/payload", "s3:bucket/prefix"},
		{"sync", "s3:bucket/prefix", "/tmp/payload"},
		{"cat", "s3:bucket/.rcw-state"},
		{"delete", "s3:bucket/prefix"},
		{"rmdirs", "s3:bucket/prefix", "--leave-root"},
		{"version"},
	} {
		for _, captured := range []bool{false, true} {
			runner := newRcloneCommand()
			var output io.Writer
			if captured {
				output = io.Discard
			}
			// Spare capacity must not let Run overwrite the caller's slice.
			backing := append(append([]string(nil), args...), "sentinel")
			err := runner.Run(backing[:len(args)], output, output)
			runner.Cancel()
			if err != nil {
				t.Fatal(err)
			}
			if backing[len(args)] != "sentinel" || !reflect.DeepEqual(backing[:len(args)], args) {
				t.Fatalf("caller arguments were modified: %v", backing)
			}
			data, err := os.ReadFile(log)
			if err != nil {
				t.Fatal(err)
			}
			want := strings.Join(append(append([]string(nil), args...), "--s3-no-check-bucket"), "\n") + "\n"
			if string(data) != want {
				t.Fatalf("rclone arguments = %q, want %q", data, want)
			}
		}
	}
}

func TestS3UploadsWithoutBucketCreation(t *testing.T) {
	requireRclone(t)
	config := filepath.Join(t.TempDir(), "rclone.conf")
	writeTestFile(t, config, "")
	t.Setenv("RCLONE_CONFIG", config)
	// The command-line default must work even when the environment says false.
	t.Setenv("RCLONE_S3_NO_CHECK_BUCKET", "false")
	for _, command := range []string{"copyto", "sync"} {
		t.Run(command, func(t *testing.T) {
			var bucketChecks, bucketCreates, uploads atomic.Int32
			const contents = "payload"
			hash := md5.Sum([]byte(contents))
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/existing-bucket" || r.URL.Path == "/existing-bucket/" {
					switch r.Method {
					case http.MethodHead:
						bucketChecks.Add(1)
					case http.MethodPut:
						bucketCreates.Add(1)
					case http.MethodGet:
						w.Header().Set("Content-Type", "application/xml")
						fmt.Fprint(w, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>existing-bucket</Name><IsTruncated>false</IsTruncated></ListBucketResult>`)
						return
					}
					w.WriteHeader(http.StatusForbidden)
					return
				}
				if r.Method == http.MethodPut && r.URL.Path == "/existing-bucket/prefix/payload.txt" {
					_, _ = io.Copy(io.Discard, r.Body)
					uploads.Add(1)
					w.Header().Set("ETag", fmt.Sprintf(`"%x"`, hash))
					w.WriteHeader(http.StatusOK)
					return
				}
				w.WriteHeader(http.StatusNotFound)
			}))
			defer server.Close()
			remote := ":s3,provider=Other,env_auth=false,access_key_id=test,secret_access_key=test,endpoint='" + server.URL + "':existing-bucket/prefix"
			source := t.TempDir()
			file := filepath.Join(source, "payload.txt")
			writeTestFile(t, file, contents)
			args := []string{command, source, remote}
			if command == "copyto" {
				args = []string{command, file, remote + "/payload.txt"}
			}
			args = append(args, "--s3-no-head", "--retries", "1", "--low-level-retries", "1")
			var output bytes.Buffer
			runner := newRcloneCommand()
			defer runner.Cancel()
			if err := runner.Run(args, &output, &output); err != nil {
				t.Fatalf("%s: %v\n%s", command, err, output.String())
			}
			if bucketChecks.Load() != 0 || bucketCreates.Load() != 0 || uploads.Load() != 1 {
				t.Fatalf("bucket checks=%d, bucket creations=%d, uploads=%d", bucketChecks.Load(), bucketCreates.Load(), uploads.Load())
			}
		})
	}
}
