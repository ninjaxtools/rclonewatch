package main

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestResolveStateFilePaths(t *testing.T) {
	source := filepath.Join(t.TempDir(), "source")
	tests := []struct {
		name         string
		destination  string
		local        string
		remote       string
		wantLocal    string
		wantRemote   string
		localFilter  string
		remoteFilter string
	}{
		{
			name:         "inside roots",
			destination:  "remote:bucket/root",
			local:        ".meta/local.json",
			remote:       ".meta/remote.json",
			wantLocal:    filepath.Join(source, ".meta", "local.json"),
			wantRemote:   "remote:bucket/root/.meta/remote.json",
			localFilter:  ".meta/local.json",
			remoteFilter: ".meta/remote.json",
		},
		{
			name:        "outside roots",
			destination: "remote:bucket/root",
			local:       "../local.json",
			remote:      "../remote.json",
			wantLocal:   filepath.Join(filepath.Dir(source), "local.json"),
			wantRemote:  "remote:bucket/remote.json",
		},
		{
			name:         "local destination",
			destination:  filepath.Join(t.TempDir(), "destination"),
			local:        defaultStateFile,
			remote:       defaultStateFile,
			wantLocal:    filepath.Join(source, ".rcw-state"),
			localFilter:  ".rcw-state",
			remoteFilter: ".rcw-state",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.wantRemote == "" {
				test.wantRemote = filepath.Join(test.destination, ".rcw-state")
			}
			got, err := resolveSyncFilePaths(source, test.destination, test.local, test.remote)
			if err != nil {
				t.Fatal(err)
			}
			want := syncFilePaths{local: test.wantLocal, remote: test.wantRemote, localFilter: test.localFilter, remoteFilter: test.remoteFilter}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("paths = %#v, want %#v", got, want)
			}
		})
	}
}

func TestResolveStateFilePathsRejectsInvalidNames(t *testing.T) {
	for _, paths := range [][2]string{{"", "state"}, {"state", ""}, {"/absolute", "state"}, {"state", "/absolute"}, {".", "state"}, {"state", "."}, {"state\nfile", "state"}, {"state", "state\rfile"}} {
		if _, err := resolveSyncFilePaths(t.TempDir(), "remote:root", paths[0], paths[1]); err == nil {
			t.Fatalf("paths %#v were accepted", paths)
		}
	}
}

func TestStatePathsWithQuotedConnectionOptions(t *testing.T) {
	for _, root := range []string{
		`:s3,endpoint='http://localhost:9000',provider=Minio:bucket/root`,
		`remote,description='it''s: a test',type="s3":bucket/root`,
	} {
		paths, err := resolveSyncFilePaths(t.TempDir(), root, "state", "../state")
		if err != nil {
			t.Fatal(err)
		}
		want := root[:len(root)-len("root")] + "state"
		if paths.remote != want || paths.remoteFilter != "" {
			t.Fatalf("resolved quoted remote = %q (filter %q), want %q", paths.remote, paths.remoteFilter, want)
		}
	}
}

func TestSyncStatePayloadFilters(t *testing.T) {
	state := &syncState{paths: syncFilePaths{localFilter: ".local/state", remoteFilter: ".remote/state"}}
	if got, want := state.payloadFilters(), []string{".local/state.rcw-tmp-*", ".local/state", ".remote/state"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("payloadFilters = %#v, want %#v", got, want)
	}
	if exact, ancestor := state.protects(".local/state"); !exact || ancestor {
		t.Fatalf("exact protection = (%v, %v)", exact, ancestor)
	}
	if exact, ancestor := state.protects(".remote"); exact || !ancestor {
		t.Fatalf("ancestor protection = (%v, %v)", exact, ancestor)
	}
	if exact, ancestor := state.protects(".local/state.rcw-tmp-123"); !exact || ancestor {
		t.Fatalf("temporary state protection = (%v, %v)", exact, ancestor)
	}
}
