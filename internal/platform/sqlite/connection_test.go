package sqlite

import (
	"testing"
)

func TestBuildConnectionString(t *testing.T) {
	tests := []struct {
		name     string
		dbPath   string
		want     string
		wantMode string
	}{
		{
			name:     "simple path",
			dbPath:   "/tmp/test.db",
			want:     "file:/tmp/test.db",
			wantMode: "rwc",
		},
		{
			name:     "windows path",
			dbPath:   "C:\\temp\\test.db",
			want:     "file:C:\\temp\\test.db",
			wantMode: "rwc",
		},
		{
			name:     "relative path",
			dbPath:   "./data/test.db",
			want:     "file:./data/test.db",
			wantMode: "rwc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildConnectionString(tt.dbPath)
			if got != tt.want+"?mode="+tt.wantMode {
				t.Errorf("buildConnectionString() = %v, want %v", got, tt.want+"?mode="+tt.wantMode)
			}
		})
	}
}

func TestDefaultPragmas(t *testing.T) {
	pragmas := defaultPragmas()

	if len(pragmas) == 0 {
		t.Error("expected default pragmas to be defined")
	}

	// Check for essential pragmas
	expectedPragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA busy_timeout=5000",
	}

	for _, expected := range expectedPragmas {
		found := false
		for _, pragma := range pragmas {
			if pragma == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected pragma %q not found in defaults", expected)
		}
	}
}
