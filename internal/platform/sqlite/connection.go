package sqlite

import "fmt"

// buildConnectionString creates a SQLite connection string from a database path
func buildConnectionString(dbPath string) string {
	return fmt.Sprintf("file:%s?mode=rwc", dbPath)
}

// defaultPragmas returns the default PRAGMA statements for SQLite configuration
func defaultPragmas() []string {
	return []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA busy_timeout=5000",
	}
}
