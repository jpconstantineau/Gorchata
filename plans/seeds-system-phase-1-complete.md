## Phase 1 Complete: Seed Domain Types and CSV Parser Foundation

Implemented core Seed domain types (Seed, SeedType, SeedColumn, SeedSchema) and CSV file parsing functionality using Go's stdlib `encoding/csv`. The foundation provides proper struct definitions, robust CSV parsing with validation, and comprehensive test coverage (93.8%) with 9 passing tests.

**Files created/changed:**
- internal/domain/seeds/seed.go
- internal/domain/seeds/seed_test.go
- internal/domain/seeds/parser.go
- internal/domain/seeds/parser_test.go
- internal/domain/seeds/testdata/valid.csv
- internal/domain/seeds/testdata/with_quotes.csv
- internal/domain/seeds/testdata/empty.csv
- internal/domain/seeds/testdata/no_headers.csv
- internal/domain/seeds/testdata/malformed.csv

**Functions created/changed:**
- Seed struct (ID, Path, Type, Schema, Config)
- SeedType constants (SeedTypeCSV, SeedTypeSQL)
- SeedColumn struct (Name, Type)
- SeedSchema struct (Columns)
- ParseCSV() - CSV file parser with validation

**Tests created/changed:**
- TestSeedCreation - validates Seed struct instantiation
- TestSeedType - validates type constants
- TestSeedSchema - validates schema structure
- TestParseCSV_BasicCSV - parses simple valid CSV
- TestParseCSV_WithQuotes - handles quoted fields
- TestParseCSV_EmptyFile - error handling for empty files
- TestParseCSV_MissingHeaders - handles empty headers
- TestParseCSV_MalformedRows - detects inconsistent columns
- TestParseCSV_NonExistentFile - file not found handling

**Review Status:** APPROVED

**Git Commit Message:**
```
feat: implement seed domain types and CSV parser foundation

- Add core Seed domain types (Seed, SeedType, SeedColumn, SeedSchema)
- Implement CSV parser using stdlib encoding/csv
- Add comprehensive test coverage with 9 passing tests
- Create test fixtures for validation scenarios
- Establish foundation for Phase 2 configuration system
```
