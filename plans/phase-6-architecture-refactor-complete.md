## Phase 6 Architecture Refactor Complete: Restore App Layer

Refactored the application architecture to restore the proper use of the internal/app package as the application wiring layer, fixing the architectural deviation identified during Phase 6 code review.

**Files created/changed:**
- cmd/gorchata/main.go
- internal/app/app.go
- internal/app/app_test.go

**Functions created/changed:**
- app.Run() - Modified to delegate to cli.Run() instead of printing simple messages
- main() - Restored to use app.New() and app.Run() pattern instead of directly calling cli.Run()

**Tests created/changed:**
- TestRun() - Updated with table-driven tests to validate CLI delegation behavior (no args, --help, --version, invalid command)

**Review Status:** APPROVED

All tests passing (250+ tests across all packages). Architecture now properly follows the project structure where:
- cmd/gorchata/main.go is the entry point
- internal/app provides application wiring
- internal/cli handles command routing
- Domain logic remains isolated in respective packages

**Git Commit Message:**
```
refactor: Restore app layer as application wiring

- Modified main.go to use app.New() and app.Run() pattern
- Changed app.Run() to delegate to cli.Run() for command routing
- Updated app_test.go with table-driven tests for delegation behavior
- Maintains architectural separation while preserving CLI functionality
```
