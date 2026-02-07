<!-- GitHub Copilot / AI agent instructions for the repo -->
# Guidance for AI coding agents

# Model Instructions: Go (1.25+) Bubble Tea + SQLite (no CGO) Repo

These instructions define **non-negotiable project rules** for all future work in this repository. Any generated code, tests, documentation, and scripts **must** follow them.

---

## 0) Hard Requirements (Do Not Violate)

1. **Language/Tooling**
   - Go version: **1.25+** (`go.mod` must declare `go 1.25` or higher).
   - **No CGO**. All dependencies must work with `CGO_ENABLED=0`.
   - Target OS: cross-platform; build scripts run on Windows via **PowerShell**.


2. **Development Process**
   - **TDD is mandatory**:
     1) Write test(s) first  
     2) Run tests and confirm failure  
     3) Implement minimal code  
     4) Run tests until passing  
     5) Refactor while keeping tests green
     6) build/run via PowerShell script to confirm
   - Any **Strategy pattern** (or other pluggable business logic) **must be unit tested**.

3. **Docs**
   - `README.md` must **always match** current behavior:
     - How to run the game
     - Configuration details
     - DB location and migration behavior
     - Troubleshooting / reset steps if applicable

---

## 1) Repository Layout (Standard Go Project Structure)

Use this layout and naming consistently:

```text
.
├─ cmd/
│  └─ app/
│     └─ main.go
├─ internal/
│  ├─ app/                # application wiring (DI), program startup
│  ├─ ui/                 # Bubble Tea models, views, keymaps
│  ├─ domain/             # domain logic (pure), rules, strategies
│  ├─ store/              # persistence interfaces + SQLite implementation
│  └─ db/                 # migrations, schema versioning, DB bootstrap
├─ scripts/
│  ├─ build.ps1
│  └─ test.ps1            # optional if separated, otherwise build.ps1 covers it
├─ assets/                # optional (help text, seed data, etc.)
├─ domain/                # optional (business domain knowledge for designing the application)
├─ ui/                    # optional (UI hierarchy diagrams, mockups)
├─ README.md
├─ go.mod
├─ go.sum
└─ LICENSE                # if present
```

Rules:
- `cmd/gapp/main.go` must remain thin: parse config, call `internal/app`.
- Domain code must live under `internal/domain` and be **UI-agnostic** and **DB-agnostic**.
- SQLite code must live under `internal/store/sqlite` (or similar) and must implement interfaces defined in `internal/store`.

---

## 2) Architecture Boundaries (Strict Separation)

### 2.1 Domain (`internal/domain`)
- Contains application state, rules, scoring, progression, etc.
- Must be deterministic and testable.
- Must not import Bubble Tea, SQL drivers, or OS/filesystem packages.

### 2.2 UI (`internal/ui`)
- Bubble Tea `tea.Model` implementations.
- Converts user input into domain commands.
- Renders domain state.
- Must not embed SQL details; it can call application services.
- Follow Bubble Tea architecture: `model -> update -> view`.
- Follow instructions in `.github/instructions/ui.instructions.md` when implementing UI components.
- Follow instructions in `.github/instructions/html-css-style-color-guide.instructions.md` when selecting UI styles.

### 2.3 Store (`internal/store`)
- Define interfaces (ports) like:
  - `AppStateRepository`
  - `SettingsRepository`
- Implementation(s) (adapters) live under `internal/store/sqlite`.

### 2.4 App Wiring (`internal/app`)
- Creates DB connection, runs migrations, instantiates repositories/services, starts Bubble Tea program.

---

## 3) Clean Code + Idiomatic Go (Mandatory)

- Run `gofmt` on all Go files.
- Keep packages small and purpose-driven.
- Prefer:
  - Small interfaces (defined where used)
  - Constructor functions (`NewX(...)`) that validate inputs
  - Context-aware DB calls (`QueryContext`, `ExecContext`)
  - Error wrapping with `%w` and sentinel errors where helpful
- Avoid:
  - Global mutable state
  - Deeply nested conditionals (refactor)
  - “Manager/God objects”
  - Logging inside domain logic (return errors/events instead)

---

## 4) Design Patterns (When Appropriate) + Testing Requirements

### 4.1 Strategy Pattern
If any business logic is configurable/swappable (rulesets, scoring, AI behavior, difficulty, etc.):
- Define a `Strategy` interface in `internal/domain` (or appropriate domain package).
- Provide concrete implementations.
- Add unit tests for:
  - Strategy selection logic
  - Each strategy’s behavior (table-driven tests)
  - Edge cases and invariants

### 4.2 Repository Pattern
- Use repository interfaces in `internal/store`.
- The SQLite implementation must be tested (unit/integration style) using temp DB files.

---

## 5) TDD Workflow (Enforced)

Every feature/change must follow this cycle:

1. **Write tests first**
   - Place tests alongside code: `file_test.go`
   - Use table-driven tests where appropriate.
2. **Run tests and confirm they fail**
3. **Implement minimal code**
4. **Run tests until green**
5. **Refactor**
   - Keep behavior identical
   - Keep tests green
6. **Build and run via PowerShell script to confirm end-to-end functionality**

Definition of Done (for any PR/change):
- `go test ./...` passes
- `go build ./...` passes
- Migration/version logic tested when touched
- README updated if behavior/config/run steps changed
- PowerShell scripts still work

---

## 6) Testing Conventions

- Use Go’s standard `testing` package by default.
- Keep tests deterministic (no network, no real user input).
- For DB tests:
  - Use `t.TempDir()` to create a temp directory
  - Create a new SQLite DB file per test where needed
- Prefer explicit assertions; keep failure messages clear.

---

## 7) PowerShell Build Scripts (Required)

Create/maintain `scripts/build.ps1` as the canonical entrypoint.

It must support at least:
- `-Task test` → runs `go test ./...`
- `-Task build` → builds binary into `./bin/` (or documented output dir)
- `-Task run` → runs `cmd/appname` with optional args
- `-Task clean` → removes build artifacts

Rules:
- Scripts must set `CGO_ENABLED=0` during build/test to enforce the constraint.
- Scripts must be copy/paste runnable in PowerShell.
- README must document the exact commands.

---

## 8) README Sync Rules (Non-Negotiable)

`README.md` must include:
- What the game is (short)
- Requirements (Go version, terminal)
- How to run:
  - Via `go run ./cmd/appname`
  - Via PowerShell script
- Configuration:
  - DB path default and how to override
  - Any environment variables or flags
- Persistence notes:
  - “DB created on first launch”
  - “Schema migrates automatically when version differs”
- Developer workflow:
  - TDD expectation
  - How to run tests

Any change in behavior/config must be reflected in README in the same change set.

---

## 9) Dependency Rules

- Minimize dependencies; justify additions.
- Any dependency must be:
  - Maintained
  - Compatible with `CGO_ENABLED=0`
- If adding a dependency affects build/run/test steps, update:
  - `go.mod`
  - `scripts/build.ps1`
  - `README.md`

---

## 10) What to Do When Implementing a New Feature (Checklist)

1. Identify which layer(s) it belongs to: domain / store / ui / app.
2. Write failing tests:
   - Domain logic tests in `internal/domain`
   - Store tests in `internal/store/sqlite`
   - Migration tests in `internal/db`
3. Implement minimal code to pass.
4. Refactor for clarity.
5. Ensure:
   - No CGO dependency introduced
   - Migrations updated if schema changes
   - README updated if user-facing behavior changed
6. Run via PowerShell script and ensure it works.

