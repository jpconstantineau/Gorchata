## Plan: Documentation Reorganization and GitHub Path Updates

Reorganize README.md into logical sections for users vs. developers, consider splitting into separate documents, and update all GitHub paths from `pierre` to `jpconstantineau`.

**Phases: 5**

### Phase 1: Analyze and Design Documentation Structure
- **Objective:** Determine optimal documentation structure - single file with better organization vs. multiple files (README.md, BUILDING.md, CONTRIBUTING.md)
- **Files/Functions to Analyze:**
  - [README.md](README.md) - current monolithic structure
  - existing [plans/](plans/) directory for reference
- **Tests to Write:** N/A (analysis phase)
- **Steps:**
  1. Identify distinct audiences: end users vs. developers vs. contributors
  2. Categorize current README sections by audience
  3. Design new documentation structure:
     - **Option A**: Keep single README with clearer section hierarchy
     - **Option B**: Split into README.md (users), BUILDING.md (developers), CONTRIBUTING.md (contributors)
  4. Create outline for each document showing section ordering
  5. Present recommendation to user

### Phase 2: Update GitHub Module Path in go.mod
- **Objective:** Update the module declaration and ensure all dependencies remain intact
- **Files/Functions to Modify/Create:**
  - [go.mod](go.mod) - line 1 module declaration
- **Tests to Write:**
  1. Test that `go mod tidy` succeeds after change
  2. Test that `go build ./...` succeeds
  3. Test that `go test ./...` passes
- **Steps:**
  1. Write test script that validates go.mod correctness
  2. Run test to confirm current state (should pass)
  3. Update module path from `github.com/pierre/gorchata` to `github.com/jpconstantineau/gorchata`
  4. Run `go mod tidy`
  5. Run tests to verify nothing broke
  6. Run build to verify compilation succeeds

### Phase 3: Update GitHub Import Paths in Go Source Files
- **Objective:** Update all import statements in Go source files to use new GitHub username
- **Files/Functions to Modify/Create:**
  - [cmd/gorchata/main.go](cmd/gorchata/main.go)
  - [internal/app/app.go](internal/app/app.go)
  - [internal/cli/run.go](internal/cli/run.go)
  - [internal/cli/compile.go](internal/cli/compile.go)
  - [internal/domain/executor/engine.go](internal/domain/executor/engine.go)
  - [internal/domain/executor/engine_test.go](internal/domain/executor/engine_test.go)
  - [internal/domain/executor/model.go](internal/domain/executor/model.go)
  - [internal/domain/executor/model_test.go](internal/domain/executor/model_test.go)
  - [internal/platform/sqlite/adapter.go](internal/platform/sqlite/adapter.go)
  - [internal/platform/sqlite/adapter_test.go](internal/platform/sqlite/adapter_test.go)
  - [internal/platform/sqlite/integration_test.go](internal/platform/sqlite/integration_test.go)
  - [test/integration_test.go](test/integration_test.go)
  - All other files with imports (approximately 69 total references)
- **Tests to Write:**
  1. Test that all existing unit tests still pass after import changes
  2. Test that integration tests pass
  3. Test that build succeeds with new imports
- **Steps:**
  1. Run existing tests to establish baseline (should all pass)
  2. Use multi-replace to update all import statements from `github.com/pierre/gorchata` to `github.com/jpconstantineau/gorchata`
  3. Run `go mod tidy` to update dependencies
  4. Run all tests to verify correctness
  5. Run build to verify compilation
  6. Run integration tests to verify end-to-end functionality

### Phase 4: Update GitHub URLs in Documentation
- **Objective:** Update all GitHub URLs in README and other documentation files
- **Files/Functions to Modify/Create:**
  - [README.md](README.md) - 4 references to github.com/jpconstantineau/gorchata
  - Any other markdown files in [plans/](plans/) if they contain old URLs
- **Tests to Write:**
  1. Manual verification test that all links work (can be scripted)
  2. Grep test to ensure no old references remain
- **Steps:**
  1. Update installation instructions: `go install github.com/jpconstantineau/gorchata/cmd/gorchata@latest`
  2. Update clone URL: `git clone https://github.com/jpconstantineau/gorchata.git`
  3. Update Issues URL: `https://github.com/jpconstantineau/gorchata/issues`
  4. Update Discussions URL: `https://github.com/jpconstantineau/gorchata/discussions`
  5. Run grep search to verify no instances of `github.com/jpconstantineau/gorchata` remain
  6. Build and test to verify documentation references don't affect functionality

### Phase 5: Reorganize README Documentation Structure
- **Objective:** Implement the approved documentation structure from Phase 1
- **Files/Functions to Modify/Create:**
  - [README.md](README.md) - reorganized structure
  - [BUILDING.md](BUILDING.md) - potentially new file for development/build instructions
  - [CONTRIBUTING.md](CONTRIBUTING.md) - potentially new file for contribution guidelines
- **Tests to Write:**
  1. Checklist verification that all original content is preserved
  2. Readability test (peer review)
  3. Verify all internal document links work
- **Steps:**
  1. Based on Phase 1 design, create new document structure
  2. If splitting files:
     - Extract user-focused content to polished README.md
     - Extract build/development content to BUILDING.md
     - Extract contribution guidelines to CONTRIBUTING.md
     - Add cross-references between documents
  3. If reorganizing single file:
     - Restructure sections with clear hierarchy
     - Add table of contents
     - Improve section progression (user journey)
  4. Verify all content from original README is preserved
  5. Update any references in other files to point to new document locations
  6. Review for clarity and completeness

**Open Questions**
1. **Documentation Structure:** Should we split into multiple files (README.md for users, BUILDING.md for developers, CONTRIBUTING.md) or keep a single well-organized README.md? **Recommendation: Split into 3 files** for better separation of concerns.
2. **README Focus:** If splitting, should README focus on: (A) End-user quick start and usage, or (B) Project overview with links to detailed docs? **Recommendation: Option A** - users should be able to get started quickly.
3. **Old Content:** Should we preserve exact wording or can we improve clarity while reorganizing? **Recommendation: Improve clarity** while preserving all information.
4. **Link Validation:** Should we add a script to validate GitHub URLs in documentation? **Recommendation: Yes, as a future enhancement** but not blocking this plan.
