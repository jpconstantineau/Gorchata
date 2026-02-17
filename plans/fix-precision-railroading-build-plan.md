## Plan: Fix Precision Railroading Multiple Main Package Build Issues

Reorganize precision_railroading example utility scripts into individual subdirectories so each has its own buildable package, allowing GitHub Actions CI to succeed while keeping scripts easily runnable.

**Phases: 3**

### **Phase 1: Create Scripts Directory Structure**
- **Objective:** Set up proper directory structure for all utility scripts to separate them into individual Go packages
- **Files/Directories to Create:**
  - `examples/precision_railroading/scripts/build_phase4/` through `scripts/build_phase8/`
  - `examples/precision_railroading/scripts/test_phase4/` through `scripts/test_phase8/`
  - `examples/precision_railroading/scripts/check_psr_data/`
  - `examples/precision_railroading/scripts/debug_patterns/`, `scripts/debug_phase6/`, `scripts/debug_trips/`, `scripts/debug_velocity/`
  - `examples/precision_railroading/scripts/verify_phase3/`
  - `examples/precision_railroading/scripts/generate_clm_data/`
- **Tests to Write:** None (directory creation only)
- **Steps:**
  1. Create `examples/precision_railroading/scripts/` base directory
  2. Create subdirectory for each of the 18 utility scripts
  3. Verify directory structure is created correctly

### **Phase 2: Move Scripts and Update PowerShell References**
- **Objective:** Move each `.go` file into its own package directory and update all PowerShell scripts that reference them
- **Files/Functions to Modify/Create:**
  - Move 18 `.go` files from `examples/precision_railroading/*.go` to `examples/precision_railroading/scripts/<name>/main.go`
  - Update `build_phase4.ps1`, `build_phase5.ps1`, `build_phase6.ps1`, `build_phase7.ps1`, `build_phase8.ps1`
  - Update `test_phase4.ps1`, `test_phase5.ps1`, `test_phase6.ps1`, `test_phase7.ps1`, `test_phase8.ps1`
  - Update any other PowerShell scripts that reference the moved Go files
- **Tests to Write:** None (file moves and script updates)
- **Steps:**
  1. Move each `.go` file to `scripts/<script_name>/main.go`
  2. Update all `.ps1` scripts to use new paths (e.g., `go run build_phase4.go` → `go run scripts/build_phase4/main.go`)
  3. Verify all PowerShell scripts reference correct paths

### **Phase 3: Verify Build and Update Documentation**
- **Objective:** Ensure CI passes with new structure and update README to reflect changes
- **Files/Functions to Modify/Create:**
  - `examples/precision_railroading/README.md` - Update script usage instructions
  - Verify `go build ./...` succeeds locally
  - Verify `go test ./...` succeeds locally
- **Tests to Write:** None (verification only)
- **Steps:**
  1. Run `go build -v ./...` from workspace root to verify no "multiple main packages" error
  2. Run `go test -v ./...` to ensure no test breakage
  3. Update README sections that reference script execution with correct paths
  4. Document new directory structure in README
