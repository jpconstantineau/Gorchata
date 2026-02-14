#!/usr/bin/env pwsh
# Test Script for Phase 9: Data Quality Tests & Validation
# Wrapper script that invokes build_phase9.ps1

$ErrorActionPreference = "Stop"

# Execute build script (which includes all tests)
& "$PSScriptRoot\build_phase9.ps1"

exit $LASTEXITCODE
