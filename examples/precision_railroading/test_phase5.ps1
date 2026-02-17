# Test script for Phase 5 velocity and dwell analysis
# Executes data quality tests on velocity vectors, nodal dwell, and dwell classification
# Uses Go-based test tool to avoid external dependencies

$ErrorActionPreference = "Stop"

# Build the Go tool first
Write-Host "Building Phase 5 test tool..." -ForegroundColor Cyan
$env:CGO_ENABLED = "0"
go build -o test_phase5_tool.exe scripts\test_phase5\main.go

if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to build the Go tool" -ForegroundColor Red
    exit 1
}

# Run the tool
.\test_phase5_tool.exe

# Clean up
Remove-Item test_phase5_tool.exe -ErrorAction SilentlyContinue

exit $LASTEXITCODE
