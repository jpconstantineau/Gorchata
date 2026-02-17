# Test script for Phase 7 metrics aggregations
# Executes data quality tests on metrics and aggregations tables
# Uses Go-based test tool to avoid external dependencies

$ErrorActionPreference = "Stop"

# Build the Go tool first
Write-Host "Building Phase 7 test tool..." -ForegroundColor Cyan
$env:CGO_ENABLED = "0"
go build -o test_phase7_tool.exe scripts\test_phase7\main.go

if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to build the Go tool" -ForegroundColor Red
    exit 1
}

# Run the tool
.\test_phase7_tool.exe

# Clean up
Remove-Item test_phase7_tool.exe -ErrorAction SilentlyContinue

exit $LASTEXITCODE
