# Test script for Phase 6 fact tables
# Executes data quality tests on trip, dwell, stop classification, and corridor transit fact tables
# Uses Go-based test tool to avoid external dependencies

$ErrorActionPreference = "Stop"

# Build the Go tool first
Write-Host "Building Phase 6 test tool..." -ForegroundColor Cyan
$env:CGO_ENABLED = "0"
go build -o test_phase6_tool.exe test_phase6.go

if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to build the Go tool" -ForegroundColor Red
    exit 1
}

# Run the tool
.\test_phase6_tool.exe

# Clean up
Remove-Item test_phase6_tool.exe -ErrorAction SilentlyContinue

exit $LASTEXITCODE
