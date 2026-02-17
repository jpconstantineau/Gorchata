# Test script for Phase 8 analytics queries
# Executes data quality tests on analytics query results
# Uses Go-based test tool to avoid external dependencies

$ErrorActionPreference = "Stop"

# Build the Go tool first
Write-Host "Building Phase 8 test tool..." -ForegroundColor Cyan
$env:CGO_ENABLED = "0"
go build -o test_phase8_tool.exe scripts\test_phase8\main.go

if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to build the Go tool" -ForegroundColor Red
    exit 1
}

# Run the tool
.\test_phase8_tool.exe

# Clean up
Remove-Item test_phase8_tool.exe -ErrorAction SilentlyContinue

exit $LASTEXITCODE
