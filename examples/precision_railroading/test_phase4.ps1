# Test script for Phase 4 intermediate layer
# Executes data quality tests on intermediate models
# Uses Go-based test tool to avoid external dependencies

$ErrorActionPreference = "Stop"

# Build the Go tool first
Write-Host "Building Phase 4 test tool..." -ForegroundColor Cyan
$env:CGO_ENABLED = "0"
go build -o test_phase4_tool.exe test_phase4.go

if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to build the Go tool" -ForegroundColor Red
    exit 1
}

# Run the tool
.\test_phase4_tool.exe

# Clean up
Remove-Item test_phase4_tool.exe -ErrorAction SilentlyContinue

exit $LASTEXITCODE
