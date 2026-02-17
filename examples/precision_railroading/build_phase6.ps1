# Build script for Phase 6 fact tables
# Builds trip, dwell, stop classification, and corridor transit fact tables
# Uses Go-based build tool to avoid external dependencies

$ErrorActionPreference = "Stop"

# Build the Go tool first
Write-Host "Building Phase 6 build tool..." -ForegroundColor Cyan
$env:CGO_ENABLED = "0"
go build -o build_phase6_tool.exe scripts\build_phase6\main.go

if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to build the Go tool" -ForegroundColor Red
    exit 1
}

# Run the tool
.\build_phase6_tool.exe

# Clean up
Remove-Item build_phase6_tool.exe -ErrorAction SilentlyContinue

exit $LASTEXITCODE
