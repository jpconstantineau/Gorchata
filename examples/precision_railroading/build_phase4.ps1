# Build script for Phase 4 intermediate layer
# Builds intermediate models on top of staging from Phase 3
# Uses Go-based build tool to avoid external dependencies

$ErrorActionPreference = "Stop"

# Build the Go tool first
Write-Host "Building Phase 4 build tool..." -ForegroundColor Cyan
$env:CGO_ENABLED = "0"
go build -o build_phase4_tool.exe scripts\build_phase4\main.go

if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to build the Go tool" -ForegroundColor Red
    exit 1
}

# Run the tool
.\build_phase4_tool.exe

# Clean up
Remove-Item build_phase4_tool.exe -ErrorAction SilentlyContinue

exit $LASTEXITCODE
