# Build script for Phase 8 analytics queries
# Builds analytics queries demonstrating data warehouse capabilities
# Uses Go-based build tool to avoid external dependencies

$ErrorActionPreference = "Stop"

# Build the Go tool first
Write-Host "Building Phase 8 build tool..." -ForegroundColor Cyan
$env:CGO_ENABLED = "0"
go build -o build_phase8_tool.exe scripts\build_phase8\main.go

if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to build the Go tool" -ForegroundColor Red
    exit 1
}

# Run the tool
.\build_phase8_tool.exe

# Clean up
Remove-Item build_phase8_tool.exe -ErrorAction SilentlyContinue

exit $LASTEXITCODE
