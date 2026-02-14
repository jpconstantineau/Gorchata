# Build script for Phase 7 metrics aggregations
# Builds network fluidity, slot adherence, shadow yards, buffer consumption,
# directional asymmetry, corridor weekly performance, and PSR evolution tables
# Uses Go-based build tool to avoid external dependencies

$ErrorActionPreference = "Stop"

# Build the Go tool first
Write-Host "Building Phase 7 build tool..." -ForegroundColor Cyan
$env:CGO_ENABLED = "0"
go build -o build_phase7_tool.exe build_phase7.go

if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to build the Go tool" -ForegroundColor Red
    exit 1
}

# Run the tool
.\build_phase7_tool.exe

# Clean up
Remove-Item build_phase7_tool.exe -ErrorAction SilentlyContinue

exit $LASTEXITCODE
