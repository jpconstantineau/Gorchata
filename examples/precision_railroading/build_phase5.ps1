# Build script for Phase 5 velocity and dwell analysis
# Builds velocity vectors, nodal dwell, and dwell classification models
# Uses Go-based build tool to avoid external dependencies

$ErrorActionPreference = "Stop"

# Build the Go tool first
Write-Host "Building Phase 5 build tool..." -ForegroundColor Cyan
$env:CGO_ENABLED = "0"
go build -o build_phase5_tool.exe build_phase5.go

if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to build the Go tool" -ForegroundColor Red
    exit 1
}

# Run the tool
.\build_phase5_tool.exe

# Clean up
Remove-Item build_phase5_tool.exe -ErrorAction SilentlyContinue

exit $LASTEXITCODE
