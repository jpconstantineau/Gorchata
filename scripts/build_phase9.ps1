#!/usr/bin/env pwsh
# Build and Test Script for Phase 9: Data Quality Tests & Validation
# Runs comprehensive data quality test suite validating referential integrity,
# temporal consistency, business rules, exclusivity constraints, and minute precision

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Phase 9: Data Quality Tests & Validation" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

$startTime = Get-Date

# Navigate to precision_railroading directory
$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location "$projectRoot\examples\precision_railroading"

try {
    Write-Host "Running data quality test suite..." -ForegroundColor Yellow
    Write-Host "  58 tests across 5 test files" -ForegroundColor Gray
    Write-Host ""
    
    # Run gorchata test command
    & gorchata test --profiles-dir .
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host ""
        Write-Host "❌ Phase 9 tests failed" -ForegroundColor Red
        Write-Host "   Review test output above for failure details" -ForegroundColor Red
        exit 1
    }
    
    Write-Host ""
    Write-Host "✓ All data quality tests passed" -ForegroundColor Green
    Write-Host ""
    Write-Host "Test Coverage:" -ForegroundColor Cyan
    Write-Host "  ✓ Referential Integrity (15 tests)" -ForegroundColor Green
    Write-Host "  ✓ Temporal Consistency (10 tests)" -ForegroundColor Green
    Write-Host "  ✓ Business Rules (15 tests)" -ForegroundColor Green
    Write-Host "  ✓ Exclusivity Constraints (8 tests)" -ForegroundColor Green
    Write-Host "  ✓ Minute Precision (10 tests)" -ForegroundColor Green
    Write-Host ""
    
    $endTime = Get-Date
    $duration = $endTime - $startTime
    Write-Host "Phase 9 completed successfully in $($duration.TotalSeconds) seconds" -ForegroundColor Green
    
} finally {
    Pop-Location
}
