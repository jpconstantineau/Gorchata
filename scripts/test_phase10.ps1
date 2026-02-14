#!/usr/bin/env pwsh
# Test Phase 10: Final Integration Testing
# Runs comprehensive data quality tests to verify Phase 9 & 10 revisions

$ErrorActionPreference = "Stop"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Phase 10: Final Integration Testing" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Navigate to precision railroading example
$originalLocation = Get-Location
Push-Location "$PSScriptRoot\..\examples\precision_railroading"

try {
    Write-Host "Running comprehensive data quality tests..." -ForegroundColor Yellow
    Write-Host ""
    
    # Run all integration tests
    Write-Host "Executing: gorchata test" -ForegroundColor Gray
    Write-Host ""
    
    $startTime = Get-Date
    
    & gorchata test
    
    if ($LASTEXITCODE -ne 0) {
        throw "Phase 10 integration tests failed with exit code: $LASTEXITCODE"
    }
    
    $endTime = Get-Date
    $duration = $endTime - $startTime
    
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Green
    Write-Host "✓ All integration tests passed" -ForegroundColor Green
    Write-Host "  Execution time: $($duration.ToString('mm\:ss'))" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Green
    Write-Host ""
    
    # Verify test counts
    Write-Host "Verifying test coverage..." -ForegroundColor Yellow
    
    $testFiles = @(
        "tests\test_referential_integrity.sql",
        "tests\test_temporal_consistency.sql",
        "tests\test_business_rules.sql",
        "tests\test_exclusivity_constraints.sql",
        "tests\test_minute_precision.sql",
        "tests\dimensions\*.sql",
        "tests\staging\*.sql",
        "tests\intermediate\*.sql",
        "tests\facts\*.sql",
        "tests\metrics\*.sql",
        "tests\analytics\*.sql"
    )
    
    $totalTestFiles = 0
    foreach ($pattern in $testFiles) {
        $files = Get-ChildItem -Path $pattern -ErrorAction SilentlyContinue
        $totalTestFiles += $files.Count
    }
    
    Write-Host "  Test files: $totalTestFiles" -ForegroundColor Cyan
    Write-Host "  Expected: 223+ individual tests" -ForegroundColor Cyan
    Write-Host ""
    
    Write-Host "========================================" -ForegroundColor Green
    Write-Host "Phase 10 Complete: Integration Verified" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Green
    
} catch {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Red
    Write-Host "✗ Phase 10 tests failed" -ForegroundColor Red
    Write-Host "========================================" -ForegroundColor Red
    Write-Host ""
    Write-Host "Error: $_" -ForegroundColor Red
    Write-Host ""
    Write-Host "Troubleshooting steps:" -ForegroundColor Yellow
    Write-Host "  1. Check test output above for specific failures" -ForegroundColor Gray
    Write-Host "  2. Verify all models built: gorchata run" -ForegroundColor Gray
    Write-Host "  3. Check schema mismatches in test files" -ForegroundColor Gray
    Write-Host "  4. Review docs/SETUP.md for troubleshooting guide" -ForegroundColor Gray
    Write-Host ""
    
    # Return to original location before exiting
    Pop-Location
    exit 1
} finally {
    Pop-Location
}

Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "  • Review README.md for warehouse overview" -ForegroundColor Gray
Write-Host "  • Explore docs/QUERIES.md for example analyses" -ForegroundColor Gray
Write-Host "  • Study docs/PSR_EVOLUTION.md for business context" -ForegroundColor Gray
Write-Host ""
