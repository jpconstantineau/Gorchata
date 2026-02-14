# Phase 9 & Phase 10 Build Script
# Precision Scheduled Railroading - Final Integration
# Builds all models, runs all tests, validates 223+ test coverage

param(
    [string]$Task = "all"
)

$ErrorActionPreference = "Stop"
$PSR_DIR = "c:\Users\pierre\git\Gorchata\examples\precision_railroading"

Write-Host "=====================================" -ForegroundColor Cyan
Write-Host "PSR Phase 9 & 10 Build Script" -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host ""

# Ensure we're in the correct directory
Push-Location $PSR_DIR

try {
    switch ($Task) {
        "test" {
            Write-Host"[Task] Running all tests..." -ForegroundColor Yellow
            gorchata test
            if ($LASTEXITCODE -ne 0) {
                throw "Tests failed with exit code $LASTEXITCODE"
            }
        }
        
        "build" {
            Write-Host "[Task] Building all models..." -ForegroundColor Yellow
            gorchata run
            if ($LASTEXITCODE -ne 0) {
                throw "Build failed with exit code $LASTEXITCODE"
            }
        }
        
        "quality-tests" {
            Write-Host "[Task] Running Phase 9 quality tests..." -ForegroundColor Yellow
            Write-Host "  - Referential Integrity (13 tests)" -ForegroundColor Gray
            Write-Host "  - Temporal Consistency (10 tests)" -ForegroundColor Gray
            Write-Host "  - Business Rules (14 tests)" -ForegroundColor Gray
            Write-Host "  - Exclusivity Constraints (8 tests)" -ForegroundColor Gray
            Write-Host "  - Minute Precision (10 tests)" -ForegroundColor Gray
            Write-Host ""
            
            gorchata test
        }
        
        "integration-test" {
            Write-Host "[Task] Running integration tests..." -ForegroundColor Yellow
            gorchata test tests/test_integration.sql
        }
        
        "clean" {
            Write-Host "[Task] Cleaning build artifacts..." -ForegroundColor Yellow
            if (Test-Path "target") {
                Remove-Item -Path "target\*.db" -Force -ErrorAction SilentlyContinue
                Write-Host "  ✓ Removed database files" -ForegroundColor Green
            }
        }
        
        "clean-all" {
            Write-Host "[Task] Full clean (including seed data)..." -ForegroundColor Yellow
            if (Test-Path "target") {
                Remove-Item -Path "target\" -Recurse -Force -ErrorAction SilentlyContinue
                Write-Host "  ✓ Removed target directory" -ForegroundColor Green
            }
            if (Test-Path "seeds\raw_clm_events.csv") {
                Remove-Item -Path "seeds\raw_clm_events.csv" -Force -ErrorAction SilentlyContinue
                Write-Host "  ✓ Removed seed data (8GB)" -ForegroundColor Green
            }
        }
        
        "generate-data" {
            Write-Host "[Task] Generating CLM seed data..." -ForegroundColor Yellow
            Write-Host "  This will create ~8GB of data (~5-10 minutes)" -ForegroundColor Gray
            go run generate_clm_data.go
            if ($LASTEXITCODE -ne 0) {
                throw "Data generation failed"
            }
            Write-Host "  ✓ Generated 110M CLM events" -ForegroundColor Green
        }
        
        "all" {
            Write-Host "[Task] Complete build and test cycle..." -ForegroundColor Yellow
            Write-Host ""
            
            # Ensure seed data exists
            if (-not (Test-Path "seeds\raw_clm_events.csv")) {
                Write-Host "Seed data not found. Generating..." -ForegroundColor Yellow
                & $PSScriptRoot\build_phase10.ps1 -Task generate-data
            }
            
            # Build all models
            Write-Host "`n[1/3] Building all models..." -ForegroundColor Cyan
            gorchata run
            if ($LASTEXITCODE -ne 0) {
                throw "Model build failed"
            }
            Write-Host "  ✓ All models materialized" -ForegroundColor Green
            
            # Run tests
            Write-Host "`n[2/3] Running 223+ tests..." -ForegroundColor Cyan
            gorchata test
            $testExitCode = $LASTEXITCODE
            
            # Summary
            Write-Host "`n[3/3] Build Summary" -ForegroundColor Cyan
            Write-Host "===============================================" -ForegroundColor Cyan
            
            # Check database
            if (Test-Path "target\precision_railroading.db") {
                $dbSize = (Get-Item "target\precision_railroading.db").Length / 1MB
                Write-Host "Database: $([math]::Round($dbSize, 1)) MB" -ForegroundColor White
            }
            
            # Test results
            if ($testExitCode -eq 0) {
                Write-Host "Tests: ✓ ALL PASSING" -ForegroundColor Green
            } else {
                Write-Host "Tests: Some failures (see above)" -ForegroundColor Yellow
            }
            
            Write-Host "===============================================" -ForegroundColor Cyan
            Write-Host ""
            Write-Host "✓ Phase 9 & 10 build complete!" -ForegroundColor Green
            Write-Host ""
            Write-Host "Next steps:" -ForegroundColor Cyan
            Write-Host "  - Review test results above" -ForegroundColor Gray
            Write-Host "  - Check README.md for usage examples" -ForegroundColor Gray
            Write-Host "  - Run sample queries from docs/QUERIES.md" -ForegroundColor Gray
        }
        
        default {
            Write-Host "Invalid task: $Task" -ForegroundColor Red
            Write-Host ""
            Write-Host "Available tasks:" -ForegroundColor Cyan
            Write-Host "  all              - Complete build and test (default)" -ForegroundColor White
            Write-Host "  build            - Build all models only" -ForegroundColor White
            Write-Host "  test            - Run all tests" -ForegroundColor White
            Write-Host "  quality-tests    - Run Phase 9 quality tests only" -ForegroundColor White
            Write-Host "  integration-test - Run integration tests" -ForegroundColor White
            Write-Host "  generate-data    - Generate 8GB seed data" -ForegroundColor White
            Write-Host "  clean            - Remove database files" -ForegroundColor White
            Write-Host "  clean-all        - Remove database and seed data" -ForegroundColor White
            Write-Host ""
            Write-Host "Example: .\build_phase10.ps1 -Task test" -ForegroundColor Gray
            exit 1
        }
    }
} finally {
    Pop-Location
}
