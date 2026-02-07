# Build script test suite
# Tests the build.ps1 PowerShell script

param(
    [switch]$Verbose
)

$ErrorActionPreference = 'Stop'
$BuildScript = Join-Path $PSScriptRoot "build.ps1"
$ProjectRoot = Split-Path $PSScriptRoot -Parent
$BinDir = Join-Path $ProjectRoot "bin"
$BinaryPath = Join-Path $BinDir "gorchata.exe"

function Write-TestResult {
    param(
        [string]$TestName,
        [bool]$Passed,
        [string]$Message = ""
    )
    
    if ($Passed) {
        Write-Host "[PASS] $TestName" -ForegroundColor Green
    } else {
        Write-Host "[FAIL] $TestName" -ForegroundColor Red
        if ($Message) {
            Write-Host "       $Message" -ForegroundColor Red
        }
    }
    
    return $Passed
}

# Track test results
$TestsPassed = 0
$TestsFailed = 0

Write-Host "`n=== Build Script Test Suite ===" -ForegroundColor Cyan
Write-Host "Testing: $BuildScript`n" -ForegroundColor Cyan

# Test 1: build.ps1 exists
$TestName = "build.ps1 exists"
$Passed = Test-Path $BuildScript
if (Write-TestResult $TestName $Passed "Script not found at: $BuildScript") {
    $TestsPassed++
} else {
    $TestsFailed++
    exit 1
}

# Test 2: build.ps1 -Task test runs successfully
$TestName = "build.ps1 -Task test executes"
try {
    & $BuildScript -Task test 2>&1 | Out-Null
    $Passed = $LASTEXITCODE -eq 0
    if (Write-TestResult $TestName $Passed "Exit code: $LASTEXITCODE") {
        $TestsPassed++
    } else {
        $TestsFailed++
    }
} catch {
    Write-TestResult $TestName $false $_.Exception.Message
    $TestsFailed++
}

# Test 3: build.ps1 -Task build creates binary in bin/
$TestName = "build.ps1 -Task build creates bin/gorchata.exe"
try {
    # Clean first
    if (Test-Path $BinDir) {
        Remove-Item $BinDir -Recurse -Force
    }
    
    & $BuildScript -Task build 2>&1 | Out-Null
    $Passed = (Test-Path $BinaryPath) -and ($LASTEXITCODE -eq 0)
    
    if (Write-TestResult $TestName $Passed "Binary not found at: $BinaryPath") {
        $TestsPassed++
    } else {
        $TestsFailed++
    }
} catch {
    Write-TestResult $TestName $false $_.Exception.Message
    $TestsFailed++
}

# Test 4: build.ps1 -Task clean removes bin/
$TestName = "build.ps1 -Task clean removes bin/"
try {
    # Ensure bin/ exists first
    if (-not (Test-Path $BinDir)) {
        New-Item -ItemType Directory -Path $BinDir | Out-Null
    }
    
    & $BuildScript -Task clean 2>&1 | Out-Null
    $Passed = (-not (Test-Path $BinDir)) -and ($LASTEXITCODE -eq 0)
    
    if (Write-TestResult $TestName $Passed "bin/ directory still exists") {
        $TestsPassed++
    } else {
        $TestsFailed++
    }
} catch {
    Write-TestResult $TestName $false $_.Exception.Message
    $TestsFailed++
}

# Test 5: build.ps1 -Task run executes the binary
$TestName = "build.ps1 -Task run executes binary"
try {
    # Build first
    & $BuildScript -Task build 2>&1 | Out-Null
    
    # Run (we expect it to exit 0 for now)
    & $BuildScript -Task run 2>&1 | Out-Null
    $Passed = $LASTEXITCODE -eq 0
    
    if (Write-TestResult $TestName $Passed "Exit code: $LASTEXITCODE") {
        $TestsPassed++
    } else {
        $TestsFailed++
    }
} catch {
    Write-TestResult $TestName $false $_.Exception.Message
    $TestsFailed++
}

# Summary
Write-Host "`n=== Test Summary ===" -ForegroundColor Cyan
Write-Host "Passed: $TestsPassed" -ForegroundColor Green
Write-Host "Failed: $TestsFailed" -ForegroundColor Red

if ($TestsFailed -gt 0) {
    exit 1
} else {
    Write-Host "`nAll tests passed!" -ForegroundColor Green
    exit 0
}
