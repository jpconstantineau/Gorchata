# Build automation script for Gorchata
# Supports: test, build, run, clean tasks
# Enforces CGO_ENABLED=0 for all operations

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet('test', 'build', 'run', 'clean')]
    [string]$Task,
    
    [Parameter(ValueFromRemainingArguments=$true)]
    [string[]]$RemainingArgs
)

$ErrorActionPreference = 'Stop'
$ProjectRoot = Split-Path $PSScriptRoot -Parent
$BinDir = Join-Path $ProjectRoot "bin"
$BinaryName = "gorchata.exe"
$BinaryPath = Join-Path $BinDir $BinaryName

# Ensure CGO is disabled
$env:CGO_ENABLED = "0"

function Invoke-Test {
    Write-Host "Running tests..." -ForegroundColor Cyan
    Push-Location $ProjectRoot
    try {
        go test ./...
        if ($LASTEXITCODE -ne 0) {
            throw "Tests failed"
        }
        Write-Host "Tests passed!" -ForegroundColor Green
    } finally {
        Pop-Location
    }
}

function Invoke-Build {
    Write-Host "Building Gorchata..." -ForegroundColor Cyan
    
    # Create bin directory if it doesn't exist
    if (-not (Test-Path $BinDir)) {
        New-Item -ItemType Directory -Path $BinDir | Out-Null
    }
    
    Push-Location $ProjectRoot
    try {
        $buildCmd = "go build -o `"$BinaryPath`" ./cmd/gorchata"
        Write-Host "Executing: $buildCmd" -ForegroundColor Gray
        
        Invoke-Expression $buildCmd
        
        if ($LASTEXITCODE -ne 0) {
            throw "Build failed"
        }
        
        if (Test-Path $BinaryPath) {
            Write-Host "Build successful: $BinaryPath" -ForegroundColor Green
        } else {
            throw "Build succeeded but binary not found at: $BinaryPath"
        }
    } finally {
        Pop-Location
    }
}

function Invoke-Run {
    Write-Host "Running Gorchata..." -ForegroundColor Cyan
    
    # Build first if binary doesn't exist
    if (-not (Test-Path $BinaryPath)) {
        Write-Host "Binary not found, building first..." -ForegroundColor Yellow
        Invoke-Build
    }
    
    # Run the binary with any additional arguments
    if ($RemainingArgs) {
        & $BinaryPath @RemainingArgs
    } else {
        & $BinaryPath
    }
    
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

function Invoke-Clean {
    Write-Host "Cleaning build artifacts..." -ForegroundColor Cyan
    
    if (Test-Path $BinDir) {
        Remove-Item $BinDir -Recurse -Force
        Write-Host "Removed: $BinDir" -ForegroundColor Green
    } else {
        Write-Host "Nothing to clean (bin/ doesn't exist)" -ForegroundColor Gray
    }
}

# Execute the requested task
switch ($Task) {
    'test' {
        Invoke-Test
    }
    'build' {
        Invoke-Build
    }
    'run' {
        Invoke-Run
    }
    'clean' {
        Invoke-Clean
    }
}

exit 0
