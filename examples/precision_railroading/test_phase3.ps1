# Test script for Phase 3 staging layer
# Executes all data quality tests for staging models

$ErrorActionPreference = "Stop"

Write-Host "=== Phase 3 Staging Layer: Test Suite ===" -ForegroundColor Cyan
Write-Host ""

$dbPath = "target\precision_railroading.db"

# Check if database exists
if (!(Test-Path $dbPath)) {
    Write-Host "Database not found. Run build_phase3.ps1 first." -ForegroundColor Red
    exit 1
}

# Load System.Data.SQLite
try {
    $sqlitePaths = @(
        "$env:USERPROFILE\.nuget\packages\system.data.sqlite.core\*\lib\net46\System.Data.SQLite.dll",
        "C:\Program Files\WindowsPowerShell\Modules\PSSQLite\*\System.Data.SQLite.dll"
    )
    
    $foundPath = $null
    foreach ($pattern in $sqlitePaths) {
        $resolved = Get-Item $pattern -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($resolved) {
            $foundPath = $resolved.FullName
            break
        }
    }
    
    if (!$foundPath) {
        Write-Host "SQLite library not found." -ForegroundColor Red
        exit 1
    }
    
    Add-Type -Path $foundPath
} catch {
    Write-Host "Error loading SQLite: $_" -ForegroundColor Red
    exit 1
}

# Create connection
$connString = "Data Source=$dbPath;Version=3;"
$conn = New-Object System.Data.SQLite.SQLiteConnection($connString)
$conn.Open()
$cmd = $conn.CreateCommand()

# Test definitions
$tests = @(
    @{
        Name = "stg_clm_events: No duplicate event_ids"
        SQL = "SELECT COUNT(*) FROM (SELECT event_id, COUNT(*) as cnt FROM stg_clm_events GROUP BY event_id HAVING cnt > 1)"
    },
    @{
        Name = "stg_clm_events: Event types valid"
        SQL = "SELECT COUNT(*) FROM stg_clm_events WHERE event_type NOT IN ('DEPA', 'ARRI', 'PULL', 'PLAC')"
    },
    @{
        Name = "stg_clm_events: Timestamps not null"
        SQL = "SELECT COUNT(*) FROM stg_clm_events WHERE timestamp IS NULL"
    },
    @{
        Name = "stg_clm_events: Car numbers not null"
        SQL = "SELECT COUNT(*) FROM stg_clm_events WHERE car_number IS NULL"
    },
    @{
        Name = "stg_clm_enriched: All SPLC codes resolve"
        SQL = "SELECT COUNT(*) FROM stg_clm_enriched WHERE location_id IS NULL"
    },
    @{
        Name = "stg_clm_enriched: All car numbers resolve"
        SQL = "SELECT COUNT(*) FROM stg_clm_enriched WHERE railcar_id IS NULL"
    },
    @{
        Name = "stg_clm_enriched: All dates resolve"
        SQL = "SELECT COUNT(*) FROM stg_clm_enriched WHERE date_id IS NULL"
    },
    @{
        Name = "stg_clm_enriched: Loaded flag logic (PLAC)"
        SQL = "SELECT COUNT(*) FROM stg_clm_enriched WHERE event_type = 'PLAC' AND (is_loaded_event IS NULL OR is_loaded_event != 1)"
    },
    @{
        Name = "stg_clm_enriched: Loaded flag logic (PULL)"
        SQL = "SELECT COUNT(*) FROM stg_clm_enriched WHERE event_type = 'PULL' AND (is_loaded_event IS NULL OR is_loaded_event != 0)"
    },
    @{
        Name = "stg_clm_enriched: Movement flag logic (DEPA/ARRI)"
        SQL = "SELECT COUNT(*) FROM stg_clm_enriched WHERE event_type IN ('DEPA', 'ARRI') AND (is_movement_event IS NULL OR is_movement_event != 1)"
    },
    @{
        Name = "stg_clm_enriched: Movement flag logic (PLAC/PULL)"
        SQL = "SELECT COUNT(*) FROM stg_clm_enriched WHERE event_type IN ('PLAC', 'PULL') AND (is_movement_event IS NULL OR is_movement_event != 0)"
    },
    @{
        Name = "stg_clm_enriched: Event sequence temporal ordering"
        SQL = @"
SELECT COUNT(*) FROM (
    SELECT 
        car_number,
        timestamp,
        event_sequence,
        LAG(timestamp) OVER (PARTITION BY car_number ORDER BY event_sequence) AS prev_timestamp
    FROM stg_clm_enriched
) WHERE prev_timestamp IS NOT NULL AND timestamp < prev_timestamp
"@
    },
    @{
        Name = "stg_clm_enriched: Geographic coordinates valid (latitude)"
        SQL = "SELECT COUNT(*) FROM stg_clm_enriched WHERE latitude IS NOT NULL AND (latitude < -90 OR latitude > 90)"
    },
    @{
        Name = "stg_clm_enriched: Geographic coordinates valid (longitude)"
        SQL = "SELECT COUNT(*) FROM stg_clm_enriched WHERE longitude IS NOT NULL AND (longitude < -180 OR longitude > 180)"
    }
)

# Run tests
$passed = 0
$failed = 0

Write-Host "Running tests..." -ForegroundColor Cyan
Write-Host ""

foreach ($test in $tests) {
    Write-Host "  Testing: $($test.Name)..." -NoNewline
    
    try {
        $cmd.CommandText = $test.SQL
        $reader = $cmd.ExecuteReader()
        $reader.Read() | Out-Null
        $violationCount = $reader.GetInt32(0)
        $reader.Close()
        
        if ($violationCount -eq 0) {
            Write-Host " PASS" -ForegroundColor Green
            $passed++
        } else {
            Write-Host " FAIL ($violationCount violations)" -ForegroundColor Red
            $failed++
        }
    } catch {
        Write-Host " ERROR" -ForegroundColor Red
        Write-Host "    $($_.Exception.Message)" -ForegroundColor Red
        $failed++
    }
}

$conn.Close()

Write-Host ""
Write-Host "================================================" -ForegroundColor Cyan
Write-Host "Test Summary: $passed passed, $failed failed" -ForegroundColor $(if ($failed -eq 0) { "Green" } else { "Red" })
Write-Host "================================================" -ForegroundColor Cyan

if ($failed -eq 0) {
    Write-Host "SUCCESS: All tests passed!" -ForegroundColor Green
    exit 0
} else {
    Write-Host "FAILURE: $failed test(s) failed" -ForegroundColor Red
    exit 1
}
