# Build script for Phase 3 staging layer
# Builds staging models on top of dimensions from Phase 2

$ErrorActionPreference = "Stop"

Write-Host "=== Precision Railroading Phase 3: Staging Layer Build ===" -ForegroundColor Cyan
Write-Host ""

$dbPath = "target\precision_railroading.db"

# Check if database exists
if (!(Test-Path $dbPath)) {
    Write-Host "Database not found. Run build_phase2.ps1 first to create dimensions." -ForegroundColor Red
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
    
    if ($foundPath) {
        Add-Type -Path $foundPath
    } else {
        Write-Host "SQLite library not found. Install PSSQLite module or System.Data.SQLite NuGet package." -ForegroundColor Red
        exit 1
    }
} catch {
    Write-Host "Error loading SQLite: $_" -ForegroundColor Red
    exit 1
}

# Create connection
$connString = "Data Source=$dbPath;Version=3;"
$conn = New-Object System.Data.SQLite.SQLiteConnection($connString)
$conn.Open()

# Drop existing staging tables if they exist
Write-Host "Dropping existing staging tables..." -ForegroundColor Yellow
$cmd = $conn.CreateCommand()
$cmd.CommandText = "DROP TABLE IF EXISTS stg_clm_enriched"
$cmd.ExecuteNonQuery() | Out-Null
$cmd.CommandText = "DROP TABLE IF EXISTS stg_clm_events"
$cmd.ExecuteNonQuery() | Out-Null

# Execute staging models in dependency order
$models = @(
    @{Name="stg_clm_events"; Path="models\staging\stg_clm_events.sql"},
    @{Name="stg_clm_enriched"; Path="models\staging\stg_clm_enriched.sql"}
)

Write-Host "Building staging models..." -ForegroundColor Cyan

foreach ($model in $models) {
    Write-Host "  Building $($model.Name)..." -NoNewline
    
    # Read SQL file
    $sql = Get-Content $model.Path -Raw
    
    # Simple Jinja2 template processing
    $sql = $sql -replace '\{\{\s*config\s*"materialized"\s*"(table|view)"\s*\}\}', ''
    $sql = $sql -replace '\{\{\s*seed\s*"raw_clm_events"\s*\}\}', 'raw_clm_events'
    $sql = $sql -replace '\{\{\s*ref\s*"stg_clm_events"\s*\}\}', 'stg_clm_events'
    $sql = $sql -replace '\{\{\s*ref\s*"dim_location"\s*\}\}', 'dim_location'
    $sql = $sql -replace '\{\{\s*ref\s*"dim_railcar"\s*\}\}', 'dim_railcar'
    $sql = $sql -replace '\{\{\s*ref\s*"dim_train"\s*\}\}', 'dim_train'
    $sql = $sql -replace '\{\{\s*ref\s*"dim_date"\s*\}\}', 'dim_date'
    
    # Execute as CREATE TABLE AS
    $createSql = "CREATE TABLE $($model.Name) AS`n$sql"
    
    try {
        $cmd.CommandText = $createSql
        $cmd.ExecuteNonQuery() | Out-Null
        Write-Host " OK" -ForegroundColor Green
    } catch {
        Write-Host " FAILED" -ForegroundColor Red
        Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
        $conn.Close()
        exit 1
    }
}

# Verify row counts
Write-Host "`nVerifying staging tables..." -ForegroundColor Cyan

$cmd.CommandText = "SELECT COUNT(*) FROM stg_clm_events"
$reader = $cmd.ExecuteReader()
$reader.Read() | Out-Null
$stgEventsCount = $reader.GetInt32(0)
$reader.Close()
Write-Host "  stg_clm_events: $stgEventsCount rows" -ForegroundColor Green

$cmd.CommandText = "SELECT COUNT(*) FROM stg_clm_enriched"
$reader = $cmd.ExecuteReader()
$reader.Read() | Out-Null
$stgEnrichedCount = $reader.GetInt32(0)
$reader.Close()
Write-Host "  stg_clm_enriched: $stgEnrichedCount rows" -ForegroundColor Green

# Verify dimension joins
Write-Host "`nVerifying dimension joins..." -ForegroundColor Cyan

$cmd.CommandText = @"
SELECT 
    COUNT(*) as total,
    SUM(CASE WHEN location_id IS NOT NULL THEN 1 ELSE 0 END) as has_location,
    SUM(CASE WHEN railcar_id IS NOT NULL THEN 1 ELSE 0 END) as has_railcar,
    SUM(CASE WHEN date_id IS NOT NULL THEN 1 ELSE 0 END) as has_date
FROM stg_clm_enriched
"@
$reader = $cmd.ExecuteReader()
$reader.Read() | Out-Null
$total = $reader.GetInt32(0)
$hasLocation = $reader.GetInt32(1)
$hasRailcar = $reader.GetInt32(2)
$hasDate = $reader.GetInt32(3)
$reader.Close()

Write-Host "  Location joins: $hasLocation/$total" -ForegroundColor $(if ($hasLocation -eq $total) { "Green" } else { "Yellow" })
Write-Host "  Railcar joins: $hasRailcar/$total" -ForegroundColor $(if ($hasRailcar -eq $total) { "Green" } else { "Yellow" })
Write-Host "  Date joins: $hasDate/$total" -ForegroundColor $(if ($hasDate -eq $total) { "Green" } else { "Yellow" })

$conn.Close()

Write-Host "`nPhase 3 build complete!" -ForegroundColor Green
Write-Host "Run test_phase3.ps1 to execute data quality tests." -ForegroundColor Cyan
