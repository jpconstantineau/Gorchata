# Manual model execution script for precision_railroading Phase 2
# Workaround for memory issues with large CSV seed loading

$ErrorActionPreference = "Stop"

Write-Host "=== Precision Railroading Phase 2: Manual Build Script ===" -ForegroundColor Cyan
Write-Host ""

# Create database if it doesn't exist
$dbPath = "target\precision_railroading.db"
$seedPath = "seeds\raw_clm_events.csv"

# Ensure target directory exists
if (!(Test-Path "target")) {
    New-Item -ItemType Directory -Path "target" | Out-Null
}

# Remove old database
if (Test-Path $dbPath) {
    Remove-Item $dbPath
    Write-Host "Removed old database"
}

Write-Host "Creating new database: $dbPath"

# Load System.Data.SQLite
Add-Type -Path "C:\Program Files\WindowsPowerShell\Modules\PSSQLite\1.1.0\System.Data.SQLite.dll" -ErrorAction SilentlyContinue 2>$null

# Create connection
$connString = "Data Source=$dbPath;Version=3;"
$conn = New-Object System.Data.SQLite.SQLiteConnection($connString)
$conn.Open()

Write-Host "Loading seed data from: $seedPath"
Write-Host "This may take a few minutes..."

# Create seed table
$cmd = $conn.CreateCommand()
$cmd.CommandText = @"
CREATE TABLE raw_clm_events (
    event_id INTEGER,
    car_number TEXT,
    timestamp TEXT,
    event_type TEXT,
    splc_code TEXT,
    train_id TEXT,
    location_name TEXT
)
"@
$cmd.ExecuteNonQuery() | Out-Null

# Load CSV in batches
$batch = @()
$batchSize = 1000
$total = 0

Get-Content $seedPath | Select-Object -Skip 1 | ForEach-Object {
    $fields = $_ -split ','
    if ($fields.Count -ge 7) {
        $batch += "($(($fields | ForEach-Object { "'$($_ -replace "'","''")'" }) -join ','))"
        
        if ($batch.Count -ge $batchSize) {
            $insertSql = "INSERT INTO raw_clm_events VALUES " + ($batch -join ',')
            $cmd.CommandText = $insertSql
            $cmd.ExecuteNonQuery() | Out-Null
            $total += $batch.Count
            $batch = @()
            if ($total % 5000 -eq 0) {
                Write-Host "  Loaded $total rows..." -NoNewline -ForegroundColor Gray
                Write-Host "`r" -NoNewline
            }
        }
    }
}

# Insert remaining batch
if ($batch.Count -gt 0) {
    $insertSql = "INSERT INTO raw_clm_events VALUES " + ($batch -join ',')
    $cmd.CommandText = $insertSql
    $cmd.ExecuteNonQuery() | Out-Null
    $total += $batch.Count
}

Write-Host "Loaded $total seed rows                    " -ForegroundColor Green

# Execute dimension models in dependency order
$models = @(
    @{Name="dim_date"; Path="models\dimensions\dim_date.sql"; Depends=@()},
    @{Name="dim_location"; Path="models\dimensions\dim_location.sql"; Depends=@()},
    @{Name="dim_railcar"; Path="models\dimensions\dim_railcar.sql"; Depends=@()},
    @{Name="dim_train"; Path="models\dimensions\dim_train.sql"; Depends=@()},
    @{Name="dim_corridor"; Path="models\dimensions\dim_corridor.sql"; Depends=@("dim_location")}
)

Write-Host "`nBuilding dimension models..." -ForegroundColor Cyan

foreach ($model in $models) {
    Write-Host "  Building $($model.Name)..." -NoNewline
    
    # Read and process SQL (remove Jinja2 templates for manual execution)
    $sql = Get-Content $model.Path -Raw
    
    # Remove config directives
    $sql = $sql -replace '\{\{\s*config\s+"[^"]+"\s+"[^"]+"\s*\}\}', ''
    
    # Replace seed references
    $sql = $sql -replace '{{.*?seed.*?"raw_clm_events".*?}}', 'raw_clm_events'
    
    # Replace ref references  
    $sql = $sql -replace '{{.*?ref.*?"([^"]+)".*?}}', '$1'
    
    # Convert to table materialization (drop and create)
    $tableName = $model.Name
    $createSql = "DROP TABLE IF EXISTS $tableName; CREATE TABLE $tableName AS $sql"
    
    $cmd.CommandText = $createSql
    try {
        $cmd.ExecuteNonQuery() | Out-Null
        Write-Host " OK" -ForegroundColor Green
    } catch {
        Write-Host " FAIL" -ForegroundColor Red
        Write-Host "    Error: $_" -ForegroundColor Red
        throw
    }
}

# Get row counts
Write-Host ""
Write-Host "Dimension table row counts:" -ForegroundColor Cyan
foreach ($model in $models) {
    $cmd.CommandText = "SELECT COUNT(*) FROM $($model.Name)"
    $count = $cmd.ExecuteScalar()
    Write-Host "  $($model.Name): $count rows" -ForegroundColor Gray
}

$conn.Close()

Write-Host ""
Write-Host "=== Phase 2 Build Complete ===" -ForegroundColor Green
Write-Host "Database: $dbPath"

