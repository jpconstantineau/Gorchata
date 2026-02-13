# Minimal test database setup for Phase 3 staging layer testing
# Creates a small SQLite database with minimal test data

$ErrorActionPreference = "Stop"

Write-Host "=== Creating Test Database for Phase 3 ===" -ForegroundColor Cyan

# Ensure target directory exists
if (!(Test-Path "target")) {
    New-Item -ItemType Directory -Path "target" | Out-Null
}

# Remove old database
$dbPath = "target\precision_railroading.db"
if (Test-Path $dbPath) {
    Remove-Item $dbPath
    Write-Host "Removed old database"
}

# Use System.Data.SQLite from NuGet or bundled with .NET
# Try to load SQLite

try {
    # Try loading from common locations
    $sqlitePaths = @(
        "$env:USERPROFILE\.nuget\packages\system.data.sqlite.core\*\lib\net46\System.Data.SQLite.dll",
        "$env:USERPROFILE\.nuget\packages\microsoft.data.sqlite\*\lib\netstandard2.0\Microsoft.Data.Sqlite.dll",
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
        Write-Host "Loaded SQLite from: $foundPath" -ForegroundColor Green
        
        # Create connection
        $connString = "Data Source=$dbPath;Version=3;"
        $conn = New-Object System.Data.SQLite.SQLiteConnection($connString)
        $conn.Open()
        
        Write-Host "Creating test data tables..." -ForegroundColor Cyan
        
        $cmd = $conn.CreateCommand()
        
        # Create raw_clm_events table with minimal test data
        $cmd.CommandText = @"
CREATE TABLE raw_clm_events (
    event_id TEXT PRIMARY KEY,
    car_number TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    event_type TEXT NOT NULL,
    splc_code TEXT NOT NULL,
    train_id TEXT,
    location_name TEXT NOT NULL
);

-- Insert 50 test events (10 cars, 5 events each over 2 days)
"@
        $cmd.ExecuteNonQuery() | Out-Null
        
        # Generate test data
        Write-Host "Generating test events..." -ForegroundColor Cyan
        $eventId = 1
        $cars = @("CAR001", "CAR002", "CAR003", "CAR004", "CAR005", "CAR006", "CAR007", "CAR008", "CAR009", "CAR010")
        $splcCodes = @("T-CHI-01", "Y-KC-02", "C-DEN-01", "I-STL-01", "T-LA-01")
        $locations = @("Chicago Terminal", "Kansas City Yard", "Denver Customer", "St Louis Interchange", "LA Terminal")
        $trains = @("T-M100", "T-M200", "T-U300", $null, "T-M150")
        $eventTypes = @("DEPA", "ARRI", "PLAC", "PULL")
        
        $baseDate = Get-Date "2024-01-01 08:00:00"
        
        foreach ($car in $cars) {
            $carDate = $baseDate
            # Each car: ARRI -> PLAC -> PULL -> DEPA -> ARRI (5 events)
            $sequence = @(
                @{Type="ARRI"; SPLCIdx=0},
                @{Type="PLAC"; SPLCIdx=0},
                @{Type="PULL"; SPLCIdx=0},
                @{Type="DEPA"; SPLCIdx=0},
                @{Type="ARRI"; SPLCIdx=1}
            )
            
            foreach ($ev in $sequence) {
                $timestamp = $carDate.ToString("yyyy-MM-dd HH:mm:00")
                $splcIdx = $ev.SPLCIdx
                $trainId = if ($ev.Type -in @("DEPA", "ARRI")) { $trains[$splcIdx] } else { $null }
                $trainVal = if ($null -eq $trainId) { "NULL" } else { "'$trainId'" }
                
                $insertSql = @"
INSERT INTO raw_clm_events VALUES (
    'EVT-$eventId',
    '$car',
    '$timestamp',
    '$($ev.Type)',
    '$($splcCodes[$splcIdx])',
    $trainVal,
    '$($locations[$splcIdx])'
);
"@
                $cmd.CommandText = $insertSql
                $cmd.ExecuteNonQuery() | Out-Null
                
                $eventId++
                $carDate = $carDate.AddHours(2)
            }
        }
        
        Write-Host "Created $($eventId - 1) test events" -ForegroundColor Green
        
        # Create dimension tables
        Write-Host "Creating dimension tables..." -ForegroundColor Cyan
        
        # dim_date (covering 2024-01-01 to 2024-01-31)
        $cmd.CommandText = @"
CREATE TABLE dim_date (
    date_id INTEGER PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name TEXT,
    week_of_year INTEGER,
    day_of_month INTEGER,
    day_of_week INTEGER,
    day_name TEXT,
    is_weekend INTEGER,
    season TEXT,
    psr_period TEXT
);
"@
        $cmd.ExecuteNonQuery() | Out-Null
        
        # Insert January 2024 dates
        for ($day = 1; $day -le 31; $day++) {
            $date = Get-Date -Year 2024 -Month 1 -Day $day
            $dateId = 20240000 + 100 + $day
            $dow = [int]$date.DayOfWeek
            if ($dow -eq 0) { $dow = 7 }  # Sunday = 7
            $dowName = $date.ToString("dddd")
            $isWeekend = if ($dow -in @(6,7)) { 1 } else { 0 }
            $quarter = 1
            $monthName = "January"
            $weekOfYear = Get-Date $date -UFormat %V
            $season = "Winter"
            
            $cmd.CommandText = @"
INSERT INTO dim_date VALUES (
    $dateId, '$($date.ToString("yyyy-MM-dd"))', 2024, $quarter, 1, '$monthName',
    $weekOfYear, $day, $dow, '$dowName', $isWeekend, '$season', 'mature_psr'
);
"@
            $cmd.ExecuteNonQuery() | Out-Null
        }
        
        Write-Host "  Created dim_date ($31 days)" -ForegroundColor Gray
        
        # dim_location
        $cmd.CommandText = @"
CREATE TABLE dim_location (
    location_id INTEGER PRIMARY KEY AUTOINCREMENT,
    splc_code TEXT NOT NULL UNIQUE,
    location_name TEXT NOT NULL,
    location_type TEXT NOT NULL,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    shadow_yard_risk_score REAL NOT NULL,
    region TEXT NOT NULL
);
"@
        $cmd.ExecuteNonQuery() | Out-Null
        
        $locations_data = @(
            @{SPLC="T-CHI-01"; Name="Chicago Terminal"; Type="terminal"; Lat=41.8781; Lon=-87.6298; Risk=10; Region="Midwest"},
            @{SPLC="Y-KC-02"; Name="Kansas City Yard"; Type="yard"; Lat=39.0997; Lon=-94.5786; Risk=25; Region="Midwest"},
            @{SPLC="C-DEN-01"; Name="Denver Customer"; Type="customer_site"; Lat=39.7392; Lon=-104.9903; Risk=5; Region="West"},
            @{SPLC="I-STL-01"; Name="St Louis Interchange"; Type="interchange"; Lat=38.6270; Lon=-90.1994; Risk=15; Region="Midwest"},
            @{SPLC="T-LA-01"; Name="LA Terminal"; Type="terminal"; Lat=34.0522; Lon=-118.2437; Risk=12; Region="West"}
        )
        
        foreach ($loc in $locations_data) {
            $cmd.CommandText = @"
INSERT INTO dim_location (splc_code, location_name, location_type, latitude, longitude, shadow_yard_risk_score, region)
VALUES ('$($loc.SPLC)', '$($loc.Name)', '$($loc.Type)', $($loc.Lat), $($loc.Lon), $($loc.Risk), '$($loc.Region)');
"@
            $cmd.ExecuteNonQuery() | Out-Null
        }
        
        Write-Host "  Created dim_location (5 locations)" -ForegroundColor Gray
        
        # dim_railcar
        $cmd.CommandText = @"
CREATE TABLE dim_railcar (
    railcar_id INTEGER PRIMARY KEY AUTOINCREMENT,
    car_number TEXT NOT NULL UNIQUE,
    railroad_owner TEXT NOT NULL,
    car_type TEXT NOT NULL
);
"@
        $cmd.ExecuteNonQuery() | Out-Null
        
        foreach ($car in $cars) {
            $owner = @("BNSF", "UP", "CSX")[[int]($car.Substring(4)) % 3]
            $type = @("boxcar", "hopper", "tank")[[int]($car.Substring(5)) % 3]
            $cmd.CommandText = @"
INSERT INTO dim_railcar (car_number, railroad_owner, car_type)
VALUES ('$car', '$owner', '$type');
"@
            $cmd.ExecuteNonQuery() | Out-Null
        }
        
        Write-Host "  Created dim_railcar (10 cars)" -ForegroundColor Gray
        
        # dim_train
        $cmd.CommandText = @"
CREATE TABLE dim_train (
    train_id TEXT PRIMARY KEY,
    train_type TEXT NOT NULL,
    priority_level INTEGER NOT NULL
);
"@
        $cmd.ExecuteNonQuery() | Out-Null
        
        $train_data = @(
            @{ID="T-M100"; Type="manifest"; Priority=3},
            @{ID="T-M200"; Type="manifest"; Priority=3},
            @{ID="T-U300"; Type="unit"; Priority=2},
            @{ID="T-M150"; Type="manifest"; Priority=3}
        )
        
        foreach ($tr in $train_data) {
            $cmd.CommandText = @"
INSERT INTO dim_train VALUES ('$($tr.ID)', '$($tr.Type)', $($tr.Priority));
"@
            $cmd.ExecuteNonQuery() | Out-Null
        }
        
        Write-Host "  Created dim_train (4 trains)" -ForegroundColor Gray
        
        $conn.Close()
        
        Write-Host ""
        Write-Host "=== Test Database Created Successfully ===" -ForegroundColor Green
        Write-Host "Database: $dbPath"
        Write-Host ""
        Write-Host "Ready to test staging models with gorchata build" -ForegroundColor Cyan
        
    } else {
        Write-Host "ERROR: Could not find System.Data.SQLite.dll" -ForegroundColor Red
        Write-Host ""
        Write-Host "Alternative: Install PSSQLite module:" -ForegroundColor Yellow
        Write-Host "  Install-Module -Name PSSQLite -Scope CurrentUser" -ForegroundColor Gray
        Write-Host ""
        Write-Host "Or install System.Data.SQLite.Core NuGet package to .nuget folder" -ForegroundColor Yellow
        exit 1
    }
    
} catch {
    Write-Host "ERROR creating database: $_" -ForegroundColor Red
    Write-Host $_.ScriptStackTrace -ForegroundColor Red
    exit 1
}
