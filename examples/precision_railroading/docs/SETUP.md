# Setup Guide: Precision Scheduled Railroading Data Warehouse

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [System Requirements](#system-requirements)
3. [Installation Steps](#installation-steps)
4. [Step-by-Step Build Process](#step-by-step-build-process)
5. [Verification and Testing](#verification-and-testing)
6. [Troubleshooting](#troubleshooting)
7. [Reset Instructions](#reset-instructions)
8. [Performance Optimization](#performance-optimization)
9. [Advanced Configuration](#advanced-configuration)

## Prerequisites

### Required Software

#### 1. Go 1.25+ (with CGO_ENABLED=0)
```powershell
# Check Go version
go version
# Should output: go version go1.25 or higher

# Verify CGO is disabled (required constraint)
go env CGO_ENABLED
# Should output: 0
```

**Install Go**: Download from [https://go.dev/dl/](https://go.dev/dl/)

#### 2. Gorchata CLI
```powershell
# Verify gorchata is installed and in PATH
gorchata --version

# If not installed, build from repository root:
cd C:\Users\pierre\git\Gorchata
go build -o bin/gorchata.exe ./cmd/gorchata
# Add bin/ to PATH or copy gorchata.exe to a PATH directory
```

#### 3. PowerShell 5.1+
```powershell
# Check PowerShell version
$PSVersionTable.PSVersion
# Should output: 5.1 or higher
```

PowerShell 5.1 included with Windows 10/11. PowerShell 7+ also supported.

### Optional Tools
- **SQLite Browser** (for manual database inspection)
- **VS Code with SQLite extension** (for query development)

## System Requirements

### Hardware
- **CPU**: 4+ cores recommended (8+ for faster builds)
- **RAM**: 16GB minimum, 32GB recommended
  - Seed generation: ~2GB peak
  - Staging layer: ~8GB peak (processing 110M events)
  - Intermediate/Facts: ~4-6GB peak
- **Disk Space**: 25GB minimum, 30GB recommended
  - Seed data: 7.88GB (raw_clm_events.csv)
  - Database: 15-20GB (full warehouse)
  - Temp space: 2-3GB during builds
- **Disk Type**: SSD strongly recommended (10x faster than HDD for SQLite)

### Software
- Windows 10/11 (PowerShell native)
- Linux with PowerShell Core (supported but not primary platform)
- macOS with PowerShell Core (supported but not primary platform)

### Network
- No network connectivity required (fully offline build)
- Optional: Internet for package downloads during initial Go setup

## Installation Steps

### Step 1: Clone Repository
```powershell
# If not already cloned
git clone https://github.com/your-org/Gorchata.git
cd Gorchata
```

### Step 2: Build Gorchata CLI
```powershell
# From repository root
go build -o bin/gorchata.exe ./cmd/gorchata
```

### Step 3: Add to PATH (Optional)
```powershell
# Option A: Add bin/ directory to PATH
$env:PATH += ";C:\Users\pierre\git\Gorchata\bin"

# Option B: Copy to existing PATH directory
Copy-Item bin\gorchata.exe "C:\Windows\System32\"

# Verify
gorchata --version
```

### Step 4: Navigate to Example
```powershell
cd examples\precision_railroading
```

## Step-by-Step Build Process

### Phase 1: Generate Seed Data (~30 minutes)

```powershell
# Generate 110M CLM events
go run generate_clm_data.go
```

**What This Does**:
- Creates 12,000 railcars with realistic characteristics
- Generates 200 locations with SPLC codes and coordinates
- Simulates 10 years of CLM events (2016-2025, minute precision)
- Models PSR adoption: pre-PSR (2016-2017) → transition (2018-2020) → mature (2021-2025)
- Applies 25% seasonal variance (summer peak, winter trough)
- Writes to `seeds/raw_clm_events.csv`

**Progress Output**:
```
Generating CLM events for 12000 railcars across 200 locations (2016-2025)...
Generated 10000000 events (9.09% complete)...
Generated 20000000 events (18.18% complete)...
...
Generated 110000000 events (100.00% complete)
Successfully generated 110000000 events in seeds/raw_clm_events.csv (7.88 GB)
Generation time: 28m 34s
```

**Verify Seed Data**:
```powershell
# Check file exists and size
Get-Item seeds\raw_clm_events.csv | Select-Object Name, Length

# Expected output:
# Name                 Length
# ----                 ------
# raw_clm_events.csv   8466123456  # ~7.88 GB

# Check row count (takes 1-2 minutes)
(Get-Content seeds\raw_clm_events.csv | Measure-Object -Line).Lines
# Expected: ~110000001 (110M events + 1 header row)
```

### Phase 2: Build Dimension Layer (5-10 minutes)

```powershell
# Build all dimension tables
gorchata run --models dim_*
```

**Model Build Order**:
1. `dim_railcar` (~30 seconds): 12,000 railcars
2. `dim_location` (~30 seconds): 200 locations
3. `dim_corridor` (~2 minutes): ~950 O-D pairs
4. `dim_train` (~1 minute): Train configurations
5. `dim_date` (~30 seconds): 3,653 days (2016-2025)

**Expected Output**:
```
Building model: dim_railcar
✓ Materialized dim_railcar (12000 rows, 32s)

Building model: dim_location
✓ Materialized dim_location (200 rows, 28s)

...

✓ All dimension models built successfully
```

**Verify Dimensions**:
```powershell
# Connect to database (if using default SQLite location)
sqlite3 gorchata.db

# Check row counts
SELECT 'dim_railcar' AS table_name, COUNT(*) FROM dim_railcar
UNION ALL
SELECT 'dim_location', COUNT(*) FROM dim_location
UNION ALL
SELECT 'dim_corridor', COUNT(*) FROM dim_corridor
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date;

# Expected output:
# dim_railcar    | 12000
# dim_location   | 200
# dim_corridor   | ~950
# dim_date       | 3653
```

### Phase 3: Build Staging Layer (20-30 minutes)

```powershell
# Build staging models
gorchata run --models stg_clm_events,stg_clm_enriched
```

**Model Build Details**:
1. `stg_clm_events` (~15 minutes):
   - Loads 110M events from CSV
   - Validates event types, timestamps, SPLC codes
   - Applies minute-precision constraints
   - Materializes 110M rows

2. `stg_clm_enriched` (~10 minutes):
   - Enriches events with dimension FKs
   - Calculates event sequences per railcar
   - Derives loaded/movement flags
   - Materializes 110M rows

**Expected Output**:
```
Building model: stg_clm_events
  Processing CSV: 10M rows... 20M rows... 30M rows...
✓ Materialized stg_clm_events (110000000 rows, 14m 23s)

Building model: stg_clm_enriched
  Enriching with dimension lookups...
✓ Materialized stg_clm_enriched (110000000 rows, 9m 47s)

✓ Staging layer complete
```

### Phase 4: Build Intermediate Layer (30-45 minutes)

```powershell
# Build intermediate models
gorchata run --models int_*
```

**Model Build Details**:
1. `int_state_intervals` (~12 minutes): Contiguous state periods (~55M rows)
2. `int_trip_segments` (~10 minutes): Trip identification (~35M rows)
3. `int_velocity_vectors` (~8 minutes): Speed calculations (~35M rows)
4. `int_nodal_dwell` (~6 minutes): Dwell event extraction (~20M rows)
5. `int_dwell_classification` (~5 minutes): Shadow yard detection (~20M rows)
6. `int_cycle_classification` (~4 minutes): Cycle analysis (~18M rows)

**Total**: ~45 minutes

**Expected Output**:
```
Building model: int_state_intervals
  Identifying contiguous state periods...
✓ Materialized int_state_intervals (55234567 rows, 11m 52s)

...

✓ Intermediate layer complete (6 models, 41m 18s)
```

### Phase 5: Build Facts Layer (15-25 minutes)

```powershell
# Build fact tables
gorchata run --models fact_*
```

**Model Build Details**:
1. `fact_trip` (~10 minutes): Trip-level metrics (~35M rows)
2. `fact_dwell` (~6 minutes): Dwell events (~20M rows)
3. `fact_stop_classification` (~5 minutes): Stop classifications (~20M rows)
4. `fact_corridor_transit` (~3 minutes): Weekly aggregations (~450K rows)

**Total**: ~24 minutes

**Expected Output**:
```
Building model: fact_trip
✓ Materialized fact_trip (35123456 rows, 9m 34s)

...

✓ Facts layer complete (4 models, 22m 47s)
```

### Phase 6: Build Metrics Layer (5-10 minutes)

```powershell
# Build aggregation tables
gorchata run --models agg_*
```

**Model Build Details**:
1. `agg_network_fluidity` (~2 minutes): Weekly fluidity index (~450K rows)
2. `agg_slot_adherence` (~1 minute): Monthly adherence scores (~12K rows)
3. `agg_shadow_yards` (~30 seconds): Shadow yard risk scores (200 rows)
4. `agg_buffer_consumption` (~1 minute): Buffer utilization (~950 rows)
5. `agg_directional_asymmetry` (~1 minute): Directional ratios (~950 rows)
6. `agg_corridor_weekly_performance` (~2 minutes): Weekly KPIs (~450K rows)
7. `agg_psr_evolution` (~30 seconds): PSR trend metrics (~120 rows)

**Total**: ~8 minutes

### Phase 7: Build Analytics Layer (1-2 minutes)

```powershell
# Build analytical queries
gorchata run --models shadow_yard_identification,worst_performing_corridors,seasonal_performance_trends,psr_strategy_shifts,network_congestion_hotspots,directional_efficiency_analysis
```

**Or simply**:
```powershell
# Build remaining models
gorchata run
```

**Model Build Details**:
1. `shadow_yard_identification` (~20 seconds): Top shadow yards (5-7 rows)
2. `network_congestion_hotspots` (~20 seconds): Congested corridors (5-8 rows)
3. `worst_performing_corridors` (~20 seconds): Low-velocity routes (10 rows)
4. `seasonal_performance_trends` (~20 seconds): Quarterly trends (40 rows)
5. `psr_strategy_shifts` (~15 seconds): Period-over-period metrics (3 rows)
6. `directional_efficiency_analysis` (~15 seconds): Asymmetry analysis (~15 rows)

**Total**: ~2 minutes

## Verification and Testing

### Run Data Quality Tests (~2 minutes)

```powershell
# Run all 223+ tests
gorchata test
```

**Expected Output**:
```
Running data quality tests...

✓ test_referential_integrity (15 tests, 0 violations, 2.1s)
✓ test_temporal_consistency (10 tests, 0 violations, 3.4s)
✓ test_business_rules (15 tests, 0 violations, 1.9s)
✓ test_exclusivity_constraints (8 tests, 0 violations, 2.8s)
✓ test_minute_precision (10 tests, 0 violations, 1.7s)
✓ dimension tests (35 tests, 0 violations, 8.2s)
✓ staging tests (25 tests, 0 violations, 12.5s)
✓ intermediate tests (54 tests, 0 violations, 18.7s)
✓ fact tests (38 tests, 0 violations, 15.3s)
✓ metric tests (21 tests, 0 violations, 6.8s)
✓ analytics tests (12 tests, 0 violations, 2.4s)

========================================
SUMMARY: 223 tests passed, 0 failed
Total execution time: 1m 56s
========================================
```

### Verify Key Metrics

```sql
-- Connect to database
sqlite3 gorchata.db

-- 1. Check shadow yards identified
SELECT COUNT(*) FROM shadow_yard_identification;
-- Expected: 5-7

-- 2. Check congestion hotspots
SELECT COUNT(*) FROM network_congestion_hotspots;
-- Expected: 5-8

-- 3. Check PSR evolution
SELECT * FROM psr_strategy_shifts ORDER BY psr_period;
-- Expected: 3 rows (pre-PSR, transition, mature)

-- 4. Verify row counts across layers
SELECT 'staging' AS layer, 'stg_clm_events' AS model, COUNT(*) FROM stg_clm_events
UNION ALL
SELECT 'intermediate', 'int_trip_segments', COUNT(*) FROM int_trip_segments
UNION ALL
SELECT 'facts', 'fact_trip', COUNT(*) FROM fact_trip
UNION ALL
SELECT 'metrics', 'agg_shadow_yards', COUNT(*) FROM agg_shadow_yards;
-- Expected: ~110M, ~35M, ~35M, 200
```

## Troubleshooting

### Problem: Out of Memory During Seed Generation

**Symptom**:
```
fatal error: out of memory
goroutine 1 [running]:
```

**Solutions**:
1. Increase system RAM (16GB minimum, 32GB recommended)
2. Reduce batch size in `generate_clm_data.go`:
   ```go
   // Line ~50: Reduce writeBatchSize
   const writeBatchSize = 10000  // Change from 50000 to 10000
   ```
3. Generate data in smaller time chunks (modify date range)

### Problem: CSV Import Very Slow (>60 minutes)

**Symptom**: `stg_clm_events` build taking excessive time

**Solutions**:
1. **Verify SSD usage**: SQLite on HDD is 10x slower
   ```powershell
   # Check drive type
   Get-PhysicalDisk | Select-Object FriendlyName, MediaType
   ```

2. **Enable memory-mapped I/O** (if not already):
   ```sql
   PRAGMA mmap_size = 268435456;  -- 256MB
   ```

3. **Increase cache size**:
   ```sql
   PRAGMA cache_size = -64000;  -- 64MB cache
   ```

4. **Disable sync during initial load** (unsafe, use only for initial build):
   ```sql
   PRAGMA synchronous = OFF;
   PRAGMA journal_mode = MEMORY;
   ```

### Problem: Test Failures

**Symptom**: Data quality tests reporting violations

**Solution Steps**:
1. **Identify failing tests**:
   ```powershell
   gorchata test --verbose
   ```

2. **Check test details**:
   ```powershell
   # Run specific test file
   gorchata test tests/test_referential_integrity.sql
   ```

3. **Verify build order**:
   - Ensure dimensions built before staging
   - Ensure staging built before intermediate
   - Ensure intermediate built before facts

4. **Check for schema mismatches**:
   ```sql
   -- Verify column existence
   PRAGMA table_info(agg_slot_adherence);
   -- Should show 'adherence_score' column (not 'slot_adherence_pct')
   ```

### Problem: Schema Mismatch Errors

**Symptom**:
```
Error: no such column: slot_adherence_pct
```

**Solution**:
1. Verify you're using correct column names (see model SQL files)
2. Rebuild affected model:
   ```powershell
   gorchata run --models agg_slot_adherence --full-refresh
   ```
3. Check test files match current schema (should be fixed in Phase 9&10 revision)

### Problem: Insufficient Disk Space

**Symptom**:
```
Error: database or disk is full
```

**Solutions**:
1. Free up space (30GB needed)
2. Change database location to larger drive:
   ```powershell
   # Edit profiles.yml
   # Under target, change dbname to different drive:
   dbname: "D:\data\gorchata_psr.db"
   ```
3. Clean up temp files:
   ```powershell
   Remove-Item -Path $env:TEMP\* -Recurse -Force -ErrorAction SilentlyContinue
   ```

### Problem: Incorrect Row Counts

**Symptom**: Model shows significantly fewer/more rows than expected

**Solutions**:
1. **Check seed data**:
   ```powershell
   (Get-Content seeds\raw_clm_events.csv | Measure-Object -Line).Lines
   # Should be ~110M
   ```

2. **Rebuild from staging**:
   ```powershell
   gorchata run --models stg_clm_events --full-refresh
   gorchata run --full-refresh
   ```

3. **Verify no data corruption**:
   ```sql
   -- Check for NULL PKs
   SELECT COUNT(*) FROM dim_railcar WHERE railcar_id IS NULL;
   -- Should be 0
   ```

## Reset Instructions

### Full Reset (Start Fresh)

```powershell
# 1. Remove seed data
Remove-Item seeds\raw_clm_events.csv -Force -ErrorAction SilentlyContinue

# 2. Remove database
Remove-Item *.db -Force -ErrorAction SilentlyContinue
Remove-Item *.db-shm -Force -ErrorAction SilentlyContinue
Remove-Item *.db-wal -Force -ErrorAction SilentlyContinue

# 3. Regenerate
go run generate_clm_data.go
gorchata run
gorchata test
```

### Partial Reset (Keep Seed Data)

```powershell
# Remove database only
Remove-Item *.db* -Force

# Rebuild warehouse
gorchata run
gorchata test
```

### Rebuild Specific Layer

```powershell
# Rebuild just facts and downstream
gorchata run --models fact_* agg_* --full-refresh
gorchata run --models shadow_yard_identification,network_congestion_hotspots,worst_performing_corridors,seasonal_performance_trends,psr_strategy_shifts,directional_efficiency_analysis --full-refresh
```

## Performance Optimization

### Use PowerShell Build Script

Create `build.ps1`:
```powershell
#!/usr/bin/env pwsh
param(
    [switch]$FullBuild,
    [switch]$Test
)

$ErrorActionPreference = "Stop"

if ($FullBuild) {
    Write-Host "Generating seed data..." -ForegroundColor Cyan
    go run generate_clm_data.go
}

Write-Host "Building warehouse..." -ForegroundColor Cyan
& gorchata run

if ($Test) {
    Write-Host "Running tests..." -ForegroundColor Cyan
    & gorchata test
}

Write-Host "✓ Build complete" -ForegroundColor Green
```

Run with:
```powershell
.\build.ps1 -FullBuild -Test
```

### Parallel Processing (Future Enhancement)

Gorchata currently builds sequentially. For faster builds, consider:
- Manually running independent models in parallel
- Dimensions can build in parallel (no dependencies between them)
- Intermediate models can build in parallel (after staging completes)

Example:
```powershell
# Terminal 1
gorchata run --models int_state_intervals

# Terminal 2 (simultaneously)
gorchata run --models int_cycle_classification
```

## Advanced Configuration

### Custom Database Location

Edit `profiles.yml`:
```yaml
gorchata_precision_railroading:
  target: dev
  outputs:
    dev:
      type: sqlite
      dbname: "D:\data\psr_warehouse.db"  # Custom location
```

### Adjust Memory Settings

For large builds, increase SQLite cache:
```powershell
# Add to model config
{{ config "materialized" "table" }}
-- PRAGMA cache_size = -128000;  -- 128MB cache
-- PRAGMA temp_store = MEMORY;
```

### Monitor Build Progress

Use verbose logging:
```powershell
gorchata run --log-level debug
```

## Next Steps

After successful setup:
1. Explore [QUERIES.md](QUERIES.md) for example analyses
2. Review [BUSINESS_CONTEXT.md](BUSINESS_CONTEXT.md) for PSR background
3. Study [PSR_EVOLUTION.md](PSR_EVOLUTION.md) for three-period framework
4. Check [ARCHITECTURE.md](ARCHITECTURE.md) for technical details

## Support

For issues or questions:
- Check warehouse tests: `gorchata test --verbose`
- Review build logs for error details
- Verify prerequisites and system requirements
- Consult [README.md](../README.md) for overview
