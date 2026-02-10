# Unit Train Analytics Architecture

## Overview

This document describes the technical architecture of the Unit Train Analytics data warehouse, including data flow, schema design rationale, table relationships, and extensibility points.

## Data Flow

### End-to-End Pipeline

```
┌──────────────────┐
│ Seed Generator   │
│ (seed_generator.go)│
└────────┬─────────┘
         │ Generates ~4,400 rows
         ▼
┌──────────────────┐
│   seed.yml       │ ← Input data (dimensions + facts)
└────────┬─────────┘
         │ gorchata init
         ▼
┌──────────────────┐
│  unit_train.db   │ ← SQLite database
│                  │
│  Dimensions (5)  │
│  Facts (4)       │
└────────┬─────────┘
         │ gorchata test (Phase 6)
         ▼
┌──────────────────┐
│  Metrics (7)     │ ← Pre-aggregated summaries
└────────┬─────────┘
         │ gorchata test (Phase 7)
         ▼
┌──────────────────┐
│  Analytics (7)   │ ← Business intelligence queries
└────────┬─────────┘
         │ gorchata test (Phase 8)
         ▼
┌──────────────────┐
│ Validations (4)  │ ← Data quality checks
└──────────────────┘
```

### Components

1. **Seed Generator** (`seed_generator.go`):
   - Go program that generates realistic test data
   - Simulates 90 days of operations
   - Outputs YAML file with dimension and fact records
   - Handles complex logic: stragglers, queues, seasonal effects

2. **Schema Definition** (`schema.yml`):
   - Declarative YAML defining all 27 tables
   - Includes column definitions, data types, constraints
   - Embeds generic data quality tests
   - Read by Gorchata to create database

3. **Database** (`unit_train.db`):
   - SQLite file-based database (no server required)
   - Stores all dimensions, facts, metrics, analytics, validations
   - 27 tables total with referential integrity

4. **Test Profiles** (`profiles.yml`):
   - SQL queries for metrics aggregations
   - SQL queries for analytical reports
   - SQL queries for data validation
   - Executed by `gorchata test` command

5. **Test Suite** (`test/unit_train_*.go`):
   - Go tests validating schema structure
   - Go tests validating seed data generation
   - Integration tests for end-to-end workflow

## Schema Design

### Star Schema Rationale

We chose a **star schema** over a snowflake or normalized design because:

**Advantages:**
- ✅ **Query Simplicity**: Fewer joins required (1-2 hops max from fact to dimension)
- ✅ **Query Performance**: Denormalized dimensions reduce join overhead
- ✅ **User Friendliness**: Business analysts can understand and query without deep SQL knowledge
- ✅ **Aggregate-Friendly**: Easy to pre-aggregate metrics at various grains
- ✅ **Tool Compatibility**: Works well with BI tools (Tableau, Power BI, etc.)

**Trade-offs:**
- ❌ **Data Duplication**: Corridors duplicate location information (acceptable for 6 corridors)
- ❌ **Update Complexity**: Denormalized dims harder to update (not an issue for historical data)
- ❌ **Storage**: Slightly more disk space (negligible for this scale)

**Decision**: Star schema benefits outweigh costs for analytical workloads.

### Dimensional Model vs. Normalized OLTP

| Aspect | Normalized (OLTP) | Star Schema (OLAP) |
|--------|-------------------|-------------------|
| **Goal** | Transaction integrity | Analytical queries |
| **Optimization** | Insert/update speed | Query speed |
| **Joins** | Many (3rd normal form) | Few (denormalized) |
| **Schema Changes** | Easy | Requires rebuild |
| **User Queries** | Complex (many tables) | Simple (few joins) |
| **Use Case** | Operational systems | Data warehouses |

**This project**: Pure analytical warehouse, no transactional requirements.

## Table Relationships

### Entity-Relationship Diagram

```
┌─────────────┐
│   dim_car   │
│ (PK: car_id)│────────┐
└─────────────┘        │
                       │
┌─────────────┐        │
│  dim_train  │        │
│(PK: train_id)│───────┤
└─────────────┘        │
                       │
┌─────────────┐        │     ┌──────────────────┐
│ dim_corridor│        ├────▶│  fact_movement   │
│(PK:corridor_id)│─────┤     │ (Grain: car-trip)│
└─────────────┘        │     └────────┬─────────┘
                       │              │
┌─────────────┐        │              │
│dim_location │        │              │
│(PK:location_id)│─────┤              │
└─────────────┘        │              │
                       │              ├───▶ fact_straggler
┌─────────────┐        │              │     (subset: stragglers only)
│  dim_date   │        │              │
│(PK: date_id)│────────┘              ├───▶ fact_queue
└─────────────┘                       │     (queuing events)
                                      │
                                      └───▶ fact_power_transfer
                                           (locomotive changes)
```

### Relationship Types

**One-to-Many (Dimension → Fact)**:
- One car → many movements
- One train → many movements
- One corridor → many movements
- One location → many queues
- One date → many movements

**Optional Relationships**:
- Movement → Straggler (not all movements are stragglers)
- Movement → Queue (movements may or may not queue)
- Movement → Power Transfer (one per movement typically)

**No Many-to-Many**: Star schema avoids junction tables for query simplicity.

## Fact Table Grains

### fact_movement (Primary Fact)

**Grain**: One row per car per trip segment

**Definition**: A trip segment is one car moving from origin to destination on a specific corridor.

**Cardinality**: ~2,737 rows for 90-day simulation
- 228 cars × 12 round trips each = 2,736 theoretical
- Actual: 2,737 (slight variation due to trip duration rounding)

**Key Design Decision**: Car-level grain (not train-level) enables:
- Individual car tracking for maintenance predictions
- Flexibility in straggler analysis (car separates from train)
- Asset-level utilization metrics

**Alternative Grains Considered**:
- Train-level: Would lose individual car visibility
- Event-level: Too granular (departure, arrival as separate rows)

### fact_straggler (Derived Fact)

**Grain**: One row per straggler event

**Relationship**: Subset of fact_movement (10% of movements become stragglers)

**Cardinality**: ~273 rows
- Straggler rate: 10% of 2,737 movements = 273
- Week 8 spike: 2x rate for that week

**Key Design Decision**: Separate table (not flag in fact_movement) because:
- Keeps fact_movement clean and fast
- Allows detailed straggler attributes (delay_hours, resolution_type)
- Enables efficient straggler-only queries

**Alternative**: Add `is_straggler` flag to fact_movement
- Rejected: Would require NULL columns for non-stragglers

### fact_queue (Orthogonal Fact)

**Grain**: One row per queuing event at a location

**Cardinality**: ~912 rows
- 2,737 movements × 2 queues (origin + destination) ≈ 5,474 theoretical
- Actual: 912 (not all movements queue simultaneously)

**Key Design Decision**: Separate fact (not embedded in fact_movement) because:
- Queue is a location-level event (multiple cars queue together)
- Allows tracking queue capacity independent of movements
- Supports queue analysis without joining movement table

### fact_power_transfer (Transaction Fact)

**Grain**: One row per locomotive change

**Cardinality**: ~273 rows (one per trip typically)

**Key Design Decision**: Separate fact enables:
- Power efficiency analysis independent of car movements
- Locomotive assignment optimization
- Crew scheduling insights (different locomotives = different crews)

## Dimension Hierarchies

### dim_date (Time Dimension)

**Hierarchy**: Date → Week → Month → Quarter → Year

```
Year (2024)
└── Quarter (Q1)
    └── Month (1, 2, 3)
        └── Week (1-13)
            └── Date (2024-01-01 to 2024-03-30)
```

**Attributes**:
- `week_number`: 1-13 (for seasonal analysis)
- `is_weekend`: Boolean (for capacity planning)

**Design Decision**: Full date dimension (not just date datatype) enables:
- Pre-computed week numbers (no runtime date math)
- Consistent week definitions across all queries
- Easy addition of fiscal calendar or holidays

### dim_location (Geographic Dimension)

**Hierarchy**: Location → Corridor → Region (future)

```
Corridor (C1)
└── Origin (LOC001)
└── Destination (LOC002)
```

**Attributes**:
- `location_type`: ORIGIN | DESTINATION
- `capacity`: Integer (queue capacity)

**Design Decision**: Flat dimension (no geographic hierarchy) because:
- Only 12 locations total (small dimension)
- Corridors already encode origin-destination pairs
- Future enhancement: Add `region_id` for multi-region analysis

### dim_car (Asset Dimension)

**Type**: Slowly Changing Dimension Type 1 (overwrite)

**Attributes**:
- `car_type`: "Hopper" (all cars in this example)
- `capacity_tons`: 100-120 (varies by car)
- `acquisition_date`: When car was added to fleet

**Design Decision**: SCD Type 1 (no history) because:
- Example dataset doesn't require historical car attributes
- Production systems would use SCD Type 2 for:
  - Track maintenance history (car condition over time)
  - Ownership changes
  - Capacity modifications

**Future Enhancement**: Add effective_date / end_date for Type 2 tracking

## Performance Considerations

### Indexing Strategy

**Dimensions**:
- Primary keys are automatically indexed (SQLite)
- No additional indexes needed (small dimensions)

**Facts**:
- Index foreign keys for join performance:
  ```sql
  CREATE INDEX idx_movement_car ON fact_movement(car_id);
  CREATE INDEX idx_movement_train ON fact_movement(train_id);
  CREATE INDEX idx_movement_corridor ON fact_movement(corridor_id);
  CREATE INDEX idx_movement_date ON fact_movement(date_id);
  ```
- Composite index for common query patterns:
  ```sql
  CREATE INDEX idx_movement_corridor_date ON fact_movement(corridor_id, date_id);
  ```

**Metrics Tables**:
- Primary key on grain columns:
  ```sql
  CREATE UNIQUE INDEX idx_corridor_weekly ON agg_corridor_weekly_metrics(corridor_id, week_number);
  ```

### Query Optimization

**Pre-Aggregation**: Metrics tables (Phase 6) avoid runtime aggregation:
- Bad: `SELECT corridor_id, AVG(duration) FROM fact_movement GROUP BY corridor_id`
- Good: `SELECT * FROM agg_corridor_weekly_metrics`
- Performance gain: 10-100x for complex aggregations

**Denormalization**: Corridor includes distance_km to avoid location joins:
- Bad: `JOIN dim_corridor JOIN dim_location` (2 joins)
- Good: `SELECT distance_km FROM dim_corridor` (0 joins)

**Materialized Views**: All metrics are tables, not views:
- Bad: `CREATE VIEW agg_metrics AS SELECT ...` (runtime computation)
- Good: `CREATE TABLE agg_metrics ...` (pre-computed)

### Scaling Considerations

**Current Scale** (90 days, 228 cars):
- Dimensions: <1,000 rows total
- Facts: ~4,000 rows total
- Query time: <10ms for most queries

**Production Scale** (5 years, 10,000 cars):
- Dimensions: ~10,000 rows total (grow slowly)
- Facts: ~20 million rows (grow linearly with time)
- Optimization needed:
  - Partition fact tables by date (annual partitions)
  - Archive old data to separate database
  - Consider columnar storage (DuckDB, Parquet)

**Query Performance at Scale**:
| Query Type | Current (4K rows) | Production (20M rows) | Mitigation |
|------------|-------------------|-----------------------|------------|
| Full table scan | <10ms | ~5s | Add WHERE clause on date |
| Indexed lookup | <1ms | <10ms | Maintain indexes |
| Aggregation | <10ms | ~10s | Use pre-aggregated metrics |
| Metrics read | <1ms | <5ms | Metrics are small (pre-aggregated) |

## Extensibility Points

### Adding New Dimensions

**Example**: Add `dim_crew` for labor analysis

1. **Create dimension table in schema.yml**:
   ```yaml
   - name: dim_crew
     columns:
       - name: crew_id
         tests: [unique, not_null]
       - name: crew_name
       - name: certification_level
       - name: hire_date
   ```

2. **Add foreign keys to fact tables**:
   ```yaml
   # fact_movement
   - name: engineer_id
     tests:
       - relationships:
           to: ref('dim_crew')
           field: crew_id
   - name: conductor_id
     tests:
       - relationships:
           to: ref('dim_crew')
           field: crew_id
   ```

3. **Update seed generator**:
   ```go
   // Generate crew members
   crews := []Crew{
       {CrewID: "CREW001", CrewName: "John Doe", CertLevel: "Senior"},
       // ...
   }
   ```

4. **New metrics tables**:
   - `agg_crew_utilization`: Hours worked per crew member
   - `agg_crew_performance`: Trips per crew, incident rates

5. **Analytics queries**:
   - `analytics_crew_efficiency`: Compare crew performance
   - `analytics_crew_schedule`: Track rest period compliance

### Adding New Facts

**Example**: Add `fact_maintenance` for repair tracking

1. **Define grain**: One row per maintenance event per car

2. **Create fact table**:
   ```yaml
   - name: fact_maintenance
     columns:
       - name: maintenance_id
         tests: [unique, not_null]
       - name: car_id
         tests:
           - relationships:
               to: ref('dim_car')
               field: car_id
       - name: maintenance_date_id
       - name: maintenance_type
         tests:
           - accepted_values:
               values: ['Preventive', 'Corrective', 'Inspection']
       - name: downtime_hours
       - name: cost_dollars
   ```

3. **Relate to existing facts**:
   - Join on `car_id` to correlate maintenance with stragglers
   - Hypothesis: Cars with deferred maintenance straggle more

4. **New metrics**:
   - `agg_maintenance_cost`: Cost per car per month
   - `agg_maintenance_forecast`: Predict upcoming maintenance needs

### Adding New Metrics

**Example**: Add `agg_fuel_efficiency` for locomotive optimization

1. **Extend fact_power_transfer** with fuel data:
   ```yaml
   # fact_power_transfer
   - name: fuel_gallons
   - name: fuel_cost_dollars
   ```

2. **Create metrics table**:
   ```yaml
   - name: agg_fuel_efficiency
     columns:
       - name: corridor_id
       - name: week_number
       - name: total_fuel_gallons
       - name: avg_fuel_per_km
       - name: fuel_cost_total
   ```

3. **SQL query in profiles.yml**:
   ```sql
   SELECT 
     corridor_id,
     week_number,
     SUM(fuel_gallons) as total_fuel_gallons,
     AVG(fuel_gallons / c.distance_km) as avg_fuel_per_km
   FROM fact_power_transfer p
   JOIN dim_corridor c ON p.corridor_id = c.corridor_id
   GROUP BY corridor_id, week_number
   ```

### Adding New Analytics

**Example**: Add `analytics_predictive_maintenance` using ML

1. **Feature engineering query**:
   ```sql
   SELECT 
     car_id,
     COUNT(*) as trips_since_maintenance,
     AVG(trip_duration_days) as avg_trip_duration,
     SUM(CASE WHEN is_straggler THEN 1 ELSE 0 END) as straggler_count
   FROM fact_movement
   GROUP BY car_id
   ```

2. **Export to CSV** for ML model training:
   ```bash
   sqlite3 -header -csv unit_train.db "SELECT ..." > features.csv
   ```

3. **Train model** (Python, R, etc.):
   ```python
   import pandas as pd
   from sklearn.ensemble import RandomForestClassifier
   
   df = pd.read_csv('features.csv')
   model = RandomForestClassifier()
   model.fit(df[['trips_since_maintenance', 'avg_trip_duration']], df['straggler_count'])
   ```

4. **Store predictions** back in warehouse:
   ```yaml
   - name: analytics_maintenance_risk
     columns:
       - name: car_id
       - name: predicted_straggler_probability
       - name: recommended_action
   ```

### Adding Validations

**Example**: Add fuel consumption anomaly detection

1. **Define rule**: Fuel consumption should be proportional to distance

2. **Create validation query**:
   ```sql
   SELECT 
     p.power_transfer_id,
     p.fuel_gallons,
     c.distance_km,
     p.fuel_gallons / c.distance_km as fuel_per_km,
     CASE 
       WHEN p.fuel_gallons / c.distance_km > 2.0 THEN 'Anomaly'
       ELSE 'Normal'
     END as status
   FROM fact_power_transfer p
   JOIN dim_corridor c ON p.corridor_id = c.corridor_id
   ```

3. **Add to validation suite**:
   ```yaml
   - name: validation_fuel_anomalies
     columns:
       - name: power_transfer_id
       - name: fuel_per_km
         tests:
           - accepted_range:
               min_value: 0.5
               max_value: 2.0
   ```

## Data Quality Framework

### Four Validation Categories

1. **Referential Integrity** (structural):
   - All foreign keys resolve
   - No orphaned records
   - Dimension records exist before fact insert

2. **Temporal Consistency** (logic):
   - start_date ≤ end_date
   - No negative durations
   - Dates within simulation period

3. **Business Rules** (domain):
   - Straggler delays are 6-72 hours (per definition)
   - Queue times are <24 hours (operational limit)
   - Locations match corridor definitions

4. **Exclusivity** (complex):
   - Cars aren't in two places at once
   - Requires temporal overlap detection
   - **Known issue**: Fails due to round-trip overlap

### Test Coverage Strategy

| Test Type | Generic (schema.yml) | Custom (profiles.yml) | Go Tests |
|-----------|----------------------|-----------------------|----------|
| **Schema structure** | ✅ unique, not_null | | ✅ TestSchemaValidation |
| **Data types** | ✅ accepted_values | | ✅ TestSchemaParsing |
| **Ranges** | ✅ accepted_range | | ✅ TestDimensionTables |
| **Relationships** | ✅ relationships | | |
| **Complex rules** | | ✅ validation_* tables | ✅ TestValidation* |
| **Seed generation** | | | ✅ TestCarFleet, etc. |

## Technology Choices

### Why SQLite?

**Advantages**:
- ✅ Zero configuration (no server to install/manage)
- ✅ Single file database (easy to share, backup, version control)
- ✅ Cross-platform (Windows, Mac, Linux)
- ✅ Fast for small-to-medium datasets (<100GB)
- ✅ Excellent for development and testing
- ✅ Built-in full-text search and JSON support

**Limitations**:
- ❌ No concurrent writes (single writer lock)
- ❌ Limited scalability (not for 100M+ rows)
- ❌ No built-in partitioning or sharding
- ❌ No built-in replication

**When to migrate**:
- Data exceeds 10GB (consider DuckDB for analytics)
- Need concurrent writes (consider PostgreSQL)
- Need distributed setup (consider ClickHouse, BigQuery)

### Why YAML for Schema?

**Advantages**:
- ✅ Human-readable and editable
- ✅ Version control friendly (diffs are clear)
- ✅ Declarative (what, not how)
- ✅ Supports comments and documentation
- ✅ Common in data engineering (dbt, etc.)

**Alternatives considered**:
- SQL DDL: Too verbose, less readable
- JSON: No comments, harder to read
- TOML: Less common in data tooling

## References

- **Data Flow**: See [README.md](README.md#data-flow)
- **Table Descriptions**: See [README.md](README.md#table-categories)
- **Metrics Details**: See [METRICS.md](METRICS.md)
- **Schema Definition**: See `schema.yml`
- **Seed Generator**: See `seed_generator.go`

## Contributing

When extending the architecture:
- Maintain star schema design (avoid snowflaking)
- Keep fact grain clear and documented
- Add indexes for foreign keys
- Pre-aggregate metrics for performance
- Document design decisions and trade-offs

## License

This architecture documentation is part of the Gorchata project. See LICENSE file in the repository root.
