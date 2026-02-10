# Unit Train Analytics Metrics Catalog

## Overview

This document describes the 7 pre-aggregated metrics tables in the Unit Train Analytics data warehouse. These tables summarize fact table data to provide fast answers to common business questions without requiring complex joins or runtime aggregation.

All metrics are **materialized tables** (not views) to optimize query performance. In a production environment, these would be refreshed on a schedule (e.g., nightly batch jobs or incremental updates).

## Metrics Tables

### 1. agg_corridor_weekly_metrics

**Purpose**: Track performance of each corridor (route) by week to identify seasonal trends and problem routes.

**Grain**: One row per corridor per week

**Business Question**: "Which corridors are performing worst this week? How does this compare to last week?"

**Key Columns**:
- `corridor_id`: Foreign key to dim_corridor
- `week_number`: Week of the year (1-52)
- `total_movements`: Count of car movements on this corridor this week
- `avg_trip_duration_days`: Average transit time (origin to destination)
- `straggler_count`: Number of straggler events
- `straggler_rate`: Percentage of movements that became stragglers
- `avg_queue_duration_hours`: Average time spent in queues (loading + unloading)
- `total_power_transfers`: Count of locomotive changes

**Calculations**:
```sql
-- Example: Straggler rate calculation
straggler_rate = (straggler_count / total_movements) * 100

-- Example: Average trip duration
avg_trip_duration_days = SUM(trip_duration_days) / COUNT(*)
```

**Usage**:
```sql
-- Find worst performing corridor this week
SELECT corridor_id, avg_trip_duration_days, straggler_rate
FROM agg_corridor_weekly_metrics
WHERE week_number = 8
ORDER BY straggler_rate DESC
LIMIT 1;

-- Compare week 5 (slowdown) vs week 8 (straggler spike)
SELECT week_number, AVG(avg_trip_duration_days) as avg_duration
FROM agg_corridor_weekly_metrics
WHERE week_number IN (5, 8)
GROUP BY week_number;
```

**Refresh Frequency**: Daily (after fact tables updated)

---

### 2. agg_train_daily_performance

**Purpose**: Monitor daily performance of each unit train to ensure consistent operations and identify underperforming consists.

**Grain**: One row per train per day

**Business Question**: "How many trips did each train complete today? Were there operational issues?"

**Key Columns**:
- `train_id`: Foreign key to dim_train
- `date_id`: Foreign key to dim_date
- `movements_completed`: Count of car movements completed
- `unique_cars_moved`: Distinct count of cars (should be ~76 per train)
- `avg_trip_duration_days`: Average transit time for movements
- `straggler_events`: Count of stragglers from this train
- `total_queue_hours`: Sum of queue time at origins and destinations
- `power_transfers`: Count of locomotive changes

**Calculations**:
```sql
-- Example: Train utilization
train_utilization_pct = (unique_cars_moved / train_capacity) * 100

-- Example: Operational efficiency
efficiency_score = movements_completed / (movements_completed + straggler_events)
```

**Usage**:
```sql
-- Daily train performance dashboard
SELECT t.train_name, m.movements_completed, m.straggler_events
FROM agg_train_daily_performance m
JOIN dim_train t ON m.train_id = t.train_id
WHERE m.date_id = (SELECT date_id FROM dim_date WHERE full_date = '2024-01-15');

-- Identify train with most stragglers this month
SELECT train_id, SUM(straggler_events) as total_stragglers
FROM agg_train_daily_performance
GROUP BY train_id
ORDER BY total_stragglers DESC;
```

**Refresh Frequency**: Daily (end of day batch)

---

### 3. agg_car_utilization_metrics

**Purpose**: Analyze individual car performance and utilization to optimize fleet management and predict maintenance needs.

**Grain**: One row per car for the entire simulation period

**Business Question**: "Which cars are underutilized? Which have excessive straggler issues?"

**Key Columns**:
- `car_id`: Foreign key to dim_car
- `total_movements`: Count of trips this car completed
- `total_days_in_service`: Sum of days car was in transit
- `utilization_rate`: Percentage of simulation period car was active
- `straggler_count`: Number of times this car straggled
- `avg_straggler_delay_hours`: Average delay when car straggles
- `corridors_served`: Count of distinct corridors this car traveled
- `total_distance_km`: Sum of distance traveled (from dim_corridor)

**Calculations**:
```sql
-- Example: Utilization rate
utilization_rate = (total_days_in_service / 90) * 100

-- Example: Straggler prone indicator
is_high_risk = CASE 
  WHEN straggler_count > PERCENTILE_90(straggler_count) THEN true 
  ELSE false 
END
```

**Usage**:
```sql
-- Find underutilized cars (potential to redeploy)
SELECT car_id, utilization_rate, total_movements
FROM agg_car_utilization_metrics
WHERE utilization_rate < 50
ORDER BY utilization_rate ASC;

-- Cars with excessive straggler problems (maintenance candidates)
SELECT car_id, straggler_count, avg_straggler_delay_hours
FROM agg_car_utilization_metrics
WHERE straggler_count > 10
ORDER BY straggler_count DESC;
```

**Refresh Frequency**: Weekly or monthly (less time-sensitive)

---

### 4. agg_straggler_analysis

**Purpose**: Deep dive into straggler events by location and time period to identify root causes and hotspots.

**Grain**: One row per location per week

**Business Question**: "Where do stragglers occur most frequently? What are the common causes?"

**Key Columns**:
- `location_id`: Foreign key to dim_location
- `week_number`: Week of the year
- `straggler_count`: Total straggler events at this location
- `avg_delay_hours`: Average delay duration
- `max_delay_hours`: Longest delay observed
- `unique_cars_affected`: Distinct count of cars that straggled here
- `common_resolution`: Most frequent resolution type (e.g., "Mechanical repair")
- `week_over_week_change`: Percent change from previous week

**Calculations**:
```sql
-- Example: Week over week change
week_over_week_change = 
  ((this_week.straggler_count - last_week.straggler_count) / last_week.straggler_count) * 100

-- Example: Location risk score
risk_score = (straggler_count * avg_delay_hours) / unique_cars_affected
```

**Usage**:
```sql
-- Straggler hotspots (locations with most events)
SELECT l.location_name, s.straggler_count, s.avg_delay_hours
FROM agg_straggler_analysis s
JOIN dim_location l ON s.location_id = l.location_id
WHERE week_number = 8
ORDER BY straggler_count DESC
LIMIT 5;

-- Trend analysis: straggler spike in week 8
SELECT week_number, SUM(straggler_count) as total_stragglers
FROM agg_straggler_analysis
GROUP BY week_number
ORDER BY week_number;
```

**Refresh Frequency**: Daily (for monitoring dashboards)

---

### 5. agg_queue_analysis

**Purpose**: Identify bottlenecks at loading/unloading facilities to optimize capacity planning.

**Grain**: One row per location per week

**Business Question**: "Which facilities have the longest wait times? Are we hitting capacity limits?"

**Key Columns**:
- `location_id`: Foreign key to dim_location
- `week_number`: Week of the year
- `total_queue_events`: Count of queuing occurrences
- `avg_queue_duration_hours`: Average wait time
- `max_queue_duration_hours`: Longest wait observed
- `p95_queue_duration_hours`: 95th percentile wait (excludes outliers)
- `location_type`: ORIGIN or DESTINATION (from dim_location)
- `capacity_utilization_pct`: Queue time vs available time

**Calculations**:
```sql
-- Example: Capacity utilization
capacity_utilization_pct = 
  (total_queue_hours / (7 * 24)) * 100  -- 7 days = 168 hours max

-- Example: Bottleneck indicator
is_bottleneck = CASE 
  WHEN p95_queue_duration_hours > 18 THEN true 
  ELSE false 
END
```

**Usage**:
```sql
-- Worst bottlenecks  (highest average queue time)
SELECT l.location_name, q.avg_queue_duration_hours, q.total_queue_events
FROM agg_queue_analysis q
JOIN dim_location l ON q.location_id = l.location_id
ORDER BY q.avg_queue_duration_hours DESC
LIMIT 5;

-- Compare origin vs destination queues
SELECT location_type, AVG(avg_queue_duration_hours) as avg_wait
FROM agg_queue_analysis q
JOIN dim_location l ON q.location_id = l.location_id
GROUP BY location_type;
```

**Refresh Frequency**: Daily (for operational monitoring)

---

### 6. agg_power_transfer_analysis

**Purpose**: Optimize locomotive assignments by analyzing power transfer patterns.

**Grain**: One row per train per week

**Business Question**: "How often are we changing locomotives? Are extended transfers impacting efficiency?"

**Key Columns**:
- `train_id`: Foreign key to dim_train
- `week_number`: Week of the year
- `total_transfers`: Count of all power transfer events
- `quick_transfers`: Count of transfers < 1 hour (same locomotives)
- `extended_transfers`: Count of transfers > 1 hour (different locomotives)
- `repower_rate`: Percentage of extended transfers
- `avg_transfer_duration_minutes`: Average time for transfer
- `movements_per_transfer`: Trips completed per power change

**Calculations**:
```sql
-- Example: Repower rate
repower_rate = (extended_transfers / total_transfers) * 100

-- Example: Power efficiency score (higher is better)
efficiency_score = movements_per_transfer * (1 - repower_rate/100)
```

**Usage**:
```sql
-- Trains with excessive repowering
SELECT t.train_name, p.repower_rate, p.total_transfers
FROM agg_power_transfer_analysis p
JOIN dim_train t ON p.train_id = t.train_id
WHERE p.repower_rate > 40
ORDER BY p.repower_rate DESC;

-- Power efficiency by week
SELECT week_number, AVG(movements_per_transfer) as avg_efficiency
FROM agg_power_transfer_analysis
GROUP BY week_number
ORDER BY week_number;
```

**Refresh Frequency**: Weekly (less time-sensitive)

---

### 7. agg_seasonal_performance

**Purpose**: Track performance trends over time to identify seasonal patterns and forecast future needs.

**Grain**: One row per week (all corridors aggregated)

**Business Question**: "How does performance vary by season? Can we predict next quarter's capacity needs?"

**Key Columns**:
- `week_number`: Week of the year (1-52)
- `total_movements`: Total car movements across all corridors
- `total_trains_active`: Count of distinct trains operating
- `avg_trip_duration_days`: Average trip duration across all movements
- `total_straggler_events`: Sum of all stragglers
- `straggler_rate`: Overall straggler percentage
- `avg_queue_duration_hours`: Average queue time (all locations)
- `total_power_transfers`: Total locomotive changes

**Calculations**:
```sql
-- Example: Performance index (lower is better)
performance_index = avg_trip_duration_days * (1 + straggler_rate/100)

-- Example: Seasonal adjustment factor
seasonal_factor = week_avg / overall_avg
```

**Usage**:
```sql
-- Week 5 slowdown impact
SELECT week_number, avg_trip_duration_days, straggler_rate
FROM agg_seasonal_performance
WHERE week_number BETWEEN 4 AND 6
ORDER BY week_number;

-- Week 8 straggler spike
SELECT week_number, straggler_rate, total_straggler_events
FROM agg_seasonal_performance
WHERE week_number BETWEEN 7 AND 9
ORDER BY week_number;

-- Quarterly trend analysis
SELECT 
  CASE 
    WHEN week_number <= 13 THEN 'Q1'
    WHEN week_number <= 26 THEN 'Q2'
    ELSE 'Q3'
  END as quarter,
  AVG(avg_trip_duration_days) as avg_duration,
  AVG(straggler_rate) as avg_straggler_pct
FROM agg_seasonal_performance
GROUP BY quarter
ORDER BY quarter;
```

**Refresh Frequency**: Weekly (after week closes)

---

## Refresh Strategy

### Incremental vs Full Refresh

**Full Refresh** (rebuild entire table):
- `agg_car_utilization_metrics` - Low cardinality (228 rows), rebuild is fast
- `agg_seasonal_performance` - Low cardinality (13 weeks), rebuild is fast

**Incremental Refresh** (update only changed rows):
- `agg_corridor_weekly_metrics` - Medium cardinality, update current week
- `agg_train_daily_performance` - High cardinality, update yesterday + today
- `agg_straggler_analysis` - Medium cardinality, update affected locations/weeks
- `agg_queue_analysis` - Medium cardinality, update affected locations/weeks
- `agg_power_transfer_analysis` - Medium cardinality, update current week

### Refresh Schedule (Production Example)

```
Daily (00:30 AM):
  1. agg_train_daily_performance (yesterday)
  2. agg_queue_analysis (yesterday's locations)
  3. agg_straggler_analysis (yesterday's locations)
  
Daily (06:00 AM):
  4. agg_corridor_weekly_metrics (current week)
  5. agg_power_transfer_analysis (current week)
  
Weekly (Sunday 02:00 AM):
  6. agg_seasonal_performance (full refresh)
  7. agg_car_utilization_metrics (full refresh)
```

### Implementation Pattern

For incremental updates, use **delete + insert** pattern:

```sql
-- Example: Update agg_train_daily_performance for yesterday
BEGIN TRANSACTION;

DELETE FROM agg_train_daily_performance
WHERE date_id = (SELECT date_id FROM dim_date WHERE full_date = DATE('now', '-1 day'));

INSERT INTO agg_train_daily_performance
SELECT train_id, date_id, COUNT(*) as movements_completed, ...
FROM fact_movement
WHERE date_id = (SELECT date_id FROM dim_date WHERE full_date = DATE('now', '-1 day'))
GROUP BY train_id, date_id;

COMMIT;
```

## Metrics Validation

Before relying on metrics for business decisions, validate:

1. **Completeness**: Row counts match expected grain
   ```sql
   -- agg_corridor_weekly_metrics should have 6 corridors * 13 weeks = 78 rows
   SELECT COUNT(*) FROM agg_corridor_weekly_metrics;
   ```

2. **Accuracy**: Spot-check calculations against source facts
   ```sql
   -- Verify straggler count for C1 in week 5
   SELECT COUNT(*) FROM fact_straggler s
   JOIN fact_movement m ON s.movement_id = m.movement_id
   WHERE m.corridor_id = 'C1' AND m.week_number = 5;
   ```

3. **Freshness**: Check last refresh timestamp (if tracked)
   ```sql
   -- Optional: Add refresh_timestamp to each metrics table
   SELECT MAX(refresh_timestamp) FROM agg_train_daily_performance;
   ```

4. **Consistency**: Cross-metric validation
   ```sql
   -- Total stragglers should match across metrics
   SELECT SUM(straggler_count) FROM agg_corridor_weekly_metrics;
   SELECT SUM(straggler_events) FROM agg_train_daily_performance;
   -- These should be equal
   ```

## Extending Metrics

To add a new metric table:

1. Define **grain** (level of aggregation)
2. Identify **business question** it answers
3. List **key columns** and **calculations**
4. Choose **refresh strategy** (full vs incremental)
5. Add to **schema.yml** with generic tests
6. Create **SQL query** in profiles.yml
7. Add **documentation** to this file
8. Implement **refresh job** in production ETL

Example: Adding `agg_car_maintenance_forecast`:

```yaml
# schema.yml
- name: agg_car_maintenance_forecast
  columns:
    - name: car_id
      tests:
        - relationships:
            to: ref('dim_car')
            field: car_id
    - name: forecast_month
    - name: predicted_straggler_risk
      tests:
        - accepted_range:
            min_value: 0
            max_value: 1
```

## References

- **Fact Tables**: See [README.md](README.md#table-categories) for fact table descriptions
- **Analytics Queries**: See [README.md](README.md#analytical-queries) for analytical query examples
- **Schema Definition**: See `schema.yml` for complete table structures
- **Test Profiles**: See `profiles.yml` for metrics refresh SQL

## Contributing

When adding new metrics:
- Follow naming convention: `agg_<subject>_<aggregation_level>`
- Document business question and use cases
- Include example queries
- Add appropriate data quality tests
- Consider refresh frequency and performance impact

## License

This metrics catalog is part of the Gorchata project. See LICENSE file in the repository root.
