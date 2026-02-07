# Star Schema Example

This example demonstrates building a complete star schema data warehouse using Gorchata, showcasing common dimensional modeling patterns for e-commerce sales analytics.

## What This Example Demonstrates

This project illustrates key data transformation capabilities:

- **Star Schema Design**: Fact and dimension tables optimized for analytical queries
- **Slowly Changing Dimensions (SCD Type 2)**: Track historical changes to dimension attributes
- **Rollup Tables**: Pre-aggregate data for faster query performance
- **Incremental Materialization**: Efficiently process only new/changed data
- **Source-to-Mart Pipeline**: Complete flow from raw data to analytics-ready tables

## Business Scenario

The example models an e-commerce sales system with:

- **Sources**: Raw transaction data from an order processing system
- **Dimensions**: 
  - Customers (with SCD Type 2 for tracking changes)
  - Products (with category hierarchy)
  - Dates (calendar table for time-based analysis)
- **Facts**: 
  - Order line items with foreign keys to dimensions
  - Measures: quantity, unit price, discount, total amount
- **Rollups**: 
  - Daily sales by product category
  - Monthly customer sales summary

## Features Showcased

1. **SCD Type 2 Implementation**
   - Effective dating (valid_from, valid_to)
   - Current record flags
   - Surrogate key generation

2. **Incremental Loading**
   - Process only records modified since last run
   - Use of `{{ var('start_date') }}` and `{{ var('end_date') }}`
   - Efficient merge/upsert strategies

3. **Rollup Materialization**
   - Pre-aggregated summary tables
   - Incremental refresh patterns
   - Performance optimization

4. **Data Quality**
   - Tests for referential integrity
   - Null checks
   - Uniqueness constraints

## Directory Structure

```
star_schema_example/
├── gorchata_project.yml    # Project configuration
├── profiles.yml             # Database connection settings
├── README.md                # This file
└── models/
    ├── sources/             # Source data definitions
    ├── dimensions/          # Dimension tables
    ├── facts/               # Fact tables
    └── rollups/             # Aggregated tables
```

## How to Run

### Prerequisites

- Go 1.25 or higher
- Gorchata CLI installed (from main project)

### Setup

1. Navigate to this directory:
   ```powershell
   cd examples/star_schema_example
   ```

2. (Optional) Set custom database path:
   ```powershell
   $env:STAR_SCHEMA_DB = "./my_custom_path.db"
   ```

3. Run the pipeline:
   ```powershell
   gorchata run
   ```

### Configuration Variables

The project uses variables defined in `gorchata_project.yml`:

- `start_date`: Beginning of data processing window (default: '2024-01-01')
- `end_date`: End of data processing window (default: '2024-12-31')

Override these at runtime:
```powershell
gorchata run --vars start_date:2024-06-01 end_date:2024-06-30
```

### Database Location

By default, the SQLite database is created at:
```
./examples/star_schema_example/star_schema.db
```

Override using the `STAR_SCHEMA_DB` environment variable in `profiles.yml`.

## Testing

Run the project's test suite:
```powershell
go test ./...
```

This validates:
- Configuration files load correctly
- Directory structure exists
- Environment variable expansion works

## Next Steps

After running this example, you can:

1. Examine the generated database:
   ```powershell
   sqlite3 star_schema.db
   .tables
   .schema dim_customers
   ```

2. Query the star schema:
   ```sql
   SELECT 
       d.year,
       d.month_name,
       p.category,
       SUM(f.total_amount) as revenue
   FROM fact_order_items f
   JOIN dim_date d ON f.order_date_key = d.date_key
   JOIN dim_products p ON f.product_key = p.product_key
   GROUP BY d.year, d.month_name, p.category
   ORDER BY d.year, d.month, p.category;
   ```

3. Modify and extend:
   - Add new dimensions (stores, promotions)
   - Create additional rollups
   - Implement data quality tests

## Troubleshooting

**Database locked error**:
- Ensure no other process is accessing the database
- Close any open SQLite connections

**Tests failing**:
- Verify you're in the `examples/star_schema_example` directory
- Check that all required directories exist
- Ensure Go module dependencies are installed: `go mod download`

**Environment variable not expanding**:
- Format must be `${VAR_NAME:default_value}`
- Verify syntax in `profiles.yml`
