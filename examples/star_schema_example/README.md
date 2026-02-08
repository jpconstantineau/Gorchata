# Star Schema Example

This example demonstrates building a complete star schema data warehouse using Gorchata, showcasing common dimensional modeling patterns for e-commerce sales analytics.

## Overview

This project provides a production-ready example of transforming raw e-commerce transaction data into an analytics-ready star schema data warehouse. It demonstrates key Enterprise Data Warehouse (EDW) concepts and best practices used in modern analytics engineering.

### What This Example Demonstrates

- **Star Schema Design:** Fact and dimension tables optimized for analytical queries
- **Slowly Changing Dimensions (SCD Type 2):** Historical tracking of customer attribute changes
- **Point-in-Time Joins:** Accurate dimensional lookups using temporal validity
- **Rollup Tables:** Pre-aggregated data marts for performance optimization
- **Incremental Materialization:** Efficient processing of only new/changed data
- **Source-to-Mart Pipeline:** Complete ETL flow from raw data to analytics tables
- **Data Quality Validation:** Referential integrity and completeness checks

### Business Scenario

The example models an e-commerce sales system tracking:

**Data Sources:**
- Raw transaction data from an order processing system
- 30 sales transactions across 8 customers and 12 products
- Sales span January through December 2024

**Analytics Requirements:**
- Track revenue by product category and customer location
- Analyze sales trends over time (daily, monthly, quarterly)
- Understand customer behavior and purchase patterns
- Maintain historical accuracy when customer attributes change
- Support fast aggregated reporting for dashboards

## Schema Design

The star schema consists of 6 tables organized in a dimensional model:

### Source Layer
- **raw_sales** (30 rows): Raw sales transactions with embedded customer and product data

### Dimension Layer
- **dim_customers** (10 rows): Customer dimension with SCD Type 2 for historical tracking
- **dim_products** (12 rows): Product dimension with category hierarchy
- **dim_dates** (30 rows): Calendar dimension with time intelligence attributes

### Fact Layer
- **fct_sales** (30 rows): Sales fact table with foreign keys to dimensions

### Mart Layer
- **rollup_daily_sales** (30 rows): Pre-aggregated daily sales by category and state

For detailed schema documentation including ERD and column specifications, see [docs/schema_diagram.md](docs/schema_diagram.md).

## SCD Type 2 Explained

### What is SCD Type 2?

**Slowly Changing Dimension Type 2** is a technique for tracking historical changes to dimension attributes. Instead of updating records in place (which loses history), each change creates a new version of the record.

### Why Use SCD Type 2?

1. **Historical Accuracy:** Preserves point-in-time relationships for accurate historical reporting
2. **Auditing:** Maintains complete audit trail of attribute changes
3. **Compliance:** Required for regulated industries (finance, healthcare, etc.)
4. **Analysis:** Enables questions like "What was revenue from Seattle customers in Q1?"

### How It Works in This Example

Customer 1001 (Alice Johnson) has **3 versions** demonstrating the SCD Type 2 pattern:

| Version | customer_sk | City     | State | valid_from | valid_to   | is_current |
|---------|-------------|----------|-------|------------|------------|------------|
| 1       | 1001001     | Seattle  | WA    | 2024-01-05 | 2024-06-10 | 0          |
| 2       | 1001002     | Portland | OR    | 2024-06-10 | 2024-11-08 | 0          |
| 3       | 1001003     | Portland | OR    | 2024-11-08 | 9999-12-31 | 1          |

**Key Elements:**

- **Surrogate Key:** `customer_sk` uniquely identifies each version (natural_key × 1000 + version_number)
- **Natural Key:** `customer_id` identifies the customer across all versions
- **Effective Dating:** `valid_from` and `valid_to` mark when each version was active
- **Current Flag:** `is_current` indicates the latest version (1=current, 0=historical)

**Point-in-Time Join:**

The fact table uses a temporal join to select the correct customer version:
```sql
FROM fct_sales f
JOIN dim_customers c 
  ON f.customer_id = c.customer_id
  AND f.sale_date >= c.valid_from 
  AND f.sale_date < c.valid_to
```

A sale on 2024-03-15 correctly joins to version 1 (Seattle), while a sale on 2024-08-20 joins to version 2 (Portland).

## Directory Structure

```
star_schema_example/
├── gorchata_project.yml    # Project configuration
├── profiles.yml             # Database connection settings
├── README.md                # This file
├── star_schema_example_test.go  # Comprehensive test suite
├── verify_data_integrity.sql    # Manual validation queries
├── docs/
│   └── schema_diagram.md    # Detailed schema documentation with ERD
└── models/
    ├── sources/
    │   └── raw_sales.sql    # Source data (inline seed data)
    ├── dimensions/
    │   ├── dim_customers.sql  # SCD Type 2 customer dimension
    │   ├── dim_products.sql   # Product dimension
    │   └── dim_dates.sql      # Date dimension
    ├── facts/
    │   └── fct_sales.sql      # Sales fact table
    └── rollups/
        └── rollup_daily_sales.sql  # Aggregated reporting mart
```

## How to Run

### Prerequisites

- **Go 1.25+** installed and in PATH
- **Gorchata CLI** installed from main project:
  ```powershell
  cd c:\Users\pierre\git\Gorchata
  go install ./cmd/gorchata
  ```

### Quick Start

1. Navigate to this directory:
   ```powershell
   cd examples/star_schema_example
   ```

2. Run the transformation pipeline:
   ```powershell
   gorchata run
   ```

   This will:
   - Create the database at `./star_schema.db` (or path from env var)
   - Execute all models in dependency order
   - Create all 6 tables with sample data

3. Verify the results:
   ```powershell
   sqlite3 star_schema.db
   ```
   ```sql
   .tables
   SELECT COUNT(*) FROM fct_sales;
   ```

### Run Tests

Execute the comprehensive test suite:

```powershell
go test -v .
```

This runs:
- Configuration validation tests
- Individual model execution tests
- SCD Type 2 validation tests
- End-to-end integration test with full pipeline

For just the integration test:
```powershell
go test -v -run TestEndToEndIntegration
```

### Run Data Quality Tests

Execute data quality tests using Gorchata's testing framework:

```bash
# Run all data quality tests
gorchata test

# Run specific test
gorchata test --select test_fact_integrity

# Run tests on specific model
gorchata test --select dim_customers

# Run with verbose output
gorchata test --verbose

# Store failures for analysis
gorchata test --store-failures
```

See the [Data Quality Tests](#data-quality-tests) section below for detailed test documentation.

### Configuration Variables

The project uses variables defined in [gorchata_project.yml](gorchata_project.yml):

```yaml
vars:
  start_date: '2024-01-01'
  end_date: '2024-12-31'
```

These variables can be used in models for incremental processing:
```sql
WHERE sale_date >= '{{ var('start_date') }}'
  AND sale_date <= '{{ var('end_date') }}'
```

Override at runtime:
```powershell
gorchata run --vars start_date:2024-06-01 end_date:2024-06-30
```

### Database Location

**Default Path:**
```
./star_schema.db
```

**Custom Path via Environment Variable:**

In [profiles.yml](profiles.yml):
```yaml
dev:
  type: sqlite
  database: ${STAR_SCHEMA_DB:./star_schema.db}
```

Set custom location:
```powershell
$env:STAR_SCHEMA_DB = "C:/custom/path/my_warehouse.db"
gorchata run
```

## Sample Queries

### 1. View SCD Type 2 History

See how customer attributes changed over time:

```sql
SELECT 
    customer_sk,
    customer_id,
    customer_name,
    customer_city,
    customer_state,
    valid_from,
    valid_to,
    is_current
FROM dim_customers
WHERE customer_id = 1001
ORDER BY valid_from;
```

**Result:**
```
customer_sk | customer_id | customer_name  | customer_city | customer_state | valid_from | valid_to   | is_current
------------|-------------|----------------|---------------|----------------|------------|------------|------------
1001001     | 1001        | Alice Johnson  | Seattle       | WA             | 2024-01-05 | 2024-06-10 | 0
1001002     | 1001        | Alice Johnson  | Portland      | OR             | 2024-06-10 | 2024-11-08 | 0
1001003     | 1001        | Alice Johnson  | Portland      | OR             | 2024-11-08 | 9999-12-31 | 1
```

## Data Quality Tests

This example includes comprehensive data quality tests following DBT testing patterns. These tests ensure data integrity, referential relationships, and business rule compliance.

### Generic Tests (via models/schema.yml)

**Dimension Table Tests:**

1. **dim_customers** (SCD Type 2): 11 tests
   - Primary key: unique, not_null on customer_sk
   - Natural key: not_null on customer_id
   - Attribute validation: not_null, not_empty_string on name, email, city, state
   - SCD Type 2 fields: not_null on valid_from, valid_to
   - Current flag: accepted_values [0, 1] on is_current
   - Table-level: at_least_one (ensure data exists)

2. **dim_products**: 5 tests
   - Primary key: unique, not_null on product_id
   - Name validation: not_null, not_empty_string
   - Category validation: accepted_values ['Electronics', 'Clothing', 'Food', 'Books']
   - Price validation: not_null, accepted_range [0-10000]

3. **dim_dates**: 9 tests
   - Primary key: unique, not_null on sale_date
   - Year validation: accepted_range [2020-2030]
   - Quarter validation: accepted_values [1, 2, 3, 4]
   - Month validation: accepted_range [1-12]
   - Day validation: accepted_range [1-31]
   - Weekend flag: accepted_values [0, 1]

**Fact Table Tests:**

4. **fct_sales**: 9 tests
   - Primary key: unique, not_null on sale_id
   - Foreign keys: not_null + relationships for customer_sk, product_id, sale_date
   - Measures: not_null on sale_amount, quantity
   - Business rules:
     - sale_amount: accepted_range [0-1000000]
     - quantity: accepted_range [1-1000]
   - Table-level:
     - at_least_one (ensure data exists)
     - recency: data within 365 days of current date

### Singular Tests (custom SQL)

**test_fact_integrity.sql**
- **Purpose**: Comprehensive fact table validation with 6 integrity checks
- **Checks**:
  1. **Orphaned Sales**: Detects sales with missing customer, product, or date references
  2. **Invalid Amounts**: Identifies negative, zero, or unreasonably high sale amounts
  3. **Invalid Quantities**: Detects non-positive or non-integer quantities
  4. **SCD Type 2 Integrity**: Verifies point-in-time joins are correct (sale_date within customer version validity)
  5. **Duplicate Sales**: Detects grain violations (duplicate sale_id)
  6. **Price Consistency**: Validates sale_amount vs (product_price × quantity) within reasonable bounds
     - Allows up to 50% discount
     - Detects overcharges >10%

### Test Coverage Summary

| Layer | Models | Tests | Coverage |
|-------|--------|-------|----------|
| Dimensions | 3 | 25 | Primary keys, attributes, SCD logic |
| Facts | 1 | 9 | Keys, measures, relationships |
| Singular | 1 | 6 | Business rules, integrity |
| **Total** | **5** | **40+** | **Comprehensive** |

### Expected Test Results

With the provided sample data (30 sales, 10 customer versions, 12 products):
- **All generic tests should PASS**: Data is clean and follows all constraints
- **Singular test should PASS**: No integrity violations in sample data

To test failure scenarios, you can:
1. Modify `raw_sales` data to introduce violations
2. Run tests again to see failure detection
3. Examine failure details in the output

### Test-Driven Development

These tests follow TDD principles:
1. ✅ Tests written first (Phase 8)
2. ✅ Tests run and verify data quality
3. ✅ Tests serve as living documentation of data contracts

When extending this example:
1. Add new tests to `schema.yml` for new columns/models
2. Create singular tests for complex business rules
3. Run tests before and after changes
4. Use test failures to guide data quality improvements

### 2. Star Schema Join: Monthly Revenue by Category

```sql
SELECT 
    d.year,
    d.month,
    p.product_category,
    SUM(f.sale_amount) as revenue,
    COUNT(*) as sale_count,
    AVG(f.sale_amount) as avg_sale
FROM fct_sales f
INNER JOIN dim_dates d ON f.sale_date = d.sale_date
INNER JOIN dim_products p ON f.product_id = p.product_id
INNER JOIN dim_customers c ON f.customer_sk = c.customer_sk
WHERE d.year = 2024
GROUP BY d.year, d.month, p.product_category
ORDER BY d.month, revenue DESC;
```

### 3. Rollup Table Query: Category Performance Summary

Fast aggregated query using pre-computed rollup:

```sql
SELECT 
    product_category,
    SUM(total_sales) as category_revenue,
    SUM(total_quantity) as units_sold,
    AVG(avg_sale_amount) as avg_transaction,
    SUM(sale_count) as transaction_count
FROM rollup_daily_sales
GROUP BY product_category
ORDER BY category_revenue DESC;
```

### 4. Time Intelligence: Quarter-over-Quarter Trends

```sql
SELECT 
    d.year,
    d.quarter,
    SUM(f.sale_amount) as revenue,
    COUNT(DISTINCT f.customer_sk) as unique_customers
FROM fct_sales f
INNER JOIN dim_dates d ON f.sale_date = d.sale_date
GROUP BY d.year, d.quarter
ORDER BY d.year, d.quarter;
```

### 5. Customer Cohort Analysis

```sql
SELECT 
    c.customer_state,
    COUNT(DISTINCT c.customer_id) as customer_count,
    SUM(f.sale_amount) as state_revenue,
    AVG(f.sale_amount) as avg_per_sale
FROM fct_sales f
INNER JOIN dim_customers c ON f.customer_sk = c.customer_sk
WHERE c.is_current = 1
GROUP BY c.customer_state
ORDER BY state_revenue DESC;
```

## Learning Objectives

After working through this example, you will understand:

1. **Dimensional Modeling Fundamentals**
   - Star schema design principles
   - Fact vs dimension table characteristics
   - Grain selection and definition

2. **SCD Type 2 Implementation**
   - How to track historical attribute changes
   - Surrogate key vs natural key usage
   - Point-in-time temporal joins
   - Effective dating patterns

3. **Data Warehouse Design Patterns**
   - Source → Dimension → Fact → Mart pipeline
   - Incremental vs full refresh strategies
   - Pre-aggregation for performance

4. **Testing Data Pipelines**
   - Unit tests for individual models
   - Integration tests for full pipeline
   - Data quality validation patterns

5. **Analytics Engineering with Gorchata**
   - Project and profile configuration
   - Model dependencies via `{{ ref }}`
   - Variable interpolation `{{ var() }}`
   - Jinja-like templating

## Extending This Example

Ideas for further exploration:

### Add New Dimensions
- **dim_stores:** Store location dimension
- **dim_promotions:** Promotion/discount dimension
- **dim_time:** Time-of-day dimension for intraday analysis

### Create New Facts
- **fct_returns:** Product returns fact
- **fct_inventory:** Inventory snapshot fact
- **fct_customer_behavior:** Web clickstream fact

### Build Additional Rollups
- **rollup_monthly_customer:** Monthly customer purchase summary
- **rollup_product_performance:** Product-level KPIs
- **rollup_hourly_sales:** Real-time sales aggregation

### Implement Data Quality Tests
- Referential integrity checks
- Null value validation
- Duplicate detection
- Business rule validation (e.g., sale_amount > 0)

### Add Incremental Processing
Modify models to use incremental materialization:
```sql
{{ config(
    materialized='incremental',
    unique_key=['sale_id']
) }}

SELECT ...
FROM {{ ref('raw_sales') }}
{% if is_incremental() %}
WHERE sale_date > (SELECT MAX(sale_date) FROM {{ this }})
{% endif %}
```

## Troubleshooting

### Database Locked Error
**Symptom:** `database is locked` error during execution

**Solutions:**
- Close any open SQLite connections or database browsers
- Ensure no other Gorchata process is running
- Delete the database file and recreate: `rm star_schema.db; gorchata run`

### Tests Failing
**Symptom:** `go test` shows failures

**Solutions:**
- Verify you're in `examples/star_schema_example` directory
- Check Go version: `go version` (requires 1.25+)
- Install dependencies: `go mod download`
- Run verbose: `go test -v .` to see detailed output

### Environment Variable Not Expanding
**Symptom:** Database created at literal path `${STAR_SCHEMA_DB:./star_schema.db}`

**Solutions:**
- Verify syntax: `${VAR_NAME:default_value}` with colon
- Check profiles.yml formatting (YAML indentation)
- Set variable before running: `$env:STAR_SCHEMA_DB = "./custom.db"`

### Gorchata Command Not Found
**Symptom:** `gorchata: The term is not recognized`

**Solutions:**
- Install from main repo: `go install ./cmd/gorchata`
- Verify GOPATH/bin is in PATH: `echo $env:PATH`
- Use full path: `$env:GOPATH/bin/gorchata run`

### Wrong Row Counts
**Symptom:** Tables have unexpected number of rows

**Solutions:**
- Drop and recreate: `rm star_schema.db; gorchata run`
- Check model SQL for logic errors
- Verify joins aren't filtering unexpectedly
- Run data quality checks in verify_data_integrity.sql

## Additional Resources

- **Schema Documentation:** [docs/schema_diagram.md](docs/schema_diagram.md)
- **Data Quality Queries:** [verify_data_integrity.sql](verify_data_integrity.sql)
- **Project Config:** [gorchata_project.yml](gorchata_project.yml)
- **Connection Config:** [profiles.yml](profiles.yml)
- **Test Suite:** [star_schema_example_test.go](star_schema_example_test.go)

### External References

- [Star Schema Design - Wikipedia](https://en.wikipedia.org/wiki/Star_schema)
- [Slowly Changing Dimensions - Wikipedia](https://en.wikipedia.org/wiki/Slowly_changing_dimension)
- [Dimensional Modeling - Kimball](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/)
- [SQLite Documentation](https://www.sqlite.org/docs.html)

## License

This example is part of the Gorchata project. See [LICENSE](../../LICENSE) for details.
