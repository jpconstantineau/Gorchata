## Plan: Star Schema Example Project

Create a comprehensive example project demonstrating data transformation from a denormalized single table into a star schema with fact and dimension tables. This example will showcase Gorchata's capabilities for dimensional modeling including SCD Type 2, incremental loading, and aggregate rollup tables using SQLite and different materialization strategies.

**Phases: 8**

### Phase 1: Project Scaffolding & Configuration
- **Objective:** Create the directory structure and configuration files for the star_schema_example project in new examples/ folder
- **Files/Functions to Modify/Create:**
  - `examples/star_schema_example/gorchata_project.yml` - Project configuration
  - `examples/star_schema_example/profiles.yml` - Connection profile
  - `examples/star_schema_example/models/` - Directory for model SQL files
  - `examples/star_schema_example/seeds/` - Directory for seed data
  - `examples/star_schema_example/README.md` - Documentation
- **Tests to Write:**
  - Test gorchata_project.yml can be loaded successfully
  - Test profiles.yml can be parsed and environment variables expanded
  - Verify directory structure exists
- **Steps:**
  1. Write tests for config file validation (fail)
  2. Create examples/ directory and star_schema_example subdirectory
  3. Create gorchata_project.yml with project name, version, paths, and variables
  4. Create profiles.yml with SQLite connection for example database
  5. Create directory structure (models/sources/, models/dimensions/, models/facts/, models/rollups/)
  6. Create README.md documenting the example purpose and schema design
  7. Run tests to verify configs load correctly (pass)

### Phase 2: Source Data Setup (Denormalized Table)
- **Objective:** Create the raw denormalized sales data as a simulated source table that will be transformed
- **Files/Functions to Modify/Create:**
  - `examples/star_schema_example/models/sources/raw_sales.sql` - Source denormalized table
  - `examples/star_schema_example/seeds/raw_sales_seed.sql` - Seed data script with temporal data
- **Tests to Write:**
  - Test raw_sales model creates table with expected columns
  - Test seed data contains sample records across multiple time periods
  - Verify denormalized structure has redundancy (customer/product details repeated)
  - Test includes records showing customer attribute changes over time (for SCD Type 2)
- **Steps:**
  1. Write test for raw_sales model existence and schema (fail)
  2. Create raw_sales.sql model with config(materialized='table')
  3. Model should simulate a denormalized e-commerce sales table with columns:
     - sale_id, sale_date, sale_amount, quantity
     - customer_id, customer_name, customer_email, customer_city, customer_state
     - product_id, product_name, product_category, product_price
  4. Create seed SQL with 20-30 sample records showing data redundancy and temporal changes
  5. Include customer records where city/state changes over time (for SCD Type 2 demo)
  6. Run tests to verify table creation and data (pass)
  7. Build and inspect output database

### Phase 3: Dimension Tables - Products & Dates
- **Objective:** Extract and deduplicate product and date information into dimension tables
- **Files/Functions to Modify/Create:**
  - `examples/star_schema_example/models/dimensions/dim_products.sql` - Product dimension
  - `examples/star_schema_example/models/dimensions/dim_dates.sql` - Date dimension
- **Tests to Write:**
  - Test dim_products extracts unique products with all attributes
  - Test dim_dates creates one row per unique date with time attributes
  - Test dimensions have no duplicates on natural keys
  - Test ref() function creates dependencies on raw_sales
- **Steps:**
  1. Write tests for dimension table creation and uniqueness (fail)
  2. Create dim_products.sql using {{ ref "raw_sales" }} to extract unique products
  3. Use SELECT DISTINCT or GROUP BY to deduplicate
  4. Configure materialization as 'table'
  5. Create dim_dates.sql that extracts unique dates from {{ ref "raw_sales" }}
  6. Add calculated fields: year, quarter, month, day, day_of_week, is_weekend
  7. Run tests to verify deduplication and ref() dependencies (pass)
  8. Build and verify dimensions in output database

### Phase 4: SCD Type 2 Customer Dimension
- **Objective:** Implement Slowly Changing Dimension Type 2 for customer dimension with history tracking
- **Files/Functions to Modify/Create:**
  - `examples/star_schema_example/models/dimensions/dim_customers.sql` - Customer dimension with SCD Type 2
- **Tests to Write:**
  - Test dim_customers uses incremental materialization with unique_key
  - Test multiple versions of same customer_id exist when attributes change
  - Test valid_from and valid_to timestamps track history
  - Test is_current flag identifies active records
  - Test surrogate key (customer_sk) is unique across all versions
- **Steps:**
  1. Write tests for SCD Type 2 dimension structure and versioning (fail)
  2. Create dim_customers.sql with config(materialized='incremental', unique_key=['customer_sk'])
  3. Implement logic to detect attribute changes (city, state, email)
  4. Add SCD Type 2 columns: customer_sk (surrogate), valid_from, valid_to, is_current
  5. Use ROW_NUMBER() or similar to create versions when attributes change
  6. Set is_current = 1 for latest version, 0 for historical
  7. Run tests to verify versioning and history tracking (pass)
  8. Build and verify customer history in output database

### Phase 5: Fact Table - Sales
- **Objective:** Create the central fact table with measures and foreign keys to dimensions
- **Files/Functions to Modify/Create:**
  - `examples/star_schema_example/models/facts/fct_sales.sql` - Sales fact table
- **Tests to Write:**
  - Test fct_sales references all dimension tables via {{ ref }}
  - Test fact table contains measures (sale_amount, quantity)
  - Test foreign keys exist (date_key, customer_sk, product_id)
  - Test joins to dim_customers use valid_from/valid_to for point-in-time accuracy
  - Test grain is one row per sale
  - Verify DAG execution order: dimensions before fact
- **Steps:**
  1. Write tests for fact table structure and dependencies (fail)
  2. Create fct_sales.sql that joins raw_sales with all dimension tables
  3. Use {{ ref "dim_customers" }}, {{ ref "dim_products" }}, {{ ref "dim_dates" }}
  4. Implement point-in-time join to dim_customers using sale_date BETWEEN valid_from AND valid_to
  5. Select only keys and measures (denormalized attributes removed)
  6. Configure materialization as 'table'
  7. Run tests to verify fact table and DAG ordering (pass)
  8. Build and verify star schema is complete with SCD Type 2 working

### Phase 6: Aggregate Rollup Table
- **Objective:** Create an aggregated rollup table for faster analytical queries
- **Files/Functions to Modify/Create:**
  - `examples/star_schema_example/models/rollups/rollup_daily_sales.sql` - Daily sales aggregates
- **Tests to Write:**
  - Test rollup_daily_sales references fct_sales via {{ ref }}
  - Test aggregations: SUM(sale_amount), SUM(quantity), COUNT(sale_id)
  - Test grain is one row per date + product + customer state
  - Test rollup reduces row count significantly vs fact table
  - Verify queries against rollup match queries against fact table
- **Steps:**
  1. Write tests for rollup table structure and aggregation correctness (fail)
  2. Create rollup_daily_sales.sql using {{ ref "fct_sales" }} as source
  3. Join to dimensions to get grouping attributes (date, product_category, customer_state)
  4. GROUP BY sale_date, product_category, customer_state
  5. Calculate aggregates: total_sales, total_quantity, sale_count, avg_sale_amount
  6. Configure materialization as 'table'
  7. Run tests comparing rollup to raw fact aggregations (pass)
  8. Build and verify rollup performance benefits

### Phase 7: Integration Test & Documentation
- **Objective:** Create end-to-end integration test and complete documentation
- **Files/Functions to Modify/Create:**
  - `examples/star_schema_example/star_schema_integration_test.go` - Integration test
  - `examples/star_schema_example/README.md` - Complete documentation update
  - `examples/star_schema_example/docs/schema_diagram.md` - Visual schema diagram
- **Tests to Write:**
  - Integration test that runs full gorchata pipeline on example
  - Test verifies all 6 tables created (raw_sales, 3 dims, 1 fact, 1 rollup)
  - Test row counts match expectations
  - Test joins between fact and dimensions work correctly
  - Test SCD Type 2 history query returns correct customer versions
  - Test rollup query performance and accuracy
  - Test demonstrates querying the star schema
- **Steps:**
  1. Write integration test that executes gorchata run on project (fail)
  2. Implement test using temp SQLite database
  3. Test should verify table existence, row counts, and sample queries
  4. Update README.md with complete documentation:
     - Star schema concepts and benefits
     - SCD Type 2 explanation with examples
     - Rollup table purpose and use cases
     - Before/after comparison (denormalized vs dimensional)
     - How to run the example
     - Sample queries demonstrating all features
  5. Create schema_diagram.md with ASCII/markdown ERD showing all relationships
  6. Run integration test (pass)
  7. Build and run example, verify all docs are accurate

### Phase 8: Repository Integration
- **Objective:** Integrate the example into the repository structure with proper references
- **Files/Functions to Modify/Create:**
  - `README.md` (root) - Add link to star schema example
  - `examples/README.md` - Create examples index if needed
- **Tests to Write:**
  - Test that example is discoverable from main README
  - Verify all relative links work correctly
- **Steps:**
  1. Write test for README link validation (fail)
  2. Update root README.md to reference examples/star_schema_example
  3. Create examples/README.md index if it doesn't exist
  4. Add badges or callouts highlighting key features demonstrated
  5. Run tests to verify links (pass)
  6. Final build and documentation review
