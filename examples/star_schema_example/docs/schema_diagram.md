# Star Schema - Entity Relationship Diagram

This document provides a visual representation of the star schema data warehouse design for the e-commerce sales analytics example.

## Schema Overview

The schema follows the classic **star schema** pattern with:
- **1 Source Table** (raw_sales)
- **3 Dimension Tables** (dim_customers, dim_products, dim_dates)
- **1 Fact Table** (fct_sales)
- **1 Rollup Table** (rollup_daily_sales)

## Full Schema Diagram

```
                                    ┌─────────────────────────────────┐
                                    │       raw_sales (Source)        │
                                    │─────────────────────────────────│
                                    │ sale_id (PK)                    │
                                    │ sale_date                       │
                                    │ sale_amount                     │
                                    │ quantity                        │
                                    │ customer_id                     │
                                    │ customer_name                   │
                                    │ customer_email                  │
                                    │ customer_city                   │
                                    │ customer_state                  │
                                    │ product_id                      │
                                    │ product_name                    │
                                    │ product_category                │
                                    │ product_price                   │
                                    └──────────────┬──────────────────┘
                                                   │
                       ┌───────────────────────────┼───────────────────────────┐
                       │                           │                           │
                       ▼                           ▼                           ▼
        ┌──────────────────────────┐  ┌────────────────────────┐  ┌──────────────────────────┐
        │   dim_customers (SCD2)   │  │     dim_products       │  │       dim_dates          │
        │──────────────────────────│  │────────────────────────│  │──────────────────────────│
        │ customer_sk (PK) *       │  │ product_id (PK)        │  │ sale_date (PK)           │
        │ customer_id              │  │ product_name           │  │ year                     │
        │ customer_name            │  │ product_category       │  │ quarter                  │
        │ customer_email           │  │ product_price          │  │ month                    │
        │ customer_city            │  └────────────┬───────────┘  │ day                      │
        │ customer_state           │               │              │ day_of_week              │
        │ valid_from *             │               │              │ is_weekend               │
        │ valid_to *               │               │              └──────────┬───────────────┘
        │ is_current *             │               │                         │
        └────────────┬─────────────┘               │                         │
                     │                             │                         │
                     │                             │                         │
                     └─────────────┐               │         ┌───────────────┘
                                   │               │         │
                                   ▼               ▼         ▼
                        ┌──────────────────────────────────────────────┐
                        │          fct_sales (Fact Table)              │
                        │──────────────────────────────────────────────│
                        │ sale_id (PK)                                 │
                        │ customer_sk (FK) → dim_customers.customer_sk │
                        │ product_id (FK) → dim_products.product_id    │
                        │ sale_date (FK) → dim_dates.sale_date         │
                        │ sale_amount                                  │
                        │ quantity                                     │
                        └──────────────────┬───────────────────────────┘
                                           │
                                           │ (aggregated by date,
                                           │  category, state)
                                           ▼
                        ┌──────────────────────────────────────────────┐
                        │    rollup_daily_sales (Aggregated Mart)     │
                        │──────────────────────────────────────────────│
                        │ sale_date                                    │
                        │ year                                         │
                        │ month                                        │
                        │ product_category                             │
                        │ customer_state                               │
                        │ total_sales (SUM)                            │
                        │ total_quantity (SUM)                         │
                        │ sale_count (COUNT)                           │
                        │ avg_sale_amount (AVG)                        │
                        └──────────────────────────────────────────────┘
```

## Table Details

### raw_sales (Source)
**Purpose:** Raw transactional data from the order processing system

**Row Count:** 30 sales transactions

**Columns:**
- `sale_id` - Unique transaction identifier
- `sale_date` - Date of sale
- `sale_amount` - Total amount of sale
- `quantity` - Number of items sold
- `customer_id` - Customer identifier
- `customer_name` - Customer full name
- `customer_email` - Customer email address
- `customer_city` - Customer city
- `customer_state` - Customer state
- `product_id` - Product identifier
- `product_name` - Product name
- `product_category` - Product category
- `product_price` - Unit price

---

### dim_customers (Dimension - SCD Type 2)
**Purpose:** Customer dimension with historical tracking of attribute changes

**Materialization:** Incremental (SCD Type 2 pattern)

**Row Count:** 10 rows (8 unique customers + 2 historical versions)

**Special Features:**
- ⭐ **SCD Type 2 Implementation**
- Tracks changes to customer attributes (city, state, email) over time
- Each change creates a new version with distinct surrogate key

**Columns:**
- `customer_sk` (Surrogate Key) - Unique identifier for each customer version (customer_id * 1000 + version_num)
- `customer_id` (Natural Key) - Business identifier for the customer
- `customer_name` - Customer full name
- `customer_email` - Customer email (tracked for changes)
- `customer_city` - Customer city (tracked for changes)
- `customer_state` - Customer state (tracked for changes)
- `valid_from` - Start date when this version became active
- `valid_to` - End date when this version was superseded (9999-12-31 for current)
- `is_current` - Flag indicating current version (1=current, 0=historical)

**Example:** Customer 1001 has 3 versions:
1. Version 1 (SK=1001001): Seattle, WA - Valid 2024-01-05 to 2024-06-10
2. Version 2 (SK=1001002): Portland, OR - Valid 2024-06-10 to 2024-11-08
3. Version 3 (SK=1001003): Portland, OR - Valid 2024-11-08 to 9999-12-31 (current)

---

### dim_products (Dimension)
**Purpose:** Product dimension with category hierarchy

**Materialization:** Table

**Row Count:** 12 unique products

**Columns:**
- `product_id` (Primary Key) - Unique product identifier
- `product_name` - Product name
- `product_category` - Product category (Accessories, Electronics, Furniture)
- `product_price` - Standard unit price

**Categories:**
- Accessories (USB cables, mice, keyboards, laptop stands, webcams)
- Electronics (laptops, monitors, tablets, printers)
- Furniture (desks, chairs, lamps)

---

### dim_dates (Dimension)
**Purpose:** Time dimension for date-based analysis

**Materialization:** Table

**Row Count:** 30 unique dates (one per sale)

**Columns:**
- `sale_date` (Primary Key) - Date value
- `year` - Year (2024)
- `quarter` - Quarter of year (1-4)
- `month` - Month number (1-12)
- `day` - Day of month (1-31)
- `day_of_week` - Day of week (0=Sunday, 6=Saturday)
- `is_weekend` - Weekend flag (1=weekend, 0=weekday)

**Date Range:** January 2024 through December 2024

---

### fct_sales (Fact Table)
**Purpose:** Fact table recording sales transactions with point-in-time dimensional relationships

**Materialization:** Table

**Row Count:** 30 sales (one per source transaction)

**Special Features:**
- ⭐ **Point-in-Time Join to SCD Type 2:** Uses temporal join to find the correct customer version
  ```sql
  AND sale_date >= valid_from AND sale_date < valid_to
  ```
- Denormalized with dimensional foreign keys for fast query performance

**Columns:**
- `sale_id` - Transaction identifier (from source)
- `customer_sk` (Foreign Key) - References dim_customers.customer_sk (correct version for sale date)
- `product_id` (Foreign Key) - References dim_products.product_id
- `sale_date` (Foreign Key) - References dim_dates.sale_date
- `sale_amount` - Total transaction amount
- `quantity` - Number of items sold

**Grain:** One row per sale transaction (sale line item level)

---

### rollup_daily_sales (Aggregated Mart)
**Purpose:** Pre-aggregated daily sales by product category and customer state

**Materialization:** Table

**Row Count:** 30 rows (varies based on aggregation grouping)

**Aggregation Level:** Daily × Category × State

**Columns:**
- `sale_date` - Date dimension
- `year` - Year (for partitioning)
- `month` - Month (for partitioning)
- `product_category` - Product category dimension
- `customer_state` - Customer state dimension
- `total_sales` - Sum of sale amounts
- `total_quantity` - Sum of quantities sold
- `sale_count` - Count of transactions
- `avg_sale_amount` - Average sale amount

**Use Cases:**
- Fast category performance reporting
- State-level sales analysis
- Daily aggregated trends

---

## Relationships

### Foreign Key Relationships

1. **fct_sales → dim_customers**
   - `fct_sales.customer_sk` → `dim_customers.customer_sk`
   - Type: Many-to-One
   - Constraint: Point-in-time join using valid_from/valid_to

2. **fct_sales → dim_products**
   - `fct_sales.product_id` → `dim_products.product_id`
   - Type: Many-to-One

3. **fct_sales → dim_dates**
   - `fct_sales.sale_date` → `dim_dates.sale_date`
   - Type: Many-to-One

### Data Flow

```
raw_sales (30 rows)
    │
    ├──→ dim_customers (10 rows with SCD Type 2)
    ├──→ dim_products (12 rows)
    └──→ dim_dates (30 rows)
           │
           └──→ fct_sales (30 rows)
                   │
                   └──→ rollup_daily_sales (30 aggregated rows)
```

## Query Patterns

### Simple Dimension Query
```sql
-- View all customer versions
SELECT * FROM dim_customers
ORDER BY customer_id, valid_from;
```

### Star Schema Join (Fact + Dimensions)
```sql
-- Monthly sales by category
SELECT 
    d.year,
    d.month,
    p.product_category,
    SUM(f.sale_amount) as revenue,
    COUNT(*) as sale_count
FROM fct_sales f
INNER JOIN dim_dates d ON f.sale_date = d.sale_date
INNER JOIN dim_products p ON f.product_id = p.product_id
GROUP BY d.year, d.month, p.product_category
ORDER BY d.year, d.month, revenue DESC;
```

### SCD Type 2 Historical Query
```sql
-- View customer 1001's address history
SELECT 
    customer_sk,
    customer_city,
    customer_state,
    valid_from,
    valid_to,
    is_current
FROM dim_customers
WHERE customer_id = 1001
ORDER BY valid_from;
```

### Rollup Query (Fast Aggregation)
```sql
-- Category performance summary
SELECT 
    product_category,
    SUM(total_sales) as category_total,
    SUM(total_quantity) as total_units,
    AVG(avg_sale_amount) as avg_transaction
FROM rollup_daily_sales
GROUP BY product_category
ORDER BY category_total DESC;
```

## Design Decisions

### Why SCD Type 2 for Customers?
- Enables historical analysis (e.g., "What was revenue from Seattle customers?")
- Preserves point-in-time accuracy for reporting
- Supports auditing and compliance requirements

### Why Surrogate Keys?
- `customer_sk = customer_id * 1000 + version_num` ensures uniqueness across versions
- Fact table remains stable even if natural keys change
- Enables multiple versions of same customer

### Why Rollup Tables?
- Pre-aggregated data speeds up common queries
- Reduces load on fact table for reporting
- Can be incrementally refreshed for efficiency

### Grain Selection
- **Fact Table Grain:** Sale transaction line item
- **Rollup Grain:** Daily × Category × State
- Granularity supports both detailed and summary analysis

## Performance Considerations

1. **Indexes** (if implementing in production):
   - `dim_customers(customer_sk)` - Primary key
   - `dim_customers(customer_id, valid_from)` - SCD Type 2 lookups
   - `fct_sales(customer_sk)`, `fct_sales(product_id)`, `fct_sales(sale_date)` - Foreign keys
   - `rollup_daily_sales(sale_date, product_category)` - Common filters

2. **Partitioning** (for larger datasets):
   - Partition `fct_sales` by month
   - Partition `rollup_daily_sales` by year/month

3. **Incremental Processing:**
   - Use `{{ var('start_date') }}` and `{{ var('end_date') }}` for date-windowed processing
   - SCD Type 2 supports merge/upsert patterns

## References

- [Star Schema Design](https://en.wikipedia.org/wiki/Star_schema)
- [Slowly Changing Dimensions](https://en.wikipedia.org/wiki/Slowly_changing_dimension)
- [Dimensional Modeling](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/)
