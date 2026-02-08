# Generic Test Examples

This directory contains examples demonstrating the usage of generic data quality tests.

## Example 1: Basic Usage

```go
package main

import (
	"fmt"
	"github.com/jpconstantineau/gorchata/internal/domain/test/generic"
)

func main() {
	// Create registry with all tests
	registry := generic.NewDefaultRegistry()
	
	// Get a specific test
	notNullTest, _ := registry.Get("not_null")
	
	// Generate SQL
	sql, _ := notNullTest.GenerateSQL("users", "email", nil)
	fmt.Println(sql)
	// Output: SELECT * FROM users WHERE email IS NULL
}
```

## Example 2: Test with Arguments

```go
// accepted_values test
acceptedValuesTest, _ := registry.Get("accepted_values")
sql, _ := acceptedValuesTest.GenerateSQL("orders", "status", map[string]interface{}{
	"values": []interface{}{"pending", "shipped", "delivered"},
})
// Output: SELECT * FROM orders WHERE status NOT IN ('pending', 'shipped', 'delivered') AND status IS NOT NULL
```

## Example 3: Relationships (Foreign Key)

```go
relationshipsTest, _ := registry.Get("relationships")
sql, _ := relationshipsTest.GenerateSQL("orders", "user_id", map[string]interface{}{
	"to":    "users",
	"field": "id",
})
// Output: SELECT * FROM orders WHERE user_id NOT IN (SELECT id FROM users) AND user_id IS NOT NULL
```

## Example 4: Multi-column Uniqueness

```go
uniqueCombinationTest, _ := registry.Get("unique_combination_of_columns")
sql, _ := uniqueCombinationTest.GenerateSQL("users", "", map[string]interface{}{
	"columns": []interface{}{"first_name", "last_name", "birthdate"},
})
// Output: SELECT first_name, last_name, birthdate, COUNT(*) as duplicate_count
//         FROM users
//         GROUP BY first_name, last_name, birthdate
//         HAVING COUNT(*) > 1
```

## Example 5: Recency Check

```go
recencyTest, _ := registry.Get("recency")
sql, _ := recencyTest.GenerateSQL("events", "created_at", map[string]interface{}{
	"datepart": "day",
	"interval": 7,
})
// Checks if most recent event is within 7 days
```

## Available Tests

### Core Tests (DBT built-in)
- `not_null` - Column contains no NULL values
- `unique` - Column contains only unique values
- `accepted_values` - Column values are in allowed list
- `relationships` - Foreign key integrity check

### Extended Tests (dbt-utils style)
- `not_empty_string` - String columns not empty after trim
- `at_least_one` - At least one non-NULL value exists
- `not_constant` - Column has more than one distinct value
- `unique_combination_of_columns` - Multi-column uniqueness
- `relationships_where` - Foreign key with WHERE conditions
- `accepted_range` - Numeric values within range
- `recency` - Most recent timestamp within interval
- `equal_rowcount` - Two tables have same row count
- `sequential_values` - Sequential values with no gaps
- `mutually_exclusive_ranges` - Date/time ranges don't overlap
