package generic

import (
	"fmt"
	"strings"
)

// SequentialValuesTest checks that a column contains sequential values with no gaps
type SequentialValuesTest struct{}

// Name returns the test identifier
func (t *SequentialValuesTest) Name() string {
	return "sequential_values"
}

// Validate checks if the test arguments are valid
func (t *SequentialValuesTest) Validate(model, column string, args map[string]interface{}) error {
	return ValidateModelColumn(model, column)
}

// GenerateSQL generates SQL that detects gaps in sequential values
func (t *SequentialValuesTest) GenerateSQL(model, column string, args map[string]interface{}) (string, error) {
	if err := t.Validate(model, column, args); err != nil {
		return "", err
	}

	// Default interval is 1
	interval := 1
	if args != nil {
		if val, ok := args["interval"]; ok {
			switch v := val.(type) {
			case int:
				interval = v
			case float64:
				interval = int(v)
			}
		}
	}

	whereClause := BuildWhereClause(args)

	var sqlBuilder strings.Builder
	sqlBuilder.WriteString("WITH ordered_values AS (\n")
	sqlBuilder.WriteString(fmt.Sprintf("  SELECT %s, ROW_NUMBER() OVER (ORDER BY %s) as rn\n", column, column))
	sqlBuilder.WriteString(fmt.Sprintf("  FROM %s\n", model))

	if whereClause != "" {
		cleanWhere := strings.TrimPrefix(whereClause, " AND ")
		sqlBuilder.WriteString(fmt.Sprintf("  WHERE %s\n", cleanWhere))
	}

	sqlBuilder.WriteString(")\n")
	sqlBuilder.WriteString("SELECT\n")
	sqlBuilder.WriteString(fmt.Sprintf("  %s as current_value,\n", column))
	sqlBuilder.WriteString(fmt.Sprintf("  LAG(%s) OVER (ORDER BY %s) as previous_value,\n", column, column))
	sqlBuilder.WriteString(fmt.Sprintf("  (%s - LAG(%s) OVER (ORDER BY %s)) as gap\n", column, column, column))
	sqlBuilder.WriteString("FROM ordered_values\n")
	sqlBuilder.WriteString(fmt.Sprintf("WHERE (%s - LAG(%s) OVER (ORDER BY %s)) != %d\n", column, column, column, interval))
	sqlBuilder.WriteString(fmt.Sprintf("  AND LAG(%s) OVER (ORDER BY %s) IS NOT NULL", column, column))

	return sqlBuilder.String(), nil
}
