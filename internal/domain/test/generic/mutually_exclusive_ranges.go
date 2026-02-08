package generic

import (
	"fmt"
	"strings"
)

// MutuallyExclusiveRangesTest checks that date/time ranges don't overlap
type MutuallyExclusiveRangesTest struct{}

// Name returns the test identifier
func (t *MutuallyExclusiveRangesTest) Name() string {
	return "mutually_exclusive_ranges"
}

// Validate checks if the test arguments are valid
func (t *MutuallyExclusiveRangesTest) Validate(model, column string, args map[string]interface{}) error {
	if model == "" {
		return fmt.Errorf("model name cannot be empty")
	}

	return ValidateRequired(args, []string{"lower_bound_column", "upper_bound_column"})
}

// GenerateSQL generates SQL that detects overlapping ranges
func (t *MutuallyExclusiveRangesTest) GenerateSQL(model, column string, args map[string]interface{}) (string, error) {
	if err := t.Validate(model, column, args); err != nil {
		return "", err
	}

	lowerBound := args["lower_bound_column"].(string)
	upperBound := args["upper_bound_column"].(string)

	// Optional partition_by for checking overlaps within groups
	partitionBy := ""
	if val, ok := args["partition_by"]; ok {
		if str, ok := val.(string); ok && str != "" {
			partitionBy = str
		}
	}

	whereClause := BuildWhereClause(args)

	var sqlBuilder strings.Builder
	sqlBuilder.WriteString("SELECT\n")
	sqlBuilder.WriteString("  a.*\n")
	sqlBuilder.WriteString(fmt.Sprintf("FROM %s a\n", model))
	sqlBuilder.WriteString(fmt.Sprintf("INNER JOIN %s b\n", model))
	sqlBuilder.WriteString("  ON a.rowid != b.rowid\n")

	if partitionBy != "" {
		sqlBuilder.WriteString(fmt.Sprintf("  AND a.%s = b.%s\n", partitionBy, partitionBy))
	}

	// Check for overlap: ranges overlap if one starts before the other ends
	sqlBuilder.WriteString(fmt.Sprintf("  AND a.%s < b.%s\n", lowerBound, upperBound))
	sqlBuilder.WriteString(fmt.Sprintf("  AND a.%s > b.%s\n", upperBound, lowerBound))

	if whereClause != "" {
		cleanWhere := strings.TrimPrefix(whereClause, " AND ")
		sqlBuilder.WriteString(fmt.Sprintf("WHERE %s", cleanWhere))
	}

	return sqlBuilder.String(), nil
}
