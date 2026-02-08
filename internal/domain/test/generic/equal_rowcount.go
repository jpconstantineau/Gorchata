package generic

import (
	"fmt"
	"strings"
)

// EqualRowcountTest checks that two tables have the same number of rows
type EqualRowcountTest struct{}

// Name returns the test identifier
func (t *EqualRowcountTest) Name() string {
	return "equal_rowcount"
}

// Validate checks if the test arguments are valid
func (t *EqualRowcountTest) Validate(model, column string, args map[string]interface{}) error {
	if model == "" {
		return fmt.Errorf("model name cannot be empty")
	}
	
	return ValidateRequired(args, []string{"compare_model"})
}

// GenerateSQL generates SQL that returns a row if the row counts differ
func (t *EqualRowcountTest) GenerateSQL(model, column string, args map[string]interface{}) (string, error) {
	if err := t.Validate(model, column, args); err != nil {
		return "", err
	}
	
	compareModel := args["compare_model"].(string)
	whereClause := BuildWhereClause(args)
	
	var sqlBuilder strings.Builder
	sqlBuilder.WriteString("SELECT\n")
	sqlBuilder.WriteString("  (SELECT COUNT(*) FROM ")
	sqlBuilder.WriteString(model)
	
	if whereClause != "" {
		// Remove leading " AND " from whereClause and use WHERE
		cleanWhere := strings.TrimPrefix(whereClause, " AND ")
		sqlBuilder.WriteString(fmt.Sprintf(" WHERE %s", cleanWhere))
	}
	
	sqlBuilder.WriteString(") as count1,\n")
	sqlBuilder.WriteString("  (SELECT COUNT(*) FROM ")
	sqlBuilder.WriteString(compareModel)
	
	if whereClause != "" {
		cleanWhere := strings.TrimPrefix(whereClause, " AND ")
		sqlBuilder.WriteString(fmt.Sprintf(" WHERE %s", cleanWhere))
	}
	
	sqlBuilder.WriteString(") as count2\n")
	sqlBuilder.WriteString("WHERE count1 != count2")
	
	return sqlBuilder.String(), nil
}
