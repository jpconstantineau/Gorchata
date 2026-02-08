package generic

import (
	"fmt"
	"strings"
)

// NotConstantTest checks that a column has more than one distinct value
type NotConstantTest struct{}

// Name returns the test identifier
func (t *NotConstantTest) Name() string {
	return "not_constant"
}

// Validate checks if the test arguments are valid
func (t *NotConstantTest) Validate(model, column string, args map[string]interface{}) error {
	return ValidateModelColumn(model, column)
}

// GenerateSQL generates SQL that returns a row if the column has <= 1 distinct values
func (t *NotConstantTest) GenerateSQL(model, column string, args map[string]interface{}) (string, error) {
	if err := t.Validate(model, column, args); err != nil {
		return "", err
	}
	
	whereClause := BuildWhereClause(args)
	
	var sqlBuilder strings.Builder
	sqlBuilder.WriteString(fmt.Sprintf("SELECT COUNT(DISTINCT %s) as distinct_count\n", column))
	sqlBuilder.WriteString(fmt.Sprintf("FROM %s\n", model))
	
	if whereClause != "" {
		// Remove leading " AND " from whereClause and use WHERE
		cleanWhere := strings.TrimPrefix(whereClause, " AND ")
		sqlBuilder.WriteString(fmt.Sprintf("WHERE %s\n", cleanWhere))
	}
	
	sqlBuilder.WriteString("HAVING COUNT(DISTINCT ") 
	sqlBuilder.WriteString(column)
	sqlBuilder.WriteString(") <= 1")
	
	return sqlBuilder.String(), nil
}
