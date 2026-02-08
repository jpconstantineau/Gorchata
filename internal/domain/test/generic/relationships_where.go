package generic

import (
	"fmt"
	"strings"
)

// RelationshipsWhereTest checks referential integrity with WHERE conditions on both tables
type RelationshipsWhereTest struct{}

// Name returns the test identifier
func (t *RelationshipsWhereTest) Name() string {
	return "relationships_where"
}

// Validate checks if the test arguments are valid
func (t *RelationshipsWhereTest) Validate(model, column string, args map[string]interface{}) error {
	if err := ValidateModelColumn(model, column); err != nil {
		return err
	}

	return ValidateRequired(args, []string{"to", "field"})
}

// GenerateSQL generates SQL that returns rows with foreign key violations considering WHERE conditions
func (t *RelationshipsWhereTest) GenerateSQL(model, column string, args map[string]interface{}) (string, error) {
	if err := t.Validate(model, column, args); err != nil {
		return "", err
	}

	toTable := args["to"].(string)
	toField := args["field"].(string)

	// Optional conditions
	fromCondition := ""
	if val, ok := args["from_condition"]; ok {
		if str, ok := val.(string); ok && str != "" {
			fromCondition = str
		}
	}

	toCondition := ""
	if val, ok := args["to_condition"]; ok {
		if str, ok := val.(string); ok && str != "" {
			toCondition = str
		}
	}

	whereClause := BuildWhereClause(args)

	var sqlBuilder strings.Builder
	sqlBuilder.WriteString(fmt.Sprintf("SELECT * FROM %s\n", model))
	sqlBuilder.WriteString(fmt.Sprintf("WHERE %s NOT IN (\n", column))
	sqlBuilder.WriteString(fmt.Sprintf("  SELECT %s FROM %s", toField, toTable))

	if toCondition != "" {
		sqlBuilder.WriteString(fmt.Sprintf("\n  WHERE %s", toCondition))
	}

	sqlBuilder.WriteString("\n)")
	sqlBuilder.WriteString(fmt.Sprintf("\n  AND %s IS NOT NULL", column))

	if fromCondition != "" {
		sqlBuilder.WriteString(fmt.Sprintf("\n  AND (%s)", fromCondition))
	}

	if whereClause != "" {
		sqlBuilder.WriteString(whereClause)
	}

	return sqlBuilder.String(), nil
}
