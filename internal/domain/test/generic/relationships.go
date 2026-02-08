package generic

import (
	"fmt"
)

// RelationshipsTest checks referential integrity between tables (foreign key check)
type RelationshipsTest struct{}

// Name returns the test identifier
func (t *RelationshipsTest) Name() string {
	return "relationships"
}

// Validate checks if the test arguments are valid
func (t *RelationshipsTest) Validate(model, column string, args map[string]interface{}) error {
	if err := ValidateModelColumn(model, column); err != nil {
		return err
	}

	return ValidateRequired(args, []string{"to", "field"})
}

// GenerateSQL generates SQL that returns rows with foreign key values not in the parent table
func (t *RelationshipsTest) GenerateSQL(model, column string, args map[string]interface{}) (string, error) {
	if err := t.Validate(model, column, args); err != nil {
		return "", err
	}

	toTable := args["to"].(string)
	toField := args["field"].(string)
	whereClause := BuildWhereClause(args)

	sql := fmt.Sprintf(
		"SELECT * FROM %s WHERE %s NOT IN (SELECT %s FROM %s) AND %s IS NOT NULL%s",
		model,
		column,
		toField,
		toTable,
		column,
		whereClause,
	)

	return sql, nil
}
