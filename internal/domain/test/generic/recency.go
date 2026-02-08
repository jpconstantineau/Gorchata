package generic

import (
	"fmt"
	"strings"
)

// RecencyTest checks that the most recent timestamp is within a specified interval
type RecencyTest struct{}

// Name returns the test identifier
func (t *RecencyTest) Name() string {
	return "recency"
}

// Validate checks if the test arguments are valid
func (t *RecencyTest) Validate(model, column string, args map[string]interface{}) error {
	if err := ValidateModelColumn(model, column); err != nil {
		return err
	}

	if err := ValidateRequired(args, []string{"datepart", "interval"}); err != nil {
		return err
	}

	// Validate datepart is one of the allowed values
	datepart, ok := args["datepart"].(string)
	if !ok {
		return fmt.Errorf("datepart must be a string")
	}

	allowedDateparts := map[string]bool{
		"day":    true,
		"hour":   true,
		"minute": true,
		"second": true,
	}

	if !allowedDateparts[datepart] {
		return fmt.Errorf("datepart must be one of: day, hour, minute, second")
	}

	return nil
}

// GenerateSQL generates SQL that returns a row if the most recent timestamp exceeds the interval
func (t *RecencyTest) GenerateSQL(model, column string, args map[string]interface{}) (string, error) {
	if err := t.Validate(model, column, args); err != nil {
		return "", err
	}

	datepart := args["datepart"].(string)
	interval := args["interval"]
	whereClause := BuildWhereClause(args)

	// Calculate multiplier for JULIANDAY (which returns days)
	multiplier := ""
	switch datepart {
	case "day":
		multiplier = ""
	case "hour":
		multiplier = " * 24"
	case "minute":
		multiplier = " * 24 * 60"
	case "second":
		multiplier = " * 24 * 60 * 60"
	}

	var sqlBuilder strings.Builder
	sqlBuilder.WriteString("SELECT\n")
	sqlBuilder.WriteString(fmt.Sprintf("  MAX(%s) as most_recent,\n", column))
	sqlBuilder.WriteString(fmt.Sprintf("  (JULIANDAY('now') - JULIANDAY(MAX(%s)))%s as %s_old\n", column, multiplier, datepart))
	sqlBuilder.WriteString(fmt.Sprintf("FROM %s\n", model))

	if whereClause != "" {
		// Remove leading " AND " from whereClause and use WHERE
		cleanWhere := strings.TrimPrefix(whereClause, " AND ")
		sqlBuilder.WriteString(fmt.Sprintf("WHERE %s\n", cleanWhere))
	}

	sqlBuilder.WriteString(fmt.Sprintf("HAVING %s_old > %v", datepart, interval))

	return sqlBuilder.String(), nil
}
