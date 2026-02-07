package sqlite

import (
	"context"
	"database/sql"
	"fmt"
)

// sqliteTransaction implements the Transaction interface
type sqliteTransaction struct {
	tx *sql.Tx
}

// Commit commits the transaction
func (t *sqliteTransaction) Commit() error {
	if err := t.tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// Rollback rolls back the transaction
func (t *sqliteTransaction) Rollback() error {
	if err := t.tx.Rollback(); err != nil {
		return fmt.Errorf("failed to rollback transaction: %w", err)
	}
	return nil
}

// Exec executes a statement within the transaction
func (t *sqliteTransaction) Exec(ctx context.Context, sql string, args ...interface{}) error {
	_, err := t.tx.ExecContext(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("failed to execute statement in transaction: %w", err)
	}
	return nil
}
