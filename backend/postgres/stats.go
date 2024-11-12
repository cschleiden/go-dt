package postgresbackend

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/cschleiden/go-workflows/backend"
	"github.com/cschleiden/go-workflows/core"
)

func (b *postgresBackend) GetStats(ctx context.Context) (*backend.Stats, error) {
	s := &backend.Stats{}

	tx, err := b.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback()

	// Get active instances
	row := tx.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM instances i WHERE i.completed_at IS NULL",
	)
	if err := row.Err(); err != nil {
		return nil, fmt.Errorf("failed to query active instances: %w", err)
	}

	var activeInstances int64
	if err := row.Scan(&activeInstances); err != nil {
		return nil, fmt.Errorf("failed to scan active instances: %w", err)
	}

	s.ActiveWorkflowInstances = activeInstances

	// Get workflow instances ready to be picked up
	now := time.Now()
	row = tx.QueryRowContext(
		ctx,
		SQLReplacer(
			`SELECT COUNT(*)
			FROM instances i
			INNER JOIN pending_events pe ON i.instance_id = pe.instance_id
			WHERE
				state = ? AND i.completed_at IS NULL
				AND (pe.visible_at IS NULL OR pe.visible_at <= ?)
				AND (i.locked_until IS NULL OR i.locked_until < ?)
			LIMIT 1
			FOR UPDATE OF i SKIP LOCKED`),
		core.WorkflowInstanceStateActive,
		now, // event.visible_at
		now, // locked_until
	)
	if err := row.Err(); err != nil {
		return nil, fmt.Errorf("failed to query active instances: %w", err)
	}

	var pendingInstances int64
	if err := row.Scan(&pendingInstances); err != nil {
		return nil, fmt.Errorf("failed to scan active instances: %w", err)
	}

	s.PendingWorkflowTasks = pendingInstances

	// Get pending activities
	row = tx.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM activities")
	if err := row.Err(); err != nil {
		return nil, fmt.Errorf("failed to query active activities: %w", err)
	}

	var pendingActivities int64
	if err := row.Scan(&pendingActivities); err != nil {
		return nil, fmt.Errorf("failed to scan active activities: %w", err)
	}

	s.PendingActivities = pendingActivities

	return s, nil
}
