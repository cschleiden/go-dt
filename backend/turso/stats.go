package turso

import (
	"context"
	"fmt"
	"time"

	"github.com/cschleiden/go-workflows/backend"
	"github.com/cschleiden/go-workflows/core"
)

func (b *tursoBackend) GetStats(ctx context.Context) (*backend.Stats, error) {
	s := &backend.Stats{}

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback()

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
		`SELECT COUNT(*) FROM instances i
			WHERE
				(locked_until IS NULL OR locked_until < ?)
				AND state = ? AND i.completed_at IS NULL
				AND EXISTS (
					SELECT 1
						FROM pending_events
						WHERE instance_id = i.id AND execution_id = i.execution_id AND (visible_at IS NULL OR visible_at <= ?)
				)
			LIMIT 1`,
		now,                              // locked_until
		core.WorkflowInstanceStateActive, // state
		now,                              // pending_event.visible_at
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
