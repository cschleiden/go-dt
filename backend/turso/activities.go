package turso

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/cschleiden/go-workflows/backend/history"
	"github.com/cschleiden/go-workflows/core"
)

func scheduleActivity(ctx context.Context, tx *sql.Tx, instance *core.WorkflowInstance, event *history.Event) error {
	// Attributes are already persisted via the history, we do not need to add them again.

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO activities
			(id, instance_id, execution_id, event_type, timestamp, schedule_event_id, visible_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		event.ID,
		instance.InstanceID,
		instance.ExecutionID,
		event.Type,
		event.Timestamp,
		event.ScheduleEventID,
		event.VisibleAt,
	); err != nil {
		return fmt.Errorf("inserting events: %w", err)
	}

	return nil
}
