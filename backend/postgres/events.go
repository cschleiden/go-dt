package postgresbackend

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/cschleiden/go-workflows/backend/history"
	"github.com/cschleiden/go-workflows/core"
)

func insertPendingEvents(ctx context.Context, tx *sql.Tx, instance *core.WorkflowInstance, newEvents []*history.Event) error {
	return insertEvents(ctx, tx, "pending_events", instance, newEvents)
}

func insertHistoryEvents(ctx context.Context, tx *sql.Tx, instance *core.WorkflowInstance, historyEvents []*history.Event) error {
	return insertEvents(ctx, tx, "history", instance, historyEvents)
}

func insertEvents(ctx context.Context, tx *sql.Tx, tableName string, instance *core.WorkflowInstance, events []*history.Event) error {
	const batchSize = 20
	for batchStart := 0; batchStart < len(events); batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > len(events) {
			batchEnd = len(events)
		}
		batchEvents := events[batchStart:batchEnd]

		aquery := "INSERT INTO attributes (event_id, instance_id, execution_id, data) VALUES (?, ?, ?, ?)" + strings.Repeat(", (?, ?, ?, ?)", len(batchEvents)-1) + " ON CONFLICT DO NOTHING"
		aargs := make([]interface{}, 0, len(batchEvents)*4)

		query := "INSERT INTO " + tableName +
			" (event_id, sequence_id, instance_id, execution_id, event_type, timestamp, schedule_event_id, visible_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)" +
			strings.Repeat(", (?, ?, ?, ?, ?, ?, ?, ?)", len(batchEvents)-1)

		args := make([]interface{}, 0, len(batchEvents)*8)

		for _, newEvent := range batchEvents {
			a, err := history.SerializeAttributes(newEvent.Attributes)
			if err != nil {
				return err
			}

			aargs = append(aargs, newEvent.ID, instance.InstanceID, instance.ExecutionID, a)

			args = append(
				args,
				newEvent.ID, newEvent.SequenceID, instance.InstanceID, instance.ExecutionID, newEvent.Type, newEvent.Timestamp, newEvent.ScheduleEventID, newEvent.VisibleAt)
		}

		if _, err := tx.ExecContext(
			ctx,
			SQLReplacer(aquery),
			aargs...,
		); err != nil {
			return fmt.Errorf("inserting attributes: %w", err)
		}

		_, err := tx.ExecContext(
			ctx,
			SQLReplacer(query),
			args...,
		)
		if err != nil {
			return fmt.Errorf("inserting events: %w", err)
		}
	}

	return nil
}

func removeFutureEvent(ctx context.Context, tx *sql.Tx, instance *core.WorkflowInstance, scheduleEventID int64) error {
	_, err := tx.ExecContext(
		ctx,
		SQLReplacer(
			"DELETE pending_events, attributes FROM pending_events INNER JOIN attributes ON pending_events.event_id = attributes.event_id WHERE pending_events.instance_id = ? AND pending_events.execution_id = ? AND pending_events.schedule_event_id = ? AND pending_events.visible_at IS NOT NULL"),
		instance.InstanceID,
		instance.ExecutionID,
		scheduleEventID,
	)

	return err
}
