package turso

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/cschleiden/go-workflows/backend/history"
	"github.com/cschleiden/go-workflows/backend/test"
	"github.com/cschleiden/go-workflows/core"
)

var _ test.TestBackend = (*tursoBackend)(nil)

func (sb *tursoBackend) GetFutureEvents(ctx context.Context) ([]*history.Event, error) {
	tx, err := sb.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// There is no index on `visible_at`, but this is okay for test only usage.
	futureEvents, err := tx.QueryContext(
		ctx,
		"SELECT pe.id, pe.sequence_id, pe.instance_id, pe.execution_id, pe.event_type, pe.timestamp, pe.schedule_event_id, pe.visible_at, a.data FROM `pending_events` pe JOIN `attributes` a ON a.id = pe.id AND a.instance_id = pe.instance_id AND a.execution_id = pe.execution_id WHERE pe.visible_at IS NOT NULL",
	)
	if err != nil {
		return nil, fmt.Errorf("getting history: %w", err)
	}

	defer futureEvents.Close()

	f := make([]*history.Event, 0)

	for futureEvents.Next() {
		var instanceID, executionID string
		var attributes []byte

		fe := &history.Event{}

		if err := futureEvents.Scan(
			&fe.ID,
			&fe.SequenceID,
			&instanceID,
			&executionID,
			&fe.Type,
			&fe.Timestamp,
			&fe.ScheduleEventID,
			&fe.VisibleAt,
			&attributes,
		); err != nil {
			return nil, fmt.Errorf("scanning event: %w", err)
		}

		a, err := history.DeserializeAttributes(fe.Type, attributes)
		if err != nil {
			return nil, fmt.Errorf("deserializing attributes: %w", err)
		}

		fe.Attributes = a

		f = append(f, fe)
	}

	return f, nil
}

func getPendingEvents(ctx context.Context, tx *sql.Tx, instance *core.WorkflowInstance) ([]*history.Event, error) {
	now := time.Now()
	events, err := tx.QueryContext(
		ctx,
		"SELECT pe.*, a.data FROM `pending_events` pe INNER JOIN `attributes` a ON a.id = pe.id AND a.instance_id = pe.instance_id AND a.execution_id = pe.execution_id WHERE pe.instance_id = ? AND pe.execution_id = ? AND (pe.`visible_at` IS NULL OR pe.`visible_at` <= ?)",
		instance.InstanceID,
		instance.ExecutionID,
		now,
	)
	if err != nil {
		return nil, fmt.Errorf("getting new events: %w", err)
	}

	defer events.Close()

	pendingEvents := make([]*history.Event, 0)

	for events.Next() {
		pendingEvent, err := scanEvent(events)
		if err != nil {
			return nil, fmt.Errorf("reading event: %w", err)
		}

		pendingEvents = append(pendingEvents, pendingEvent)
	}

	return pendingEvents, nil
}

func getHistory(ctx context.Context, tx *sql.Tx, instance *core.WorkflowInstance, lastSequenceID *int64) ([]*history.Event, error) {
	var historyEvents *sql.Rows
	var err error
	if lastSequenceID != nil {
		historyEvents, err = tx.QueryContext(
			ctx, "SELECT h.*, a.data FROM `history` h INNER JOIN `attributes` a ON a.id = h.id AND a.instance_id = h.instance_id AND a.execution_id = h.execution_id WHERE h.instance_id = ? AND h.execution_id = ? AND h.sequence_id > ?", instance.InstanceID, instance.ExecutionID, *lastSequenceID)
	} else {
		historyEvents, err = tx.QueryContext(
			ctx, "SELECT h.*, a.data FROM `history` h INNER JOIN `attributes` a ON a.id = h.id AND a.instance_id = h.instance_id AND a.execution_id = h.execution_id WHERE h.instance_id = ? AND h.execution_id = ?", instance.InstanceID, instance.ExecutionID)
	}
	if err != nil {
		return nil, fmt.Errorf("getting history: %w", err)
	}

	defer historyEvents.Close()

	events := make([]*history.Event, 0)

	for historyEvents.Next() {
		historyEvent, err := scanEvent(historyEvents)
		if err != nil {
			return nil, fmt.Errorf("reading event: %w", err)
		}

		events = append(events, historyEvent)
	}

	return events, nil
}

type Scanner interface {
	Scan(dest ...interface{}) error
}

func scanEvent(row Scanner) (*history.Event, error) {
	var instanceID, executionID string
	var attributes []byte

	historyEvent := &history.Event{}

	if err := row.Scan(
		&historyEvent.ID,
		&historyEvent.SequenceID,
		&instanceID,
		&executionID,
		&historyEvent.Type,
		&historyEvent.Timestamp,
		&historyEvent.ScheduleEventID,
		&historyEvent.VisibleAt,
		&attributes,
	); err != nil {
		return historyEvent, fmt.Errorf("scanning event: %w", err)
	}

	a, err := history.DeserializeAttributes(historyEvent.Type, attributes)
	if err != nil {
		return historyEvent, fmt.Errorf("deserializing attributes: %w", err)
	}

	historyEvent.Attributes = a

	return historyEvent, nil
}

func insertPendingEvents(ctx context.Context, tx *sql.Tx, instance *core.WorkflowInstance, newEvents []*history.Event) error {
	return insertEvents(ctx, tx, "pending_events", instance, newEvents)
}

func insertEvents(ctx context.Context, tx *sql.Tx, tableName string, instance *core.WorkflowInstance, events []*history.Event) error {
	const batchSize = 20
	for batchStart := 0; batchStart < len(events); batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > len(events) {
			batchEnd = len(events)
		}
		batchEvents := events[batchStart:batchEnd]

		// INSERT OR IGNORE since the attributes might already exist due to an event being moved from pending to history.
		aquery := "INSERT OR IGNORE INTO `attributes` (id, instance_id, execution_id, data) VALUES (?, ?, ?, ?)" + strings.Repeat(", (?, ?, ?, ?)", len(batchEvents)-1)
		aargs := make([]interface{}, 0, len(batchEvents)*4)

		query := "INSERT INTO `" + tableName + "` (id, sequence_id, instance_id, execution_id, event_type, timestamp, schedule_event_id, visible_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)" +
			strings.Repeat(", (?, ?, ?, ?, ?, ?, ?, ?)", len(batchEvents)-1)

		args := make([]interface{}, 0, len(batchEvents)*8)

		for _, newEvent := range batchEvents {
			a, err := history.SerializeAttributes(newEvent.Attributes)
			if err != nil {
				return err
			}

			aargs = append(aargs, newEvent.ID, instance.InstanceID, instance.ExecutionID, a)

			args = append(
				args, newEvent.ID, newEvent.SequenceID, instance.InstanceID, instance.ExecutionID, newEvent.Type, newEvent.Timestamp, newEvent.ScheduleEventID, newEvent.VisibleAt)
		}

		if _, err := tx.ExecContext(
			ctx,
			aquery,
			aargs...,
		); err != nil {
			return fmt.Errorf("inserting attributes: %w", err)
		}

		if _, err := tx.ExecContext(
			ctx,
			query,
			args...,
		); err != nil {
			return fmt.Errorf("inserting events: %w", err)
		}
	}
	return nil
}

func removeFutureEvent(ctx context.Context, tx *sql.Tx, instance *core.WorkflowInstance, scheduleEventID int64) error {
	row, err := tx.QueryContext(
		ctx,
		"DELETE FROM `pending_events` WHERE instance_id = ? AND execution_id = ? AND schedule_event_id = ? AND visible_at IS NOT NULL RETURNING id",
		instance.InstanceID,
		instance.ExecutionID,
		scheduleEventID,
	)
	if err != nil {
		return fmt.Errorf("removing future event: %w", err)
	}

	ids := make([]interface{}, 0)

	defer row.Close()
	for row.Next() {
		var id string
		if err := row.Scan(&id); err != nil {
			return fmt.Errorf("scanning id: %w", err)
		}

		ids = append(ids, id)
	}

	// Delete attributes
	if len(ids) > 0 {
		query := "DELETE FROM `attributes` WHERE id IN (?)" + strings.Repeat(", (?)", len(ids)-1)

		if _, err := tx.ExecContext(
			ctx,
			query,
			ids...,
		); err != nil {
			return fmt.Errorf("removing attributes: %w", err)
		}
	}

	return err
}
