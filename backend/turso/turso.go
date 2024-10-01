package turso

import (
	"context"
	"database/sql"
	"embed"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/cschleiden/go-workflows/backend"
	"github.com/cschleiden/go-workflows/backend/converter"
	"github.com/cschleiden/go-workflows/backend/history"
	"github.com/cschleiden/go-workflows/backend/metadata"
	"github.com/cschleiden/go-workflows/backend/metrics"
	"github.com/cschleiden/go-workflows/core"
	"github.com/cschleiden/go-workflows/internal/metrickeys"
	"github.com/cschleiden/go-workflows/internal/workflowerrors"
	"github.com/cschleiden/go-workflows/workflow"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	_ "github.com/tursodatabase/libsql-client-go/libsql"
	_ "modernc.org/sqlite"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed db/migrations/*.sql
var migrationsFS embed.FS

func NewInMemoryBackend(opts ...option) *tursoBackend {
	b := newTursoBackend("file::memory:?mode=memory&cache=shared", opts...)

	b.db.SetConnMaxIdleTime(0)
	b.db.SetMaxIdleConns(1)

	// WORKAROUND: Keep a connection open at all times to prevent hte in-memory db from being dropped
	b.db.SetMaxOpenConns(2)

	var err error
	b.memConn, err = b.db.Conn(context.Background())
	if err != nil {
		panic(err)
	}

	return b
}

func NewTursoBackend(dsn string, opts ...option) *tursoBackend {
	return newTursoBackend(dsn, opts...)
}

func newTursoBackend(dsn string, opts ...option) *tursoBackend {
	options := &options{
		Options:         backend.ApplyOptions(),
		ApplyMigrations: true,
	}

	for _, opt := range opts {
		opt(options)
	}

	db, err := sql.Open("libsql", dsn)
	if err != nil {
		panic(err)
	}

	// SQLite does not support multiple writers on the database, see https://www.sqlite.org/faq.html#q5
	// A frequently used workaround is to have a single connection, effectively acting as a mutex
	// See https://github.com/mattn/go-sqlite3/issues/274 for more context
	db.SetMaxOpenConns(1)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = db.PingContext(ctx)
	if err != nil {
		panic(err)
	}

	b := &tursoBackend{
		db:         db,
		workerName: fmt.Sprintf("worker-%v", uuid.NewString()),
		options:    options,
	}

	// Apply migrations
	if options.ApplyMigrations {
		if err := b.Migrate(); err != nil {
			panic(err)
		}
	}

	return b
}

type tursoBackend struct {
	db         *sql.DB
	workerName string
	options    *options

	memConn *sql.Conn
}

var _ backend.Backend = (*tursoBackend)(nil)

func (sb *tursoBackend) Close() error {
	if sb.memConn != nil {
		if err := sb.memConn.Close(); err != nil {
			return err
		}
	}

	return sb.db.Close()
}

// Migrate applies any pending database migrations.
func (sb *tursoBackend) Migrate() error {
	sb.options.Logger.Info("Applying migrations...")

	dbi, err := sqlite.WithInstance(sb.db, &sqlite.Config{})
	if err != nil {
		return fmt.Errorf("creating migration instance: %w", err)
	}

	migrations, err := iofs.New(migrationsFS, "db/migrations")
	if err != nil {
		return fmt.Errorf("creating migration source: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", migrations, "sqlite", dbi)
	if err != nil {
		return fmt.Errorf("creating migration: %w", err)
	}

	if err := m.Up(); err != nil {
		if !errors.Is(err, migrate.ErrNoChange) {
			return fmt.Errorf("running migrations: %w", err)
		}

		sb.options.Logger.Info("No migrations to apply")
	}

	return nil
}

func (sb *tursoBackend) Logger() *slog.Logger {
	return sb.options.Logger
}

func (sb *tursoBackend) Metrics() metrics.Client {
	return sb.options.Metrics.WithTags(metrics.Tags{metrickeys.Backend: "sqlite"})
}

func (sb *tursoBackend) Tracer() trace.Tracer {
	return sb.options.TracerProvider.Tracer(backend.TracerName)
}

func (sb *tursoBackend) Converter() converter.Converter {
	return sb.options.Converter
}

func (sb *tursoBackend) ContextPropagators() []workflow.ContextPropagator {
	return sb.options.ContextPropagators
}

func (sb *tursoBackend) CreateWorkflowInstance(ctx context.Context, instance *workflow.Instance, event *history.Event) error {
	tx, err := sb.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback()

	// Create workflow instance
	if err := createInstance(ctx, tx, instance, event.Attributes.(*history.ExecutionStartedAttributes).Metadata); err != nil {
		return err
	}

	if err := insertPendingEvents(ctx, tx, instance, []*history.Event{event}); err != nil {
		return fmt.Errorf("inserting new event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("creating workflow instance: %w", err)
	}

	return nil
}

func createInstance(ctx context.Context, tx *sql.Tx, wfi *workflow.Instance, metadata *workflow.Metadata) error {
	// Check for existing instance
	if err := tx.QueryRowContext(ctx, "SELECT 1 FROM `instances` WHERE id = ? AND state = ? LIMIT 1", wfi.InstanceID, core.WorkflowInstanceStateActive).
		Scan(new(int)); err != sql.ErrNoRows {
		return backend.ErrInstanceAlreadyExists
	}

	var parentInstanceID, parentExecutionID *string
	var parentEventID *int64
	if wfi.SubWorkflow() {
		parentInstanceID = &wfi.Parent.InstanceID
		parentExecutionID = &wfi.Parent.ExecutionID
		parentEventID = &wfi.ParentEventID
	}

	metadataJson, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("marshaling metadata: %w", err)
	}

	_, err = tx.ExecContext(
		ctx,
		"INSERT INTO `instances` (id, execution_id, parent_instance_id, parent_execution_id, parent_schedule_event_id, metadata, state) VALUES (?, ?, ?, ?, ?, ?, ?)",
		wfi.InstanceID,
		wfi.ExecutionID,
		parentInstanceID,
		parentExecutionID,
		parentEventID,
		string(metadataJson),
		core.WorkflowInstanceStateActive,
	)
	if err != nil {
		return fmt.Errorf("inserting workflow instance: %w", err)
	}

	return nil
}

func (sb *tursoBackend) RemoveWorkflowInstance(ctx context.Context, instance *core.WorkflowInstance) error {
	tx, err := sb.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	instanceID := instance.InstanceID
	executionID := instance.ExecutionID

	// Check status of the instance
	row := tx.QueryRowContext(ctx, "SELECT state FROM `instances` WHERE id = ? AND execution_id = ? LIMIT 1", instanceID, executionID)
	var state core.WorkflowInstanceState
	if err := row.Scan(&state); err != nil {
		if err == sql.ErrNoRows {
			return backend.ErrInstanceNotFound
		}
	}

	if state == core.WorkflowInstanceStateActive {
		return backend.ErrInstanceNotFinished
	}

	// Delete from instances and history tables
	if _, err := tx.ExecContext(ctx, "DELETE FROM `instances` WHERE id = ? AND execution_id = ?", instanceID, executionID); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, "DELETE FROM `history` WHERE instance_id = ? AND execution_id = ?", instanceID, executionID); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, "DELETE FROM `attributes` WHERE instance_id = ? AND execution_id = ?", instanceID, executionID); err != nil {
		return err
	}

	return tx.Commit()
}

func (sb *tursoBackend) CancelWorkflowInstance(ctx context.Context, instance *workflow.Instance, event *history.Event) error {
	tx, err := sb.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	instanceID := instance.InstanceID
	executionID := instance.ExecutionID

	// TODO: Combine with event insertion
	res := tx.QueryRowContext(ctx, "SELECT 1 FROM `instances` WHERE id = ? AND execution_id = ? LIMIT 1", instanceID, executionID)
	if err := res.Scan(new(int)); err != nil {
		if err == sql.ErrNoRows {
			return backend.ErrInstanceNotFound
		}

		return err
	}

	if err := insertPendingEvents(ctx, tx, instance, []*history.Event{event}); err != nil {
		return fmt.Errorf("inserting cancellation event: %w", err)
	}

	return tx.Commit()
}

func (sb *tursoBackend) GetWorkflowInstanceHistory(ctx context.Context, instance *workflow.Instance, lastSequenceID *int64) ([]*history.Event, error) {
	tx, err := sb.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	h, err := getHistory(ctx, tx, instance, lastSequenceID)
	if err != nil {
		return nil, fmt.Errorf("getting workflow history: %w", err)
	}

	return h, nil
}

func (sb *tursoBackend) GetWorkflowInstanceState(ctx context.Context, instance *workflow.Instance) (core.WorkflowInstanceState, error) {
	tx, err := sb.db.BeginTx(ctx, nil)
	if err != nil {
		return core.WorkflowInstanceStateActive, err
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(
		ctx,
		"SELECT state FROM instances WHERE id = ? AND execution_id = ?",
		instance.InstanceID,
		instance.ExecutionID,
	)

	var state core.WorkflowInstanceState
	if err := row.Scan(&state); err != nil {
		if err == sql.ErrNoRows {
			return core.WorkflowInstanceStateActive, backend.ErrInstanceNotFound
		}
	}

	return state, nil
}

func (sb *tursoBackend) SignalWorkflow(ctx context.Context, instanceID string, event *history.Event) error {
	tx, err := sb.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// TODO: Combine this with the event insertion
	var executionID string
	res := tx.QueryRowContext(ctx, "SELECT execution_id FROM `instances` WHERE id = ? AND state = ? LIMIT 1", instanceID, core.WorkflowInstanceStateActive)
	if err := res.Scan(&executionID); err == sql.ErrNoRows {
		return backend.ErrInstanceNotFound
	}

	if err := insertPendingEvents(ctx, tx, core.NewWorkflowInstance(instanceID, executionID), []*history.Event{event}); err != nil {
		return fmt.Errorf("inserting signal event: %w", err)
	}

	return tx.Commit()
}

func (sb *tursoBackend) GetWorkflowTask(ctx context.Context) (*backend.WorkflowTask, error) {
	tx, err := sb.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Lock next workflow task by finding an unlocked instance with new events to process
	// (work around missing LIMIT support in sqlite driver for UPDATE statements by using sub-query)
	now := time.Now()
	row := tx.QueryRowContext(
		ctx,
		`UPDATE instances
			SET locked_until = ?, worker = ?
			WHERE rowid = (
				SELECT rowid FROM instances i
					WHERE
						(locked_until IS NULL OR locked_until < ?)
						AND (sticky_until IS NULL OR sticky_until < ? OR worker = ?)
						AND state = ? AND i.completed_at IS NULL
						AND EXISTS (
							SELECT 1
								FROM pending_events
								WHERE instance_id = i.id AND execution_id = i.execution_id AND (visible_at IS NULL OR visible_at <= ?)
						)
					LIMIT 1
			) RETURNING id, execution_id, parent_instance_id, parent_execution_id, parent_schedule_event_id, metadata, sticky_until`,
		now.Add(sb.options.WorkflowLockTimeout), // new locked_until
		sb.workerName,
		now,                              // locked_until
		now,                              // sticky_until
		sb.workerName,                    // worker
		core.WorkflowInstanceStateActive, // state
		now,                              // pending_event.visible_at
	)

	var instanceID, executionID string
	var parentInstanceID, parentExecutionID *string
	var parentEventID *int64
	var metadataJson sql.NullString
	var stickyUntil *time.Time
	if err := row.Scan(&instanceID, &executionID, &parentInstanceID, &parentExecutionID, &parentEventID, &metadataJson, &stickyUntil); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}

		return nil, fmt.Errorf("locking workflow task: %w", err)
	}

	var wfi *workflow.Instance
	if parentInstanceID != nil {
		wfi = core.NewSubWorkflowInstance(instanceID, executionID, core.NewWorkflowInstance(*parentInstanceID, *parentExecutionID), *parentEventID)
	} else {
		wfi = core.NewWorkflowInstance(instanceID, executionID)
	}

	var metadata *metadata.WorkflowMetadata
	if metadataJson.Valid {
		if err := json.Unmarshal([]byte(metadataJson.String), &metadata); err != nil {
			return nil, fmt.Errorf("parsing workflow metadata: %w", err)
		}
	}

	t := &backend.WorkflowTask{
		ID:                    wfi.InstanceID,
		WorkflowInstance:      wfi,
		WorkflowInstanceState: core.WorkflowInstanceStateActive,
		Metadata:              metadata,
		NewEvents:             []*history.Event{},
	}

	// Get new events
	pendingEvents, err := getPendingEvents(ctx, tx, wfi)
	if err != nil {
		return nil, fmt.Errorf("getting pending events: %w", err)
	}

	// Return if there aren't any new events
	if len(pendingEvents) == 0 {
		return nil, nil
	}

	t.NewEvents = pendingEvents

	// Get only most recent sequence ID
	// TODO: Denormalize to instances table
	row = tx.QueryRowContext(ctx, "SELECT sequence_id FROM `history` WHERE instance_id = ? AND execution_id = ? ORDER BY rowid DESC LIMIT 1", instanceID, executionID)
	if err := row.Scan(&t.LastSequenceID); err != nil {
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("getting most recent sequence id: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return t, nil
}

func (sb *tursoBackend) CompleteWorkflowTask(
	ctx context.Context,
	task *backend.WorkflowTask,
	instance *workflow.Instance,
	state core.WorkflowInstanceState,
	executedEvents, activityEvents, timerEvents []*history.Event,
	workflowEvents []history.WorkflowEvent,
) error {
	tx, err := sb.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var completedAt *time.Time
	if state == core.WorkflowInstanceStateContinuedAsNew || state == core.WorkflowInstanceStateFinished {
		t := time.Now()
		completedAt = &t
	}

	// Unlock instance, but keep it sticky to the current worker
	if res, err := tx.ExecContext(
		ctx,
		`UPDATE instances SET locked_until = NULL, sticky_until = ?, completed_at = ?, state = ? WHERE id = ? AND execution_id = ? AND worker = ?`,
		time.Now().Add(sb.options.StickyTimeout),
		completedAt,
		state,
		instance.InstanceID,
		instance.ExecutionID,
		sb.workerName,
	); err != nil {
		return fmt.Errorf("unlocking workflow instance: %w", err)
	} else if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("checking for unlocked workflow instances: %w", err)
	} else if n != 1 {
		return errors.New("could not find workflow instance to unlock")
	}

	// Remove handled events from task
	if len(executedEvents) > 0 {
		args := make([]interface{}, 0, len(executedEvents)+1)
		args = append(args, instance.InstanceID, instance.ExecutionID)
		for _, e := range executedEvents {
			args = append(args, e.ID)
		}

		// Remove from pending
		if _, err := tx.ExecContext(
			ctx,
			fmt.Sprintf(`DELETE FROM pending_events WHERE instance_id = ? AND execution_id = ? AND id IN (?%v)`, strings.Repeat(",?", len(executedEvents)-1)),
			args...,
		); err != nil {
			return fmt.Errorf("deleting handled new events: %w", err)
		}
	}

	if err := insertEvents(ctx, tx, "history", instance, executedEvents); err != nil {
		return fmt.Errorf("inserting history events: %w", err)
	}

	// Schedule activities
	for _, event := range activityEvents {
		if err := scheduleActivity(ctx, tx, instance, event); err != nil {
			return fmt.Errorf("scheduling activity: %w", err)
		}
	}

	// Timer events
	if err := insertPendingEvents(ctx, tx, instance, timerEvents); err != nil {
		return fmt.Errorf("scheduling timers: %w", err)
	}

	for _, event := range executedEvents {
		switch event.Type {
		case history.EventType_TimerCanceled:
			if err := removeFutureEvent(ctx, tx, instance, event.ScheduleEventID); err != nil {
				return fmt.Errorf("removing future event: %w", err)
			}
		}
	}

	// Insert new workflow events
	groupedEvents := history.EventsByWorkflowInstance(workflowEvents)

	for targetInstance, events := range groupedEvents {
		// Are we creating a new sub-workflow instance?
		m := events[0]
		if m.HistoryEvent.Type == history.EventType_WorkflowExecutionStarted {
			a := m.HistoryEvent.Attributes.(*history.ExecutionStartedAttributes)
			// Create new instance
			if err := createInstance(ctx, tx, m.WorkflowInstance, a.Metadata); err != nil {
				if err == backend.ErrInstanceAlreadyExists {
					if err := insertPendingEvents(ctx, tx, instance, []*history.Event{
						history.NewPendingEvent(time.Now(), history.EventType_SubWorkflowFailed, &history.SubWorkflowFailedAttributes{
							Error: workflowerrors.FromError(backend.ErrInstanceAlreadyExists),
						}, history.ScheduleEventID(m.WorkflowInstance.ParentEventID)),
					}); err != nil {
						return fmt.Errorf("inserting sub-workflow failed event: %w", err)
					}

					continue
				}

				return fmt.Errorf("creating sub-workflow instance: %w", err)
			}
		}

		// Insert pending events for target instance
		historyEvents := []*history.Event{}
		for _, m := range events {
			historyEvents = append(historyEvents, m.HistoryEvent)
		}
		if err := insertPendingEvents(ctx, tx, &targetInstance, historyEvents); err != nil {
			return fmt.Errorf("inserting messages: %w", err)
		}
	}

	return tx.Commit()
}

func (sb *tursoBackend) ExtendWorkflowTask(ctx context.Context, taskID string, instance *workflow.Instance) error {
	tx, err := sb.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	until := time.Now().Add(sb.options.WorkflowLockTimeout)
	res, err := tx.ExecContext(
		ctx,
		`UPDATE instances SET locked_until = ? WHERE id = ? AND execution_id = ? AND worker = ?`,
		until,
		instance.InstanceID,
		instance.ExecutionID,
		sb.workerName,
	)
	if err != nil {
		return fmt.Errorf("extending workflow task lock: %w", err)
	}

	if rowsAffected, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("determining if workflow task was extended: %w", err)
	} else if rowsAffected == 0 {
		return errors.New("could not extend workflow task")
	}

	return tx.Commit()
}

func (sb *tursoBackend) GetActivityTask(ctx context.Context) (*backend.ActivityTask, error) {
	tx, err := sb.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Lock next activity
	// (work around missing LIMIT support in sqlite driver for UPDATE statements by using sub-query)
	now := time.Now()
	row := tx.QueryRowContext(
		ctx,
		`UPDATE activities
			SET locked_until = ?, worker = ?
			WHERE rowid = (
				SELECT rowid FROM activities WHERE locked_until IS NULL OR locked_until < ? LIMIT 1
			) RETURNING id, instance_id, execution_id, event_type, timestamp, schedule_event_id, visible_at`,
		now.Add(sb.options.ActivityLockTimeout),
		sb.workerName,
		now,
	)
	if err != nil {
		return nil, err
	}

	var instanceID, executionID string
	event := &history.Event{}

	if err := row.Scan(
		&event.ID,
		&instanceID,
		&executionID,
		&event.Type,
		&event.Timestamp,
		&event.ScheduleEventID,
		&event.VisibleAt,
	); err != nil {
		if err == sql.ErrNoRows {
			// No rows locked, just return
			return nil, nil
		}

		return nil, fmt.Errorf("scanning event: %w", err)
	}

	var attributes []byte
	if err := tx.QueryRowContext(
		ctx, "SELECT data FROM attributes WHERE instance_id = ? AND execution_id = ? AND id = ?", instanceID, executionID, event.ID,
	).Scan(&attributes); err != nil {
		return nil, fmt.Errorf("scanning attributes: %w", err)
	}

	a, err := history.DeserializeAttributes(event.Type, attributes)
	if err != nil {
		return nil, fmt.Errorf("deserializing attributes: %w", err)
	}

	event.Attributes = a

	var metadataJson sql.NullString
	if err := tx.QueryRowContext(ctx, "SELECT metadata FROM instances WHERE id = ?", instanceID).Scan(&metadataJson); err != nil {
		return nil, fmt.Errorf("scanning metadata: %w", err)
	}

	var metadata *workflow.Metadata
	if err := json.Unmarshal([]byte(metadataJson.String), &metadata); err != nil {
		return nil, fmt.Errorf("unmarshaling metadata: %w", err)
	}

	t := &backend.ActivityTask{
		ID:               event.ID,
		WorkflowInstance: core.NewWorkflowInstance(instanceID, executionID),
		Event:            event,
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return t, nil
}

func (sb *tursoBackend) CompleteActivityTask(ctx context.Context, instance *workflow.Instance, id string, event *history.Event) error {
	tx, err := sb.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Remove activity but keep the attributes, they are still needed for the history
	if res, err := tx.ExecContext(
		ctx,
		`DELETE FROM activities WHERE instance_id = ? AND execution_id = ? AND id = ? AND worker = ?`,
		instance.InstanceID,
		instance.ExecutionID,
		id,
		sb.workerName,
	); err != nil {
		return fmt.Errorf("unlocking instance: %w", err)
	} else if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("checking for deleted activities: %w", err)
	} else if n != 1 {
		return errors.New("could not find activity to delete")
	}

	// Insert new event generated during this workflow execution
	if err := insertPendingEvents(ctx, tx, instance, []*history.Event{event}); err != nil {
		return fmt.Errorf("inserting new events for completed activity: %w", err)
	}

	return tx.Commit()
}

func (sb *tursoBackend) ExtendActivityTask(ctx context.Context, activityID string) error {
	tx, err := sb.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	until := time.Now().Add(sb.options.ActivityLockTimeout)
	res, err := tx.ExecContext(
		ctx,
		`UPDATE activities SET locked_until = ? WHERE id = ? AND worker = ?`,
		until,
		activityID,
		sb.workerName,
	)
	if err != nil {
		return fmt.Errorf("extending activity lock: %w", err)
	}

	if rowsAffected, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("determining if activity was extended: %w", err)
	} else if rowsAffected == 0 {
		return errors.New("could not extend activity")
	}

	return tx.Commit()
}
