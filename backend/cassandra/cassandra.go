package cassandra

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/cschleiden/go-workflows/backend"
	"github.com/cschleiden/go-workflows/backend/history"
	"github.com/cschleiden/go-workflows/backend/metadata"
	"github.com/cschleiden/go-workflows/backend/metrics"
	"github.com/cschleiden/go-workflows/core"
	"github.com/cschleiden/go-workflows/internal/metrickeys"
	"github.com/cschleiden/go-workflows/internal/workflowerrors"
	"github.com/cschleiden/go-workflows/workflow"
	"go.opentelemetry.io/otel/trace"
)

type cassandraBackend struct {
	session    *gocql.Session
	workerName string
	options    *options
}

type options struct {
	*backend.Options
}

type option func(*options)

func NewCassandraBackend(hosts []string, keyspace string, opts ...option) *cassandraBackend {
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.Quorum

	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}

	options := &options{
		Options: backend.ApplyOptions(),
	}

	for _, opt := range opts {
		opt(options)
	}

	return &cassandraBackend{
		session:    session,
		workerName: fmt.Sprintf("worker-%v", gocql.TimeUUID()),
		options:    options,
	}
}

func (cb *cassandraBackend) FeatureSupported(feature backend.Feature) bool {
	return true
}

func (cb *cassandraBackend) Close() error {
	cb.session.Close()
	return nil
}

func (cb *cassandraBackend) Tracer() trace.Tracer {
	return cb.options.TracerProvider.Tracer(backend.TracerName)
}

func (cb *cassandraBackend) Metrics() metrics.Client {
	return cb.options.Metrics.WithTags(metrics.Tags{metrickeys.Backend: "cassandra"})
}

func (cb *cassandraBackend) Options() *backend.Options {
	return cb.options.Options
}

func (cb *cassandraBackend) CreateWorkflowInstance(ctx context.Context, instance *workflow.Instance, event *history.Event) error {
	a := event.Attributes.(*history.ExecutionStartedAttributes)

	metadataJson, err := json.Marshal(a.Metadata)
	if err != nil {
		return fmt.Errorf("marshaling metadata: %w", err)
	}

	if err := cb.session.Query(`
		INSERT INTO instances (queue, instance_id, execution_id, parent_instance_id, parent_execution_id, parent_schedule_event_id, metadata, state)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		a.Queue, instance.InstanceID, instance.ExecutionID, instance.Parent.InstanceID, instance.Parent.ExecutionID, instance.ParentEventID, string(metadataJson), core.WorkflowInstanceStateActive,
	).Exec(); err != nil {
		return fmt.Errorf("inserting workflow instance: %w", err)
	}

	if err := cb.insertPendingEvents(ctx, instance, []*history.Event{event}); err != nil {
		return fmt.Errorf("inserting new event: %w", err)
	}

	return nil
}

func (cb *cassandraBackend) insertPendingEvents(ctx context.Context, instance *workflow.Instance, events []*history.Event) error {
	for _, event := range events {
		attributes, err := history.SerializeAttributes(event.Attributes)
		if err != nil {
			return fmt.Errorf("serializing attributes: %w", err)
		}

		if err := cb.session.Query(`
			INSERT INTO pending_events (id, sequence_id, instance_id, execution_id, event_type, timestamp, schedule_event_id, visible_at, attributes)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			event.ID, event.SequenceID, instance.InstanceID, instance.ExecutionID, event.Type, event.Timestamp, event.ScheduleEventID, event.VisibleAt, attributes,
		).Exec(); err != nil {
			return fmt.Errorf("inserting pending event: %w", err)
		}
	}

	return nil
}

func (cb *cassandraBackend) GetWorkflowInstanceHistory(ctx context.Context, instance *workflow.Instance, lastSequenceID *int64) ([]*history.Event, error) {
	var query string
	var args []interface{}

	if lastSequenceID != nil {
		query = "SELECT id, sequence_id, event_type, timestamp, schedule_event_id, visible_at, attributes FROM history WHERE instance_id = ? AND execution_id = ? AND sequence_id > ?"
		args = append(args, instance.InstanceID, instance.ExecutionID, *lastSequenceID)
	} else {
		query = "SELECT id, sequence_id, event_type, timestamp, schedule_event_id, visible_at, attributes FROM history WHERE instance_id = ? AND execution_id = ?"
		args = append(args, instance.InstanceID, instance.ExecutionID)
	}

	iter := cb.session.Query(query, args...).Iter()
	defer iter.Close()

	var events []*history.Event
	for iter.Scan(&event.ID, &event.SequenceID, &event.Type, &event.Timestamp, &event.ScheduleEventID, &event.VisibleAt, &attributes) {
		event.Attributes, err = history.DeserializeAttributes(event.Type, attributes)
		if err != nil {
			return nil, fmt.Errorf("deserializing attributes: %w", err)
		}

		events = append(events, event)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("closing iterator: %w", err)
	}

	return events, nil
}

func (cb *cassandraBackend) GetWorkflowInstanceState(ctx context.Context, instance *workflow.Instance) (core.WorkflowInstanceState, error) {
	var state core.WorkflowInstanceState
	if err := cb.session.Query(`
		SELECT state FROM instances WHERE instance_id = ? AND execution_id = ?`,
		instance.InstanceID, instance.ExecutionID,
	).Scan(&state); err != nil {
		if err == gocql.ErrNotFound {
			return core.WorkflowInstanceStateActive, backend.ErrInstanceNotFound
		}
		return core.WorkflowInstanceStateActive, err
	}

	return state, nil
}

func (cb *cassandraBackend) CancelWorkflowInstance(ctx context.Context, instance *workflow.Instance, event *history.Event) error {
	if err := cb.insertPendingEvents(ctx, instance, []*history.Event{event}); err != nil {
		return fmt.Errorf("inserting cancellation event: %w", err)
	}

	return nil
}

func (cb *cassandraBackend) RemoveWorkflowInstance(ctx context.Context, instance *core.WorkflowInstance) error {
	if err := cb.session.Query(`
		DELETE FROM instances WHERE instance_id = ? AND execution_id = ?`,
		instance.InstanceID, instance.ExecutionID,
	).Exec(); err != nil {
		return fmt.Errorf("deleting workflow instance: %w", err)
	}

	if err := cb.session.Query(`
		DELETE FROM history WHERE instance_id = ? AND execution_id = ?`,
		instance.InstanceID, instance.ExecutionID,
	).Exec(); err != nil {
		return fmt.Errorf("deleting workflow history: %w", err)
	}

	if err := cb.session.Query(`
		DELETE FROM attributes WHERE instance_id = ? AND execution_id = ?`,
		instance.InstanceID, instance.ExecutionID,
	).Exec(); err != nil {
		return fmt.Errorf("deleting workflow attributes: %w", err)
	}

	return nil
}

func (cb *cassandraBackend) RemoveWorkflowInstances(ctx context.Context, options ...backend.RemovalOption) error {
	ro := backend.DefaultRemovalOptions
	for _, opt := range options {
		opt(&ro)
	}

	iter := cb.session.Query(`
		SELECT instance_id, execution_id FROM instances WHERE completed_at < ?`,
		ro.FinishedBefore,
	).Iter()
	defer iter.Close()

	var instanceIDs []string
	var executionIDs []string
	for iter.Scan(&instanceID, &executionID) {
		instanceIDs = append(instanceIDs, instanceID)
		executionIDs = append(executionIDs, executionID)
	}

	batchSize := ro.BatchSize
	for i := 0; i < len(instanceIDs); i += batchSize {
		instanceIDs := instanceIDs[i:min(i+batchSize, len(instanceIDs))]
		executionIDs := executionIDs[i:min(i+batchSize, len(executionIDs))]

		batch := cb.session.NewBatch(gocql.LoggedBatch)
		for j := range instanceIDs {
			batch.Query(`
				DELETE FROM instances WHERE instance_id = ? AND execution_id = ?`,
				instanceIDs[j], executionIDs[j],
			)
			batch.Query(`
				DELETE FROM history WHERE instance_id = ? AND execution_id = ?`,
				instanceIDs[j], executionIDs[j],
			)
			batch.Query(`
				DELETE FROM attributes WHERE instance_id = ? AND execution_id = ?`,
				instanceIDs[j], executionIDs[j],
			)
		}

		if err := cb.session.ExecuteBatch(batch); err != nil {
			return fmt.Errorf("executing batch: %w", err)
		}
	}

	return nil
}

func (cb *cassandraBackend) SignalWorkflow(ctx context.Context, instanceID string, event *history.Event) error {
	var executionID string
	if err := cb.session.Query(`
		SELECT execution_id FROM instances WHERE instance_id = ? AND state = ? LIMIT 1`,
		instanceID, core.WorkflowInstanceStateActive,
	).Scan(&executionID); err != nil {
		if err == gocql.ErrNotFound {
			return backend.ErrInstanceNotFound
		}
		return err
	}

	instance := core.NewWorkflowInstance(instanceID, executionID)
	if err := cb.insertPendingEvents(ctx, instance, []*history.Event{event}); err != nil {
		return fmt.Errorf("inserting signal event: %w", err)
	}

	return nil
}

func (cb *cassandraBackend) PrepareWorkflowQueues(ctx context.Context, queues []workflow.Queue) error {
	return nil
}

func (cb *cassandraBackend) PrepareActivityQueues(ctx context.Context, queues []workflow.Queue) error {
	return nil
}

func (cb *cassandraBackend) GetWorkflowTask(ctx context.Context, queues []workflow.Queue) (*backend.WorkflowTask, error) {
	now := time.Now()

	var instanceID, executionID, queue string
	var parentInstanceID, parentExecutionID *string
	var parentEventID *int64
	var metadataJson string
	var stickyUntil *time.Time

	query := cb.session.Query(`
		SELECT instance_id, execution_id, queue, parent_instance_id, parent_execution_id, parent_schedule_event_id, metadata, sticky_until
		FROM instances
		WHERE state = ? AND completed_at IS NULL AND (locked_until IS NULL OR locked_until < ?) AND (sticky_until IS NULL OR sticky_until < ? OR worker = ?)
		AND queue IN ? AND EXISTS (
			SELECT 1 FROM pending_events WHERE instance_id = instances.instance_id AND execution_id = instances.execution_id AND (visible_at IS NULL OR visible_at <= ?)
		)
		LIMIT 1`,
		core.WorkflowInstanceStateActive, now, now, cb.workerName, queues, now,
	)

	if err := query.Scan(&instanceID, &executionID, &queue, &parentInstanceID, &parentExecutionID, &parentEventID, &metadataJson, &stickyUntil); err != nil {
		if err == gocql.ErrNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("locking workflow task: %w", err)
	}

	if err := cb.session.Query(`
		UPDATE instances SET locked_until = ?, worker = ? WHERE instance_id = ? AND execution_id = ?`,
		now.Add(cb.options.WorkflowLockTimeout), cb.workerName, instanceID, executionID,
	).Exec(); err != nil {
		return nil, fmt.Errorf("locking workflow instance: %w", err)
	}

	var wfi *workflow.Instance
	if parentInstanceID != nil {
		wfi = core.NewSubWorkflowInstance(instanceID, executionID, core.NewWorkflowInstance(*parentInstanceID, *parentExecutionID), *parentEventID)
	} else {
		wfi = core.NewWorkflowInstance(instanceID, executionID)
	}

	var metadata *metadata.WorkflowMetadata
	if err := json.Unmarshal([]byte(metadataJson), &metadata); err != nil {
		return nil, fmt.Errorf("parsing workflow metadata: %w", err)
	}

	t := &backend.WorkflowTask{
		ID:                    wfi.InstanceID,
		Queue:                 workflow.Queue(queue),
		WorkflowInstance:      wfi,
		WorkflowInstanceState: core.WorkflowInstanceStateActive,
		Metadata:              metadata,
		NewEvents:             []*history.Event{},
	}

	iter := cb.session.Query(`
		SELECT id, sequence_id, event_type, timestamp, schedule_event_id, visible_at, attributes
		FROM pending_events
		WHERE instance_id = ? AND execution_id = ? AND (visible_at IS NULL OR visible_at <= ?)
		ORDER BY sequence_id`,
		instanceID, executionID, now,
	).Iter()
	defer iter.Close()

	for iter.Scan(&event.ID, &event.SequenceID, &event.Type, &event.Timestamp, &event.ScheduleEventID, &event.VisibleAt, &attributes) {
		event.Attributes, err = history.DeserializeAttributes(event.Type, attributes)
		if err != nil {
			return nil, fmt.Errorf("deserializing attributes: %w", err)
		}

		t.NewEvents = append(t.NewEvents, event)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("closing iterator: %w", err)
	}

	if len(t.NewEvents) == 0 {
		return nil, nil
	}

	var lastSequenceID int64
	if err := cb.session.Query(`
		SELECT MAX(sequence_id) FROM history WHERE instance_id = ? AND execution_id = ?`,
		instanceID, executionID,
	).Scan(&lastSequenceID); err != nil {
		if err != gocql.ErrNotFound {
			return nil, fmt.Errorf("getting most recent sequence id: %w", err)
		}
	}

	t.LastSequenceID = lastSequenceID

	return t, nil
}

func (cb *cassandraBackend) CompleteWorkflowTask(
	ctx context.Context,
	task *backend.WorkflowTask,
	state core.WorkflowInstanceState,
	executedEvents, activityEvents, timerEvents []*history.Event,
	workflowEvents []*history.WorkflowEvent,
) error {
	now := time.Now()

	var completedAt *time.Time
	if state == core.WorkflowInstanceStateContinuedAsNew || state == core.WorkflowInstanceStateFinished {
		t := time.Now()
		completedAt = &t
	}

	if err := cb.session.Query(`
		UPDATE instances SET locked_until = NULL, sticky_until = ?, completed_at = ?, state = ? WHERE instance_id = ? AND execution_id = ? AND worker = ?`,
		now.Add(cb.options.StickyTimeout), completedAt, state, task.WorkflowInstance.InstanceID, task.WorkflowInstance.ExecutionID, cb.workerName,
	).Exec(); err != nil {
		return fmt.Errorf("unlocking workflow instance: %w", err)
	}

	if len(executedEvents) > 0 {
		args := make([]interface{}, 0, len(executedEvents)+2)
		args = append(args, task.WorkflowInstance.InstanceID, task.WorkflowInstance.ExecutionID)
		for _, e := range executedEvents {
			args = append(args, e.ID)
		}

		if err := cb.session.Query(`
			DELETE FROM pending_events WHERE instance_id = ? AND execution_id = ? AND id IN ?`,
			args...,
		).Exec(); err != nil {
			return fmt.Errorf("deleting handled new events: %w", err)
		}
	}

	if err := cb.insertHistoryEvents(ctx, task.WorkflowInstance, executedEvents); err != nil {
		return fmt.Errorf("inserting history events: %w", err)
	}

	for _, e := range activityEvents {
		a := e.Attributes.(*history.ActivityScheduledAttributes)
		queue := a.Queue
		if queue == "" {
			queue = task.Queue
		}

		if err := cb.scheduleActivity(ctx, queue, task.WorkflowInstance, e); err != nil {
			return fmt.Errorf("scheduling activity: %w", err)
		}
	}

	if err := cb.insertPendingEvents(ctx, task.WorkflowInstance, timerEvents); err != nil {
		return fmt.Errorf("scheduling timers: %w", err)
	}

	for _, event := range executedEvents {
		switch event.Type {
		case history.EventType_TimerCanceled:
			if err := cb.removeFutureEvent(ctx, task.WorkflowInstance, event.ScheduleEventID); err != nil {
				return fmt.Errorf("removing future event: %w", err)
			}
		}
	}

	groupedEvents := history.EventsByWorkflowInstance(workflowEvents)
	for targetInstance, events := range groupedEvents {
		m := events[0]
		if m.HistoryEvent.Type == history.EventType_WorkflowExecutionStarted {
			a := m.HistoryEvent.Attributes.(*history.ExecutionStartedAttributes)
			queue := a.Queue
			if queue == "" {
				queue = task.Queue
			}

			if err := cb.createInstance(ctx, queue, m.WorkflowInstance, a.Metadata); err != nil {
				if err == backend.ErrInstanceAlreadyExists {
					if err := cb.insertPendingEvents(ctx, task.WorkflowInstance, []*history.Event{
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

		historyEvents := []*history.Event{}
		for _, m := range events {
			historyEvents = append(historyEvents, m.HistoryEvent)
		}
		if err := cb.insertPendingEvents(ctx, &targetInstance, historyEvents); err != nil {
			return fmt.Errorf("inserting messages: %w", err)
		}
	}

	if cb.options.RemoveContinuedAsNewInstances && state == core.WorkflowInstanceStateContinuedAsNew {
		if err := cb.removeWorkflowInstance(ctx, task.WorkflowInstance); err != nil {
			return fmt.Errorf("removing old instance: %w", err)
		}
	}

	return nil
}

func (cb *cassandraBackend) ExtendWorkflowTask(ctx context.Context, task *backend.WorkflowTask) error {
	if err := cb.session.Query(`
		UPDATE instances SET locked_until = ? WHERE instance_id = ? AND execution_id = ? AND worker = ?`,
		time.Now().Add(cb.options.WorkflowLockTimeout), task.WorkflowInstance.InstanceID, task.WorkflowInstance.ExecutionID, cb.workerName,
	).Exec(); err != nil {
		return fmt.Errorf("extending workflow task lock: %w", err)
	}

	return nil
}

func (cb *cassandraBackend) GetActivityTask(ctx context.Context, queues []workflow.Queue) (*backend.ActivityTask, error) {
	now := time.Now()

	var instanceID, executionID, queue string
	var event history.Event

	query := cb.session.Query(`
		SELECT instance_id, execution_id, queue, id, event_type, timestamp, schedule_event_id, visible_at, attributes
		FROM activities
		WHERE (locked_until IS NULL OR locked_until < ?) AND queue IN ?
		LIMIT 1`,
		now, queues,
	)

	if err := query.Scan(&instanceID, &executionID, &queue, &event.ID, &event.Type, &event.Timestamp, &event.ScheduleEventID, &event.VisibleAt, &attributes); err != nil {
		if err == gocql.ErrNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("locking activity task: %w", err)
	}

	if err := cb.session.Query(`
		UPDATE activities SET locked_until = ?, worker = ? WHERE instance_id = ? AND execution_id = ? AND id = ?`,
		now.Add(cb.options.ActivityLockTimeout), cb.workerName, instanceID, executionID, event.ID,
	).Exec(); err != nil {
		return nil, fmt.Errorf("locking activity: %w", err)
	}

	event.Attributes, err = history.DeserializeAttributes(event.Type, attributes)
	if err != nil {
		return nil, fmt.Errorf("deserializing attributes: %w", err)
	}

	t := &backend.ActivityTask{
		ID:               event.ID,
		ActivityID:       event.ID,
		Queue:            workflow.Queue(queue),
		WorkflowInstance: core.NewWorkflowInstance(instanceID, executionID),
		Event:            event,
	}

	return t, nil
}

func (cb *cassandraBackend) CompleteActivityTask(ctx context.Context, task *backend.ActivityTask, result *history.Event) error {
	if err := cb.session.Query(`
		DELETE FROM activities WHERE instance_id = ? AND execution_id = ? AND id = ? AND worker = ?`,
		task.WorkflowInstance.InstanceID, task.WorkflowInstance.ExecutionID, task.ActivityID, cb.workerName,
	).Exec(); err != nil {
		return fmt.Errorf("deleting activity: %w", err)
	}

	if err := cb.insertPendingEvents(ctx, task.WorkflowInstance, []*history.Event{result}); err != nil {
		return fmt.Errorf("inserting new events for completed activity: %w", err)
	}

	return nil
}

func (cb *cassandraBackend) ExtendActivityTask(ctx context.Context, task *backend.ActivityTask) error {
	if err := cb.session.Query(`
		UPDATE activities SET locked_until = ? WHERE id = ? AND worker = ?`,
		time.Now().Add(cb.options.ActivityLockTimeout), task.ActivityID, cb.workerName,
	).Exec(); err != nil {
		return fmt.Errorf("extending activity lock: %w", err)
	}

	return nil
}

func (cb *cassandraBackend) insertHistoryEvents(ctx context.Context, instance *workflow.Instance, events []*history.Event) error {
	for _, event := range events {
		attributes, err := history.SerializeAttributes(event.Attributes)
		if err != nil {
			return fmt.Errorf("serializing attributes: %w", err)
		}

		if err := cb.session.Query(`
			INSERT INTO history (id, sequence_id, instance_id, execution_id, event_type, timestamp, schedule_event_id, visible_at, attributes)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			event.ID, event.SequenceID, instance.InstanceID, instance.ExecutionID, event.Type, event.Timestamp, event.ScheduleEventID, event.VisibleAt, attributes,
		).Exec(); err != nil {
			return fmt.Errorf("inserting history event: %w", err)
		}
	}

	return nil
}

func (cb *cassandraBackend) scheduleActivity(ctx context.Context, queue workflow.Queue, instance *workflow.Instance, event *history.Event) error {
	attributes, err := history.SerializeAttributes(event.Attributes)
	if err != nil {
		return fmt.Errorf("serializing attributes: %w", err)
	}

	if err := cb.session.Query(`
		INSERT INTO activities (id, queue, instance_id, execution_id, event_type, timestamp, schedule_event_id, visible_at, attributes)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		event.ID, queue, instance.InstanceID, instance.ExecutionID, event.Type, event.Timestamp, event.ScheduleEventID, event.VisibleAt, attributes,
	).Exec(); err != nil {
		return fmt.Errorf("scheduling activity: %w", err)
	}

	return nil
}

func (cb *cassandraBackend) removeFutureEvent(ctx context.Context, instance *workflow.Instance, scheduleEventID int64) error {
	if err := cb.session.Query(`
		DELETE FROM pending_events WHERE instance_id = ? AND execution_id = ? AND schedule_event_id = ? AND visible_at IS NOT NULL`,
		instance.InstanceID, instance.ExecutionID, scheduleEventID,
	).Exec(); err != nil {
		return fmt.Errorf("removing future event: %w", err)
	}

	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
