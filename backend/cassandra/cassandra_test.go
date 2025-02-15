package cassandra

import (
	"context"
	"testing"
	"time"

	"github.com/cschleiden/go-workflows/backend"
	"github.com/cschleiden/go-workflows/backend/history"
	"github.com/cschleiden/go-workflows/backend/test"
	"github.com/cschleiden/go-workflows/core"
	"github.com/cschleiden/go-workflows/workflow"
	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func Test_CassandraBackend(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "test_keyspace"
	cluster.Consistency = gocql.Quorum

	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	test.BackendTest(t, func(options ...backend.BackendOption) test.TestBackend {
		options = append(options, backend.WithStickyTimeout(0))
		return NewCassandraBackend([]string{"127.0.0.1"}, "test_keyspace", options...)
	}, nil)
}

func Test_EndToEndCassandraBackend(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "test_keyspace"
	cluster.Consistency = gocql.Quorum

	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	test.EndToEndBackendTest(t, func(options ...backend.BackendOption) test.TestBackend {
		options = append(options, backend.WithStickyTimeout(0))
		return NewCassandraBackend([]string{"127.0.0.1"}, "test_keyspace", options...)
	}, nil)
}

var _ test.TestBackend = (*cassandraBackend)(nil)

func (cb *cassandraBackend) GetFutureEvents(ctx context.Context) ([]*history.Event, error) {
	iter := cb.session.Query(`
		SELECT id, sequence_id, instance_id, execution_id, event_type, timestamp, schedule_event_id, visible_at, attributes
		FROM pending_events
		WHERE visible_at IS NOT NULL
	`).Iter()
	defer iter.Close()

	var events []*history.Event
	for iter.Scan(&event.ID, &event.SequenceID, &instanceID, &executionID, &event.Type, &event.Timestamp, &event.ScheduleEventID, &event.VisibleAt, &attributes) {
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
