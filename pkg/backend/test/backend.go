package test

import (
	"context"
	"testing"
	"time"

	"github.com/cschleiden/go-workflows/pkg/backend"
	"github.com/cschleiden/go-workflows/pkg/core"
	"github.com/cschleiden/go-workflows/pkg/core/task"
	"github.com/cschleiden/go-workflows/pkg/history"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type Tester struct {
	New func() backend.Backend

	Teardown func()
}

func TestBackend(t *testing.T, tester Tester) {
	s := new(BackendTestSuite)
	s.Tester = tester
	s.Assertions = require.New(t)
	suite.Run(t, s)
}

type BackendTestSuite struct {
	suite.Suite
	*require.Assertions

	Tester Tester
	b      backend.Backend
}

func (s *BackendTestSuite) SetupTest() {
	s.b = s.Tester.New()
}

func (s *BackendTestSuite) TearDownTest() {
	if s.Tester.Teardown != nil {
		s.Tester.Teardown()
	}
}

func (s *BackendTestSuite) TestTester() {
	s.NotNil(s.b)
}

func (s *BackendTestSuite) Test_GetWorkflowTask_ReturnsNilWhenTimeout() {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Millisecond)
	defer cancel()

	task, _ := s.b.GetWorkflowTask(ctx)
	s.Nil(task)
}

func (s *BackendTestSuite) Test_GetActivityTask_ReturnNilWhenTimeout() {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Millisecond)
	defer cancel()

	task, _ := s.b.GetActivityTask(ctx)
	s.Nil(task)
}

func (s *BackendTestSuite) Test_CreateWorkflowInstance_DoesNotError() {
	ctx := context.Background()

	err := s.b.CreateWorkflowInstance(ctx, core.WorkflowEvent{
		WorkflowInstance: core.NewWorkflowInstance(uuid.NewString(), uuid.NewString()),
		HistoryEvent:     history.NewHistoryEvent(history.EventType_WorkflowExecutionStarted, -1, &history.ExecutionStartedAttributes{}),
	})
	s.NoError(err)
}

func (s *BackendTestSuite) Test_GetWorkflowTask_ReturnsTask() {
	ctx := context.Background()

	wfi := core.NewWorkflowInstance(uuid.NewString(), uuid.NewString())
	err := s.b.CreateWorkflowInstance(ctx, core.WorkflowEvent{
		WorkflowInstance: wfi,
		HistoryEvent:     history.NewHistoryEvent(history.EventType_WorkflowExecutionStarted, -1, &history.ExecutionStartedAttributes{}),
	})
	s.NoError(err)

	t, err := s.b.GetWorkflowTask(ctx)

	s.NoError(err)
	s.NotNil(t)
	s.Equal(wfi.GetInstanceID(), t.WorkflowInstance.GetInstanceID())
}

func (s *BackendTestSuite) Test_GetWorkflowTask_LocksTask() {
	ctx := context.Background()

	wfi := core.NewWorkflowInstance(uuid.NewString(), uuid.NewString())
	err := s.b.CreateWorkflowInstance(ctx, core.WorkflowEvent{
		WorkflowInstance: wfi,
		HistoryEvent:     history.NewHistoryEvent(history.EventType_WorkflowExecutionStarted, -1, &history.ExecutionStartedAttributes{}),
	})
	s.Nil(err)

	// Get and lock only task
	t, err := s.b.GetWorkflowTask(ctx)
	s.NoError(err)
	s.NotNil(t)

	// First task is locked, second call should return nil
	ctx, cancel := context.WithTimeout(ctx, time.Millisecond)
	defer cancel()

	t, err = s.b.GetWorkflowTask(ctx)

	s.NoError(err)
	s.Nil(t)
}

func (s *BackendTestSuite) Test_CompleteWorkflowTask_ReturnsErrorIfNotLocked() {
	ctx := context.Background()

	wfi := core.NewWorkflowInstance(uuid.NewString(), uuid.NewString())
	err := s.b.CreateWorkflowInstance(ctx, core.WorkflowEvent{
		WorkflowInstance: wfi,
		HistoryEvent:     history.NewHistoryEvent(history.EventType_WorkflowExecutionStarted, -1, &history.ExecutionStartedAttributes{}),
	})
	s.NoError(err)

	err = s.b.CompleteWorkflowTask(ctx, wfi, []history.Event{}, []core.WorkflowEvent{})

	s.Error(err)
}

func (s *BackendTestSuite) Test_CompleteWorkflowTask_AddsNewEventsToHistory() {
	ctx := context.Background()

	startedEvent := history.NewHistoryEvent(history.EventType_WorkflowExecutionStarted, -1, &history.ExecutionStartedAttributes{})
	activityScheduledEvent := history.NewHistoryEvent(history.EventType_ActivityScheduled, 0, &history.ActivityScheduledAttributes{})
	activityCompletedEvent := history.NewHistoryEvent(history.EventType_ActivityCompleted, 0, &history.ActivityCompletedAttributes{})

	wfi := core.NewWorkflowInstance(uuid.NewString(), uuid.NewString())
	err := s.b.CreateWorkflowInstance(ctx, core.WorkflowEvent{
		WorkflowInstance: wfi,
		HistoryEvent:     startedEvent,
	})
	s.NoError(err)

	_, err = s.b.GetWorkflowTask(ctx)
	s.NoError(err)

	taskStartedEvent := history.NewHistoryEvent(history.EventType_WorkflowTaskStarted, -1, &history.WorkflowTaskStartedAttributes{})
	taskFinishedEvent := history.NewHistoryEvent(history.EventType_WorkflowTaskFinished, -1, &history.WorkflowTaskFinishedAttributes{})
	events := []history.Event{
		taskStartedEvent,
		startedEvent,
		activityScheduledEvent,
		taskFinishedEvent,
	}

	workflowEvents := []core.WorkflowEvent{
		{
			WorkflowInstance: wfi,
			HistoryEvent:     activityCompletedEvent,
		},
	}

	err = s.b.CompleteWorkflowTask(ctx, wfi, events, workflowEvents)
	s.NoError(err)

	time.Sleep(time.Second)

	t, err := s.b.GetWorkflowTask(ctx)
	s.NotEqual(task.Continuation, t.Kind, "Expect full task")
	s.NoError(err)
	s.NotNil(t)
	s.Equal(len(events), len(t.History))
	// Only compare event types
	for i, expected := range events {
		s.Equal(expected.Type, t.History[i].Type)
	}
	s.Len(t.NewEvents, 1)
	s.Equal(activityCompletedEvent.Type, t.NewEvents[0].Type, "Expected new events to be returned")
}
