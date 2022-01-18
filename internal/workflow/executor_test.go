package workflow

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/cschleiden/go-dt/internal/command"
	"github.com/cschleiden/go-dt/internal/converter"
	"github.com/cschleiden/go-dt/internal/payload"
	"github.com/cschleiden/go-dt/internal/sync"
	"github.com/cschleiden/go-dt/pkg/core"
	"github.com/cschleiden/go-dt/pkg/core/task"
	"github.com/cschleiden/go-dt/pkg/history"
	"github.com/stretchr/testify/require"
)

func activity1(ctx context.Context, r int) (int, error) {
	fmt.Println("Entering Activity1")

	return r, nil
}

func Test_ExecuteWorkflow(t *testing.T) {
	r := NewRegistry()

	var workflowHits int

	Workflow1 := func(ctx sync.Context) error {
		workflowHits++

		return nil
	}

	r.RegisterWorkflow(Workflow1)

	task := &task.Workflow{
		WorkflowInstance: core.NewWorkflowInstance("instanceID", "executionID"),
		History: []history.Event{
			history.NewHistoryEvent(
				history.EventType_WorkflowExecutionStarted,
				-1,
				&history.ExecutionStartedAttributes{
					Name:   "Workflow1",
					Inputs: []payload.Payload{},
				},
			),
		},
	}

	e := &executor{
		registry:      r,
		task:          task,
		workflow:      NewWorkflow(reflect.ValueOf(Workflow1)),
		workflowState: newWorkflowState(),
		logger:        log.Default(),
	}

	e.Execute(context.Background())

	require.Equal(t, 1, workflowHits)
	require.True(t, e.workflow.Completed())
	require.Len(t, e.workflowState.commands, 1)
}

var workflowActivityHit int

func workflowWithActivity(ctx sync.Context) error {
	workflowActivityHit++

	f1 := ExecuteActivity(ctx, activity1, 42)

	var r int
	err := f1.Get(ctx, &r)
	if err != nil {
		panic("error getting activity 1 result")
	}

	workflowActivityHit++

	return nil
}

func Test_ReplayWorkflowWithActivityResult(t *testing.T) {
	r := NewRegistry()

	workflowActivityHit = 0

	r.RegisterWorkflow(workflowWithActivity)
	r.RegisterActivity(activity1)

	inputs, _ := converter.DefaultConverter.To(42)
	result, _ := converter.DefaultConverter.To(42)

	task := &task.Workflow{
		WorkflowInstance: core.NewWorkflowInstance("instanceID", "executionID"),
		History: []history.Event{
			history.NewHistoryEvent(
				history.EventType_WorkflowExecutionStarted,
				-1,
				&history.ExecutionStartedAttributes{
					Name:   "WorkflowWithActivity",
					Inputs: []payload.Payload{inputs},
				},
			),
			history.NewHistoryEvent(
				history.EventType_ActivityScheduled,
				0,
				&history.ActivityScheduledAttributes{
					Name:   "activity1",
					Inputs: []payload.Payload{inputs},
				},
			),
			history.NewHistoryEvent(
				history.EventType_ActivityCompleted,
				0,
				&history.ActivityCompletedAttributes{
					Result: result,
				},
			),
		},
	}

	e := &executor{
		registry:      r,
		task:          task,
		workflow:      NewWorkflow(reflect.ValueOf(workflowWithActivity)),
		workflowState: newWorkflowState(),
		logger:        log.Default(),
	}

	e.Execute(context.Background())

	require.Equal(t, 2, workflowActivityHit)
	require.True(t, e.workflow.Completed())
	require.Len(t, e.workflowState.commands, 1)
}

func Test_ExecuteWorkflowWithActivityCommand(t *testing.T) {
	r := NewRegistry()

	workflowActivityHit = 0

	r.RegisterWorkflow(workflowWithActivity)
	r.RegisterActivity(activity1)

	task := &task.Workflow{
		WorkflowInstance: core.NewWorkflowInstance("instanceID", "executionID"),
		History: []history.Event{
			history.NewHistoryEvent(
				history.EventType_WorkflowExecutionStarted,
				-1,
				&history.ExecutionStartedAttributes{
					Name:   "workflowWithActivity",
					Inputs: []payload.Payload{},
				},
			),
		},
	}

	e := &executor{
		registry:      r,
		task:          task,
		workflow:      NewWorkflow(reflect.ValueOf(workflowWithActivity)),
		workflowState: newWorkflowState(),
		logger:        log.Default(),
	}

	e.Execute(context.Background())

	require.Equal(t, 1, workflowActivityHit)
	require.Len(t, e.workflowState.commands, 1)

	inputs, _ := converter.DefaultConverter.To(42)
	require.Equal(t, command.Command{
		ID:   0,
		Type: command.CommandType_ScheduleActivityTask,
		Attr: &command.ScheduleActivityTaskCommandAttr{
			Name:   "activity1",
			Inputs: []payload.Payload{inputs},
		},
	}, e.workflowState.commands[0])
}

var workflowTimerHits int

func workflowWithTimer(ctx sync.Context) error {
	workflowTimerHits++

	var r bool
	if err := ScheduleTimer(ctx, time.Millisecond*5).Get(ctx, &r); err != nil {
		panic("error getting timer future")
	}

	workflowTimerHits++

	return nil
}

func Test_ExecuteWorkflowWithTimer(t *testing.T) {
	r := NewRegistry()

	workflowTimerHits = 0

	r.RegisterWorkflow(workflowWithTimer)

	task := &task.Workflow{
		WorkflowInstance: core.NewWorkflowInstance("instanceID", "executionID"),
		History: []history.Event{
			history.NewHistoryEvent(
				history.EventType_WorkflowExecutionStarted,
				-1,
				&history.ExecutionStartedAttributes{
					Name:   "workflowWithTimer",
					Inputs: []payload.Payload{},
				},
			),
		},
	}

	e := &executor{
		registry:      r,
		task:          task,
		workflow:      NewWorkflow(reflect.ValueOf(workflowWithTimer)),
		workflowState: newWorkflowState(),
		logger:        log.Default(),
	}

	e.Execute(context.Background())

	require.Equal(t, 1, workflowTimerHits)
	require.Len(t, e.workflowState.commands, 1)

	require.Equal(t, 0, e.workflowState.commands[0].ID)
	require.Equal(t, command.CommandType_ScheduleTimer, e.workflowState.commands[0].Type)
}

var workflowWithSelectorHits int

func workflowWithSelector(ctx sync.Context) error {
	workflowWithSelectorHits++

	f1 := ExecuteActivity(ctx, activity1, 42)
	t := ScheduleTimer(ctx, time.Millisecond*2)

	s := sync.NewSelector()
	s.AddFuture(f1, func(ctx sync.Context, f sync.Future) {
		workflowWithSelectorHits++
	})

	s.AddFuture(t, func(ctx sync.Context, t sync.Future) {
		workflowWithSelectorHits++
	})

	s.Select(ctx)

	workflowWithSelectorHits++

	return nil
}

func Test_ExecuteWorkflowWithSelector(t *testing.T) {
	r := NewRegistry()

	r.RegisterWorkflow(workflowWithSelector)
	r.RegisterActivity(activity1)

	task := &task.Workflow{
		WorkflowInstance: core.NewWorkflowInstance("instanceID", "executionID"),
		History: []history.Event{
			history.NewHistoryEvent(
				history.EventType_WorkflowExecutionStarted,
				-1,
				&history.ExecutionStartedAttributes{
					Name:   "workflowWithSelector",
					Inputs: []payload.Payload{},
				},
			),
		},
	}

	e := &executor{
		registry:      r,
		task:          task,
		workflow:      NewWorkflow(reflect.ValueOf(workflowWithSelector)),
		workflowState: newWorkflowState(),
		logger:        log.Default(),
	}

	e.Execute(context.Background())

	require.Equal(t, 1, workflowWithSelectorHits)
	require.Len(t, e.workflowState.commands, 2)

	require.Equal(t, command.CommandType_ScheduleActivityTask, e.workflowState.commands[0].Type)
	require.Equal(t, command.CommandType_ScheduleTimer, e.workflowState.commands[1].Type)
}

func Test_ExecuteNewEvents(t *testing.T) {
	r := NewRegistry()

	workflowActivityHit = 0

	r.RegisterWorkflow(workflowWithActivity)
	r.RegisterActivity(activity1)

	inputs, _ := converter.DefaultConverter.To(42)
	result, _ := converter.DefaultConverter.To(42)

	oldTask := &task.Workflow{
		WorkflowInstance: core.NewWorkflowInstance("instanceID", "executionID"),
		History:          []history.Event{},
		NewEvents: []history.Event{
			history.NewHistoryEvent(
				history.EventType_WorkflowExecutionStarted,
				-1,
				&history.ExecutionStartedAttributes{
					Name:   "workflowWithActivity",
					Inputs: []payload.Payload{inputs},
				},
			),
			history.NewHistoryEvent(
				history.EventType_ActivityScheduled,
				0,
				&history.ActivityScheduledAttributes{
					Name:   "activity1",
					Inputs: []payload.Payload{inputs},
				},
			),
		},
	}

	e := &executor{
		registry:      r,
		task:          oldTask,
		workflow:      NewWorkflow(reflect.ValueOf(workflowWithActivity)),
		workflowState: newWorkflowState(),
		logger:        log.Default(),
	}

	e.Execute(context.Background())

	require.Equal(t, 1, workflowActivityHit)
	require.False(t, e.workflow.Completed())
	require.Len(t, e.workflowState.commands, 0)

	newTask := &task.Workflow{
		WorkflowInstance: oldTask.WorkflowInstance,
		History:          oldTask.NewEvents,
		NewEvents: []history.Event{
			history.NewHistoryEvent(
				history.EventType_ActivityCompleted,
				0,
				&history.ActivityCompletedAttributes{
					Result: result,
				},
			),
		},
	}

	// Execute the workflow again with the activity completed event
	_, err := e.ExecuteContinuationTask(context.Background(), newTask)

	require.NoError(t, err)
	require.Equal(t, 2, workflowActivityHit)
	require.True(t, e.workflow.Completed())
	require.Len(t, e.workflowState.commands, 1)
}
