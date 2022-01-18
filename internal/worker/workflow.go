package worker

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/cschleiden/go-dt/internal/command"
	"github.com/cschleiden/go-dt/internal/workflow"
	"github.com/cschleiden/go-dt/pkg/backend"
	"github.com/cschleiden/go-dt/pkg/core"
	"github.com/cschleiden/go-dt/pkg/core/task"
	"github.com/cschleiden/go-dt/pkg/history"
	"github.com/google/uuid"
)

type WorkflowWorker interface {
	Start(context.Context) error

	// Poll(ctx context.Context, timeout time.Duration) (*task.WorkflowTask, error)
}

type workflowWorker struct {
	backend backend.Backend

	registry *workflow.Registry

	cache workflow.WorkflowExecutorCache

	workflowTaskQueue chan task.Workflow

	logger *log.Logger
}

func NewWorkflowWorker(backend backend.Backend, registry *workflow.Registry) WorkflowWorker {
	return &workflowWorker{
		backend: backend,

		registry:          registry,
		workflowTaskQueue: make(chan task.Workflow),

		cache: workflow.NewWorkflowExecutorCache(workflow.DefaultWorkflowExecutorCacheOptions),

		logger: log.Default(),
	}
}

func (ww *workflowWorker) Start(ctx context.Context) error {
	go ww.cache.StartEviction(ctx)

	go ww.runPoll(ctx)

	go ww.runDispatcher(ctx)

	return nil
}

func (ww *workflowWorker) runPoll(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			task, err := ww.poll(ctx, 30*time.Second)
			if err != nil {
				log.Println("error while polling for workflow task:", err)
			} else if task != nil {
				ww.workflowTaskQueue <- *task
			}
		}
	}
}

func (ww *workflowWorker) runDispatcher(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-ww.workflowTaskQueue:
			if t.Kind == task.Continuation {
				go ww.handleContinuationTask(ctx, t)
			} else {
				go ww.handleTask(ctx, t)
			}
		}
	}
}

func (ww *workflowWorker) handleTask(ctx context.Context, task task.Workflow) {
	// Start heartbeat while processing workflow task
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	defer cancelHeartbeat()
	go ww.heartbeatTask(heartbeatCtx, &task)

	workflowTaskExecutor := workflow.NewExecutor(ww.registry, &task)

	if err := ww.cache.Store(ctx, task.WorkflowInstance, workflowTaskExecutor); err != nil {
		ww.logger.Println("error while storing workflow task executor:", err)
	}

	commands, err := workflowTaskExecutor.Execute(ctx)
	if err != nil {
		ww.logger.Panic(err)
	}

	cancelHeartbeat()

	newEvents, workflowEvents := ww.processCommands(ctx, task.WorkflowInstance, commands)

	if err := ww.backend.CompleteWorkflowTask(ctx, task, newEvents, workflowEvents); err != nil {
		ww.logger.Panic(err)
	}
}

func (ww *workflowWorker) handleContinuationTask(ctx context.Context, task task.Workflow) {
	// Start heartbeat while processing workflow task
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	defer cancelHeartbeat()
	go ww.heartbeatTask(heartbeatCtx, &task)

	workflowTaskExecutor, ok, cerr := ww.cache.Get(ctx, task.WorkflowInstance)
	if cerr != nil {
		// TODO: Can we fall back to getting a full task here?
		ww.logger.Fatal(cerr)
	} else if !ok {
		ww.logger.Fatal(errors.New("workflow task executor not found in cache"))
	}

	commands, err := workflowTaskExecutor.ExecuteContinuationTask(ctx, &task)
	if err != nil {
		ww.logger.Panic(err)
	}

	cancelHeartbeat()

	newEvents, workflowEvents := ww.processCommands(ctx, task.WorkflowInstance, commands)

	if err := ww.backend.CompleteWorkflowTask(ctx, task, newEvents, workflowEvents); err != nil {
		ww.logger.Panic(err)
	}
}

func (ww *workflowWorker) heartbeatTask(ctx context.Context, task *task.Workflow) {
	t := time.NewTicker(25 * time.Second)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := ww.backend.ExtendWorkflowTask(ctx, task.WorkflowInstance); err != nil {
				ww.logger.Panic(err)
			}
		}
	}
}

func (ww *workflowWorker) processCommands(ctx context.Context, instance core.WorkflowInstance, commands []command.Command) ([]history.Event, []core.WorkflowEvent) {
	newEvents := make([]history.Event, 0)
	workflowEvents := make([]core.WorkflowEvent, 0)

	for _, c := range commands {
		switch c.Type {
		case command.CommandType_ScheduleActivityTask:
			a := c.Attr.(*command.ScheduleActivityTaskCommandAttr)

			newEvents = append(newEvents, history.NewHistoryEvent(
				history.EventType_ActivityScheduled,
				c.ID,
				&history.ActivityScheduledAttributes{
					Name:   a.Name,
					Inputs: a.Inputs,
				},
			))

		case command.CommandType_ScheduleSubWorkflow:
			a := c.Attr.(*command.ScheduleSubWorkflowCommandAttr)

			subWorkflowInstance := core.NewSubWorkflowInstance(a.InstanceID, uuid.NewString(), instance, c.ID)

			newEvents = append(newEvents, history.NewHistoryEvent(
				history.EventType_SubWorkflowScheduled,
				c.ID,
				&history.SubWorkflowScheduledAttributes{
					InstanceID: subWorkflowInstance.GetInstanceID(),
					Name:       a.Name,
					Inputs:     a.Inputs,
				},
			))

			// Send message to new workflow instance
			workflowEvents = append(workflowEvents, core.WorkflowEvent{
				WorkflowInstance: subWorkflowInstance,
				HistoryEvent: history.NewHistoryEvent(
					history.EventType_WorkflowExecutionStarted,
					c.ID,
					&history.ExecutionStartedAttributes{
						Name:   a.Name,
						Inputs: a.Inputs,
					},
				),
			})

		case command.CommandType_ScheduleTimer:
			a := c.Attr.(*command.ScheduleTimerCommandAttr)

			newEvents = append(newEvents, history.NewHistoryEvent(
				history.EventType_TimerScheduled,
				c.ID,
				&history.TimerScheduledAttributes{
					At: a.At,
				},
			))

			// Create timer_fired event which will become visible in the future
			workflowEvents = append(workflowEvents, core.WorkflowEvent{
				WorkflowInstance: instance,
				HistoryEvent: history.NewFutureHistoryEvent(
					history.EventType_TimerFired,
					c.ID,
					&history.TimerFiredAttributes{
						At: a.At,
					},
					a.At,
				)},
			)

		case command.CommandType_CompleteWorkflow:
			a := c.Attr.(*command.CompleteWorkflowCommandAttr)

			newEvents = append(newEvents, history.NewHistoryEvent(
				history.EventType_WorkflowExecutionFinished,
				c.ID,
				&history.ExecutionCompletedAttributes{
					Result: a.Result,
					Error:  a.Error,
				},
			))

			if instance.SubWorkflow() {
				workflowEvents = append(workflowEvents, core.WorkflowEvent{
					WorkflowInstance: instance.ParentInstance(),
					HistoryEvent: history.NewHistoryEvent(
						history.EventType_SubWorkflowCompleted,
						instance.ParentEventID(), // Ensure the message gets sent back to the parent workflow with the right eventID
						&history.SubWorkflowCompletedAttributes{
							Result: a.Result,
							Error:  a.Error,
						},
					),
				})
			}

		default:
			ww.logger.Panicf("unknown command type: %v", c.Type)
		}
	}

	return newEvents, workflowEvents
}

func (ww *workflowWorker) poll(ctx context.Context, timeout time.Duration) (*task.Workflow, error) {
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	done := make(chan struct{})

	var task *task.Workflow
	var err error

	go func() {
		task, err = ww.backend.GetWorkflowTask(ctx)
		close(done)
	}()

	select {
	case <-ctx.Done():
		return nil, nil

	case <-done:
		return task, err
	}
}
