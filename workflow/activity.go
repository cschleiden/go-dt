package workflow

import (
	"fmt"

	"github.com/cschleiden/go-workflows/core"
	a "github.com/cschleiden/go-workflows/internal/args"
	"github.com/cschleiden/go-workflows/internal/command"
	"github.com/cschleiden/go-workflows/internal/contextvalue"
	"github.com/cschleiden/go-workflows/internal/fn"
	"github.com/cschleiden/go-workflows/internal/sync"
	"github.com/cschleiden/go-workflows/internal/workflowstate"
)

type ActivityOptions struct {
	// Queue defines the activity queue this activity will be delivered to. By default, this inherits
	// the queue from the workflow instance.
	Queue core.Queue

	// RetryOptions defines how to retry the activity in case of failure.
	RetryOptions RetryOptions
}

var DefaultActivityOptions = ActivityOptions{
	RetryOptions: DefaultRetryOptions,
}

// ExecuteActivity schedules the given activity to be executed
func ExecuteActivity[TResult any](ctx Context, options ActivityOptions, activity Activity, args ...any) Future[TResult] {
	return WithRetries(ctx, options.RetryOptions, func(ctx Context, attempt int) Future[TResult] {
		return executeActivity[TResult](ctx, options, attempt, activity, args...)
	})
}

func executeActivity[TResult any](ctx Context, options ActivityOptions, attempt int, activity Activity, args ...any) Future[TResult] {
	f := sync.NewFuture[TResult]()

	if ctx.Err() != nil {
		f.Set(*new(TResult), ctx.Err())
		return f
	}

	var name string

	// Check if activity is referred by name
	if activityName, ok := activity.(string); ok {
		name = activityName
	} else {
		// Check return type
		if err := a.ReturnTypeMatch[TResult](activity); err != nil {
			f.Set(*new(TResult), err)
			return f
		}

		// Check arguments
		if err := a.ParamsMatch(activity, args...); err != nil {
			f.Set(*new(TResult), err)
			return f
		}

		name = fn.Name(activity)
	}

	cv := contextvalue.Converter(ctx)
	inputs, err := a.ArgsToInputs(cv, args...)
	if err != nil {
		f.Set(*new(TResult), fmt.Errorf("converting activity input: %w", err))
		return f
	}

	wfState := workflowstate.WorkflowState(ctx)
	scheduleEventID := wfState.GetNextScheduleEventID()

	// Capture context
	propagators := propagators(ctx)
	metadata := &Metadata{}
	if err := injectFromWorkflow(ctx, metadata, propagators); err != nil {
		f.Set(*new(TResult), fmt.Errorf("injecting workflow context: %w", err))
		return f
	}

	cmd := command.NewScheduleActivityCommand(scheduleEventID, name, inputs, attempt, metadata, options.Queue)
	wfState.AddCommand(cmd)
	wfState.TrackFuture(scheduleEventID, workflowstate.AsDecodingSettable(cv, fmt.Sprintf("activity: %s", name), f))

	// Handle cancellation
	if d := ctx.Done(); d != nil {
		if c, ok := d.(sync.ChannelInternal[struct{}]); ok {
			if _, ok := c.ReceiveNonBlocking(); ok {
				// Workflow has been canceled, check if the activity has already been scheduled, no need to schedule otherwise
				if cmd.State() == command.CommandState_Pending {
					cmd.Done()
					wfState.RemoveFuture(scheduleEventID)
					f.Set(*new(TResult), Canceled)
				}
			}
		}
	}

	return f
}
