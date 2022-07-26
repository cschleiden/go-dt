package worker

import (
	"context"
	"sync"

	"github.com/benbjohnson/clock"
	"github.com/ticctech/go-workflows/backend"
	internal "github.com/ticctech/go-workflows/internal/worker"
	workflowinternal "github.com/ticctech/go-workflows/internal/workflow"
	"github.com/ticctech/go-workflows/workflow"
)

type WorkflowRegistry interface {
	RegisterWorkflow(w workflow.Workflow) error
}

type ActivityRegistry interface {
	RegisterActivity(a interface{}) error
}

type Registry interface {
	WorkflowRegistry
	ActivityRegistry
}

type Worker interface {
	Registry

	// Start starts the worker.
	//
	// To stop the worker, cancel the context passed to Start. To wait for completion of the active
	// work items, call `WaitForCompletion`.
	Start(ctx context.Context) error

	// WaitForCompletion
	WaitForCompletion() error
}

type worker struct {
	backend backend.Backend

	done chan struct{}
	wg   *sync.WaitGroup

	registry *workflowinternal.Registry

	workflowWorker internal.WorkflowWorker
	activityWorker internal.ActivityWorker

	workflows  map[string]interface{}
	activities map[string]interface{}
}

type Options = internal.Options

var DefaultWorkerOptions = internal.DefaultOptions

func New(backend backend.Backend, options *Options) Worker {
	if options == nil {
		options = &internal.DefaultOptions
	}

	registry := workflowinternal.NewRegistry()

	return &worker{
		backend: backend,

		done: make(chan struct{}),
		wg:   &sync.WaitGroup{},

		workflowWorker: internal.NewWorkflowWorker(backend, registry, options),
		activityWorker: internal.NewActivityWorker(backend, registry, clock.New(), options),

		registry: registry,

		workflows:  map[string]interface{}{},
		activities: map[string]interface{}{},
	}
}

func (w *worker) Start(ctx context.Context) error {
	w.workflowWorker.Start(ctx)
	w.activityWorker.Start(ctx)

	return nil
}

func (w *worker) WaitForCompletion() error {
	if err := w.workflowWorker.WaitForCompletion(); err != nil {
		return err
	}

	if err := w.activityWorker.WaitForCompletion(); err != nil {
		return err
	}

	return nil
}

func (w *worker) RegisterWorkflow(wf workflow.Workflow) error {
	return w.registry.RegisterWorkflow(wf)
}

func (w *worker) RegisterActivity(a interface{}) error {
	return w.registry.RegisterActivity(a)
}
