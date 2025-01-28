package workflow

import (
	"log/slog"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/cschleiden/go-workflows/backend/converter"
	"github.com/cschleiden/go-workflows/core"
	"github.com/cschleiden/go-workflows/internal/contextvalue"
	"github.com/cschleiden/go-workflows/internal/sync"
	"github.com/cschleiden/go-workflows/internal/workflowstate"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
)

func Test_executeActivity_ResultMismatch(t *testing.T) {
	a := func(ctx Context) (int, error) {
		return 42, nil
	}

	ctx := sync.Background()
	ctx = contextvalue.WithConverter(ctx, converter.DefaultConverter)
	ctx = workflowstate.WithWorkflowState(
		ctx,
		workflowstate.NewWorkflowState(
			core.NewWorkflowInstance("a", ""), slog.Default(), noop.NewTracerProvider().Tracer("test"), clock.New()),
	)

	c := sync.NewCoroutine(ctx, func(ctx Context) error {
		f := executeActivity[string](ctx, DefaultActivityOptions, 1, a)
		_, err := f.Get(ctx)
		require.Error(t, err)

		return nil
	})

	c.Execute()
	require.True(t, c.Finished())
}
func Test_executeActivity_ParamMismatch(t *testing.T) {
	a := func(ctx Context, s string, n int) (int, error) {
		return 42, nil
	}

	ctx := sync.Background()
	ctx = contextvalue.WithConverter(ctx, converter.DefaultConverter)
	ctx = workflowstate.WithWorkflowState(
		ctx,
		workflowstate.NewWorkflowState(
			core.NewWorkflowInstance("a", ""), slog.Default(), noop.NewTracerProvider().Tracer("test"), clock.New()),
	)

	c := sync.NewCoroutine(ctx, func(ctx Context) error {
		f := executeActivity[int](ctx, DefaultActivityOptions, 1, a)
		_, err := f.Get(ctx)
		require.Error(t, err)

		return nil
	})

	c.Execute()
	require.True(t, c.Finished())
}

func Test_executeActivity_NameAsString(t *testing.T) {
	ctx := sync.Background()
	ctx = contextvalue.WithConverter(ctx, converter.DefaultConverter)
	ctx = workflowstate.WithWorkflowState(
		ctx,
		workflowstate.NewWorkflowState(
			core.NewWorkflowInstance("actName", ""), slog.Default(), noop.NewTracerProvider().Tracer("test"), clock.New()),
	)

	c := sync.NewCoroutine(ctx, func(ctx Context) error {
		executeActivity[int](ctx, DefaultActivityOptions, 1, "actName", "foo", 1)
		return nil
	})

	c.Execute()

	require.NoError(t, c.Error())
	require.True(t, c.Finished())
}