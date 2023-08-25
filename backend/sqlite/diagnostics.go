package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/cschleiden/go-workflows/diag"
	"github.com/cschleiden/go-workflows/internal/core"
)

var _ diag.Backend = (*sqliteBackend)(nil)

func (sb *sqliteBackend) GetWorkflowInstances(ctx context.Context, afterInstanceID, afterExecutionID string, count int) ([]*diag.WorkflowInstanceRef, error) {
	var err error
	tx, err := sb.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var rows *sql.Rows
	if afterInstanceID != "" {
		rows, err = tx.QueryContext(
			ctx,
			`SELECT i.id, i.execution_id, i.created_at, i.completed_at
			FROM instances i
			INNER JOIN (SELECT id, created_at FROM instances WHERE id = ? AND execution_id = ?) ii
				ON i.created_at < ii.created_at OR (i.created_at = ii.created_at AND i.id < ii.id)
			ORDER BY i.created_at DESC, i.id DESC
			LIMIT ?`,
			afterInstanceID,
			afterExecutionID,
			count,
		)
	} else {
		rows, err = tx.QueryContext(
			ctx,
			`SELECT i.id, i.execution_id, i.created_at, i.completed_at
			FROM instances i
			ORDER BY i.created_at DESC, i.id DESC
			LIMIT ?`,
			count,
		)
	}
	if err != nil {
		return nil, err
	}

	var instances []*diag.WorkflowInstanceRef

	for rows.Next() {
		var id, executionID string
		var createdAt time.Time
		var completedAt *time.Time
		err = rows.Scan(&id, &executionID, &createdAt, &completedAt)
		if err != nil {
			return nil, err
		}

		var state core.WorkflowInstanceState
		if completedAt != nil {
			state = core.WorkflowInstanceStateFinished
		}

		instances = append(instances, &diag.WorkflowInstanceRef{
			Instance:    core.NewWorkflowInstance(id, executionID),
			CreatedAt:   createdAt,
			CompletedAt: completedAt,
			State:       state,
		})
	}

	return instances, nil
}

func (sb *sqliteBackend) GetWorkflowInstance(ctx context.Context, instance *core.WorkflowInstance) (*diag.WorkflowInstanceRef, error) {
	tx, err := sb.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	res := tx.QueryRowContext(ctx, "SELECT id, execution_id, created_at, completed_at FROM instances WHERE id = ? AND execution_id = ?", instance.InstanceID, instance.ExecutionID)

	var id, executionID string
	var createdAt time.Time
	var completedAt *time.Time

	if instance.ExecutionID == "" {
		// Get the latest execution ID if it's not provided
		err = tx.QueryRowContext(ctx, "SELECT execution_id FROM instances WHERE id = ? ORDER BY created_at DESC LIMIT 1", instance.InstanceID).Scan(&executionID)
		if err != nil {
			return nil, err
		}
	} else {
		executionID = instance.ExecutionID
	}

	err = res.Scan(&id, &executionID, &createdAt, &completedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}

		return nil, err
	}

	var state core.WorkflowInstanceState
	if completedAt != nil {
		state = core.WorkflowInstanceStateFinished
	}

	return &diag.WorkflowInstanceRef{
		Instance:    core.NewWorkflowInstance(id, executionID),
		CreatedAt:   createdAt,
		CompletedAt: completedAt,
		State:       state,
	}, nil
}

func (sb *sqliteBackend) GetWorkflowTree(ctx context.Context, instance *core.WorkflowInstance) (*diag.WorkflowInstanceTree, error) {
	itb := diag.NewInstanceTreeBuilder(sb)
	return itb.BuildWorkflowInstanceTree(ctx, instance)
}
