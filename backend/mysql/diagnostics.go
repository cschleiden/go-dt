package mysql

import (
	"context"
	"database/sql"
	"time"

	"github.com/cschleiden/go-workflows/core"
	"github.com/cschleiden/go-workflows/diag"
)

var _ diag.Backend = (*mysqlBackend)(nil)

func (mb *mysqlBackend) GetWorkflowInstances(ctx context.Context, afterInstanceID, afterExecutionID string, count int) ([]*diag.WorkflowInstanceRef, error) {
	var err error
	tx, err := mb.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var rows *sql.Rows
	if afterInstanceID != "" {
		rows, err = tx.QueryContext(
			ctx,
			`SELECT i.instance_id, i.execution_id, i.created_at, i.completed_at
			FROM instances i
			INNER JOIN (SELECT instance_id, created_at FROM instances WHERE id = ? AND execution_id = ?) ii
				ON i.created_at < ii.created_at OR (i.created_at = ii.created_at AND i.instance_id < ii.instance_id)
			ORDER BY i.created_at DESC, i.instance_id DESC
			LIMIT ?`,
			afterInstanceID,
			afterExecutionID,
			count,
		)
	} else {
		rows, err = tx.QueryContext(
			ctx,
			`SELECT i.instance_id, i.execution_id, i.created_at, i.completed_at
			FROM instances i
			ORDER BY i.created_at DESC, i.instance_id DESC
			LIMIT ?`,
			count,
		)
	}
	if err != nil {
		return nil, err
	}

	defer rows.Close()

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

func (mb *mysqlBackend) GetWorkflowInstance(ctx context.Context, instance *core.WorkflowInstance) (*diag.WorkflowInstanceRef, error) {
	tx, err := mb.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	executionID := instance.ExecutionID
	if executionID == "" {
		executionID, err = mb.GetLatestExecutionID(ctx, instance.InstanceID)
		if err != nil {
			return nil, err
		}
	}

	res := tx.QueryRowContext(
		ctx,
		"SELECT instance_id, execution_id, created_at, completed_at FROM instances WHERE instance_id = ? AND execution_id = ?", instance.InstanceID, instance.ExecutionID)

	var id string
	var createdAt time.Time
	var completedAt *time.Time

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

func (mb *mysqlBackend) GetLatestExecutionID(ctx context.Context, instanceID string) (string, error) {
	tx, err := mb.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	var executionID string
	res := tx.QueryRowContext(
		ctx,
		"SELECT execution_id FROM instances WHERE instance_id = ? ORDER BY created_at DESC LIMIT 1", instanceID)

	err = res.Scan(&executionID)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}

		return "", err
	}

	return executionID, nil
}

func (mb *mysqlBackend) GetWorkflowTree(ctx context.Context, instance *core.WorkflowInstance) (*diag.WorkflowInstanceTree, error) {
	itb := diag.NewInstanceTreeBuilder(mb)
	return itb.BuildWorkflowInstanceTree(ctx, instance)
}
