package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/cschleiden/go-workflows/diag"
	"github.com/cschleiden/go-workflows/internal/core"
	"github.com/cschleiden/go-workflows/log"
	redis "github.com/redis/go-redis/v9"
)

var _ diag.Backend = (*redisBackend)(nil)

func (rb *redisBackend) GetWorkflowInstances(ctx context.Context, afterInstanceID, afterExecutionID string, count int) ([]*diag.WorkflowInstanceRef, error) {
	max := "+inf"

	if afterInstanceID != "" {
		afterID := instanceSegment(core.NewWorkflowInstance(afterInstanceID, afterExecutionID))
		scores, err := rb.rdb.ZMScore(ctx, instancesByCreation(), afterID).Result()
		if err != nil {
			return nil, fmt.Errorf("getting instance score for %v: %w", afterID, err)
		}

		if len(scores) == 0 {
			rb.Logger().Error("could not find instance %v",
				log.NamespaceKey+".redis.afterInstanceID", afterInstanceID,
				log.NamespaceKey+".redis.afterExecutionID", afterExecutionID,
			)
			return nil, nil
		}

		max = fmt.Sprintf("(%v", int64(scores[0]))
	}

	result, err := rb.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     instancesByCreation(),
		Stop:    max,
		Start:   "-inf",
		ByScore: true,
		Rev:     true,
		Count:   int64(count),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("getting instances after %v: %w", max, err)
	}

	instanceKeys := make([]string, 0)
	for _, r := range result {
		instanceSegment := r
		instanceKeys = append(instanceKeys, instanceKeyFromSegment(instanceSegment))
	}

	instances, err := rb.rdb.MGet(ctx, instanceKeys...).Result()
	if err != nil {
		return nil, fmt.Errorf("getting instances: %w", err)
	}

	var instanceRefs []*diag.WorkflowInstanceRef
	for _, instance := range instances {
		var state instanceState
		if err := json.Unmarshal([]byte(instance.(string)), &state); err != nil {
			return nil, fmt.Errorf("unmarshaling instance state: %w", err)
		}

		instanceRefs = append(instanceRefs, &diag.WorkflowInstanceRef{
			Instance:    state.Instance,
			CreatedAt:   state.CreatedAt,
			CompletedAt: state.CompletedAt,
			State:       state.State,
		})
	}

	return instanceRefs, nil
}

func (rb *redisBackend) GetWorkflowInstance(ctx context.Context, instance *core.WorkflowInstance) (*diag.WorkflowInstanceRef, error) {
	if instance.ExecutionID == "" {
		executionID, err := rb.GetLatestExecutionID(ctx, instance.InstanceID)
		if err != nil {
			return nil, err
		}
		instance.ExecutionID = executionID
	}

	instanceState, err := readInstance(ctx, rb.rdb, instanceKey(instance))
	if err != nil {
		return nil, err
	}

	return mapWorkflowInstance(instanceState), nil
}

func (rb *redisBackend) GetLatestExecutionID(ctx context.Context, instanceID string) (string, error) {
	keys, err := rb.rdb.Keys(ctx, fmt.Sprintf("instance:%s:*", instanceID)).Result()
	if err != nil {
		return "", err
	}

	var latestExecutionID string
	var latestCreatedAt time.Time

	for _, key := range keys {
		value, err := rb.rdb.Get(ctx, key).Result()
		if err != nil {
			return "", err
		}

		// Unmarshal the JSON into a structure to access the created_at field
		var instanceData struct {
			CreatedAt string `json:"created_at"`
		}
		if err := json.Unmarshal([]byte(value), &instanceData); err != nil {
			return "", err
		}

		createdAt, err := time.Parse(time.RFC3339, instanceData.CreatedAt)
		if err != nil {
			return "", err
		}

		if createdAt.After(latestCreatedAt) {
			latestCreatedAt = createdAt
			// Extract the execution ID from the key
			segments := strings.Split(key, ":")
			latestExecutionID = segments[len(segments)-1]
		}
	}

	return latestExecutionID, nil
}

func (rb *redisBackend) GetWorkflowTree(ctx context.Context, instance *core.WorkflowInstance) (*diag.WorkflowInstanceTree, error) {
	itb := diag.NewInstanceTreeBuilder(rb)
	return itb.BuildWorkflowInstanceTree(ctx, instance)
}

func mapWorkflowInstance(instance *instanceState) *diag.WorkflowInstanceRef {
	return &diag.WorkflowInstanceRef{
		Instance:    instance.Instance,
		CreatedAt:   instance.CreatedAt,
		CompletedAt: instance.CompletedAt,
		State:       instance.State,
	}
}
