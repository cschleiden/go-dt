package cache

import (
	"fmt"

	"github.com/cschleiden/go-workflows/core"
)

func getKey(instance *core.WorkflowInstance) string {
	return fmt.Sprintf("%s-%s", instance.InstanceID, instance.ExecutionID)
}
