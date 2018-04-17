package executor

import (
	"fmt"

	"github.com/m3db/m3coordinator/parser"
	"github.com/m3db/m3coordinator/plan"
)

type ExecutionState struct {
	plan plan.PhysicalPlan
}

func GenerateExecutionState(plan plan.PhysicalPlan) (*ExecutionState, error) {
	result := plan.ResultStep
	state := &ExecutionState{}
	_ := state.createNode(result)
	return state, nil

}

func (s *ExecutionState) createNode(step plan.LogicalStep) (*parser.TransformNode, error) {
	stepNode := step.Transform.Node()
	for _, parentID := range step.Parents {
		parentStep, ok := s.plan.Step(parentID)
		if !ok {
			return nil, fmt.Errorf("incorrect parent reference, parentId: %s, node: %s", parentID, step.ID())
		}

		parentNode, err := s.createNode(parentStep)
		if err != nil {
			return nil, err
		}

	}

}