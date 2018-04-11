package executor

import (
	"github.com/m3db/m3coordinator/parser"
	"github.com/m3db/m3coordinator/plan"
)

type ExecutionState struct {

}

func GenerateExecutionState(plan *plan.PhysicalPlan) (*ExecutionState, error) {
	result := plan.ResultStep
	state := &ExecutionState{}
	state.createNode(result)
	return state, nil

}

func (s *ExecutionState) createNode(step *plan.LogicalStep) *parser.TransformNode {
	stepNode := step.Transform.Node()
	for _, parent := range step.Parents {
	}

}