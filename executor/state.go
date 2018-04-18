package executor

import (
	"context"
	"fmt"

	"github.com/m3db/m3coordinator/parser"
	"github.com/m3db/m3coordinator/plan"
	"github.com/m3db/m3coordinator/util/execution"
)


// Source represents data sources which are handled differently than other transforms as they are always independent and can always be parallelized
type Source interface {
	Execute(ctx context.Context) error
}

// ExecutionState represents the execution hierarchy
type ExecutionState struct {
	plan       plan.PhysicalPlan
	sources    []Source
	resultNode parser.OpNode
}

// GenerateExecutionState creates an execution state from the physical plan
func GenerateExecutionState(plan plan.PhysicalPlan) (*ExecutionState, error) {
	result := plan.ResultStep
	state := &ExecutionState{
		plan: plan,
	}

	resultNode, _, err := state.createNode(result)
	if err != nil {
		return nil, err
	}

	if len(state.sources) == 0 {
		return nil, fmt.Errorf("empty sources for the execution state")
	}

	state.resultNode = resultNode
	return state, nil
}

// createNode helps to create an execution node recursively
// TODO: consider modifying this function so that ExecutionState can have a non pointer receiver
func (s *ExecutionState) createNode(step plan.LogicalStep) (parser.OpNode, *parser.TransformController, error) {
	stepNode, controller := step.Transform.Node()
	// TODO: consider using a registry instead of casting to an interface
	source, ok := stepNode.(Source)
	if ok {
		s.sources = append(s.sources, source)
	}

	for _, parentID := range step.Parents {
		parentStep, ok := s.plan.Step(parentID)
		if !ok {
			return nil, nil, fmt.Errorf("incorrect parent reference, parentId: %s, node: %s", parentID, step.ID())
		}

		_, parentController, err := s.createNode(parentStep)
		if err != nil {
			return nil, nil, err
		}

		parentController.AddTransform(stepNode)
	}

	return stepNode, controller, nil
}

// Execute the sources in parallel and return the first error
func (s *ExecutionState) Execute(ctx context.Context) error {
	requests := make([]execution.Request, len(s.sources))
	for idx, source := range s.sources {
		requests[idx] = sourceRequest{source}
	}

	return execution.ExecuteParallel(ctx, requests)
}

// String representation of the state
func (s *ExecutionState) String() string {
	return fmt.Sprintf("plan : %s\nsources: %s\nresult: %s", s.plan, s.sources, s.resultNode)
}

type sourceRequest struct {
	source Source
}

func (s sourceRequest) Process(ctx context.Context) error {
	return s.source.Execute(ctx)
}
