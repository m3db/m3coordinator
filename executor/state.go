package executor

import (
	"context"
	"fmt"

	"github.com/m3db/m3coordinator/parser"
	"github.com/m3db/m3coordinator/plan"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/util/execution"
)

// ExecutionState represents the execution hierarchy
type ExecutionState struct {
	plan       plan.PhysicalPlan
	sources    []parser.Source
	resultNode parser.OpNode
	storage    storage.Storage
}

// GenerateExecutionState creates an execution state from the physical plan
func GenerateExecutionState(plan plan.PhysicalPlan, storage storage.Storage) (*ExecutionState, error) {
	result := plan.ResultStep
	state := &ExecutionState{
		plan:    plan,
		storage: storage,
	}

	if len(result.Parents) > 1 {
		return nil, fmt.Errorf("result node should have a single parent")
	}

	step, ok := plan.Step(result.Parents[0])
	if !ok {
		return nil, fmt.Errorf("incorrect parent reference in result node, parentId: %s, node: %s", result.Parents[0], result.ID())
	}

	controller, err := state.createNode(step)
	if err != nil {
		return nil, err
	}

	if len(state.sources) == 0 {
		return nil, fmt.Errorf("empty sources for the execution state")
	}

	state.resultNode = plan.ResultNode{}
	controller.AddTransform(state.resultNode)

	return state, nil
}

// createNode helps to create an execution node recursively
// TODO: consider modifying this function so that ExecutionState can have a non pointer receiver
func (s *ExecutionState) createNode(step plan.LogicalStep) (*parser.TransformController, error) {
	// TODO: consider using a registry instead of casting to an interface
	sourceParams, ok := step.Transform.Op.(parser.SourceParams)
	if ok {
		source, controller := parser.CreateSource(step.ID(), sourceParams, s.storage)
		s.sources = append(s.sources, source)
		return controller, nil
	}

	transformParams, ok := step.Transform.Op.(parser.TransformParams)
	if !ok {
		return nil, fmt.Errorf("invalid transform step, %s", step)
	}

	transformNode, controller := parser.CreateTransform(step.ID(), transformParams)
	for _, parentID := range step.Parents {
		parentStep, ok := s.plan.Step(parentID)
		if !ok {
			return nil, fmt.Errorf("incorrect parent reference, parentId: %s, node: %s", parentID, step.ID())
		}

		parentController, err := s.createNode(parentStep)
		if err != nil {
			return nil, err
		}

		parentController.AddTransform(transformNode)
	}

	return controller, nil
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
	source parser.Source
}

func (s sourceRequest) Process(ctx context.Context) error {
	return s.source.Execute(ctx)
}
