package plan

import (
	"fmt"

	"github.com/m3db/m3coordinator/parser"
)

// LogicalPlan converts a DAG into a list of steps to be executed in order
type LogicalPlan struct {
	Steps    map[parser.TransformID]*LogicalStep
	Pipeline []parser.TransformID // Ordered list of steps to be performed
}

// LogicalStep is a single step in a logical plan
type LogicalStep struct {
	Parents   []parser.TransformID
	Children  []parser.TransformID
	Transform *parser.Transform
}

// newLogicalPlan returns an empty logical plan
func newLogicalPlan() *LogicalPlan {
	return &LogicalPlan{
		Steps:    make(map[parser.TransformID]*LogicalStep),
		Pipeline: make([]parser.TransformID, 0),
	}
}

// newLogicalStep returns an empty plan step
func newLogicalStep(Transform *parser.Transform) *LogicalStep {
	return &LogicalStep{
		Transform: Transform,
		Parents:   make([]parser.TransformID, 0),
		Children:  make([]parser.TransformID, 0),
	}
}

// GenerateLogicalPlan creates a plan from the DAG structure
func GenerateLogicalPlan(transforms parser.Transforms, edges parser.Edges) (*LogicalPlan, error) {
	lp := newLogicalPlan()

	// Create all steps
	for _, transform := range transforms {
		lp.Steps[transform.ID()] = newLogicalStep(transform)
		lp.Pipeline = append(lp.Pipeline, transform.ID())
	}

	// Link all parent/children
	for _, edge := range edges {
		parent, ok := lp.Steps[edge.ParentID]
		if !ok {
			return nil, fmt.Errorf("invalid DAG found, parent %s not found", edge.ParentID)
		}

		child, ok := lp.Steps[edge.ChildID]
		if !ok {
			return nil, fmt.Errorf("invalid DAG found, child %s not found", edge.ChildID)
		}

		parent.Children = append(parent.Children, child.ID())
		child.Parents = append(child.Parents, parent.ID())
	}

	return lp, nil
}

func (l *LogicalPlan) String() string {
	return fmt.Sprintf("Steps: %s, Pipeline: %s", l.Steps, l.Pipeline)
}

// Clone the plan
func (l *LogicalPlan) Clone() *LogicalPlan {
	steps := make(map[parser.TransformID]*LogicalStep)
	for id, step := range l.Steps {
		steps[id] = step.Clone()
	}

	pipeline := make([]parser.TransformID, len(l.Pipeline))
	copy(pipeline, l.Pipeline)
	return &LogicalPlan{
		Steps:    steps,
		Pipeline: pipeline,
	}
}

func (l *LogicalStep) String() string {
	return fmt.Sprintf("Parents: %s, Children: %s, Transform: %s", l.Parents, l.Children, l.Transform)
}

// ID is a convenience method to expose the inner transforms' ID
func (l *LogicalStep) ID() parser.TransformID {
	return l.Transform.ID()
}

// Clone the step, the transform is immutable so its left as is
func (l *LogicalStep) Clone() *LogicalStep {
	parents := make([]parser.TransformID, len(l.Parents))
	copy(parents, l.Parents)

	children := make([]parser.TransformID, len(l.Children))
	copy(children, l.Children)

	return &LogicalStep{
		Transform: l.Transform,
		Parents:   parents,
		Children:  children,
	}
}
