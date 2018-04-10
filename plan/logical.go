package plan

import (
	"fmt"

	"github.com/m3db/m3coordinator/parser"
)

// LogicalPlan converts a DAG into a list of steps to be executed in order
type LogicalPlan struct {
	Transforms map[parser.TransformID]*LogicalStep
	Pipeline   []parser.TransformID // Ordered list of steps to be performed
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
		Transforms: make(map[parser.TransformID]*LogicalStep),
		Pipeline:   make([]parser.TransformID, 0),
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
		lp.Transforms[transform.ID()] = newLogicalStep(transform)
		lp.Pipeline = append(lp.Pipeline, transform.ID())
	}

	// Link all parent/children
	for _, edge := range edges {
		parent, ok := lp.Transforms[edge.ParentID]
		if !ok {
			return nil, fmt.Errorf("invalid DAG found, parent %s not found", edge.ParentID)
		}

		child, ok := lp.Transforms[edge.ChildID]
		if !ok {
			return nil, fmt.Errorf("invalid DAG found, child %s not found", edge.ChildID)
		}

		parent.Children = append(parent.Children, child.Transform.ID())
		child.Parents = append(child.Parents, parent.Transform.ID())
	}

	return lp, nil
}

func (l *LogicalPlan) String() string {
	return fmt.Sprintf("Transforms: %s, Pipeline: %s", l.Transforms, l.Pipeline)
}

// Clone the plan, the transform is immutable so its left as is
func (l *LogicalPlan) Clone() *LogicalPlan {
	transforms := make(map[parser.TransformID]*LogicalStep)
	for id, step := range l.Transforms {
		transforms[id] = step.Clone()
	}

	pipeline := make([]parser.TransformID, len(l.Pipeline))
	copy(pipeline, l.Pipeline)
	return &LogicalPlan{
		Transforms: transforms,
		Pipeline:   pipeline,
	}
}

func (l *LogicalStep) String() string {
	return fmt.Sprintf("Parents: %s, Children: %s, Transform: %s", l.Parents, l.Children, l.Transform)
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
