package plan

import (
	"fmt"

	"github.com/m3db/m3coordinator/parser"
)

type LogicalPlan struct {
	Transforms map[parser.TransformID]*PlanStep
	Pipeline   []parser.TransformID // Ordered list of steps to be performed
}

type PlanStep struct {
	Parents   []parser.TransformID
	Children  []parser.TransformID
	Transform *parser.Transform
}

func NewLogicalPlan() *LogicalPlan {
	return &LogicalPlan{
		Transforms: make(map[parser.TransformID]*PlanStep),
		Pipeline:   make([]parser.TransformID, 0),
	}
}

func NewPlanStep(Transform *parser.Transform) *PlanStep {
	return &PlanStep{
		Transform: Transform,
		Parents:   make([]parser.TransformID, 0),
		Children:  make([]parser.TransformID, 0),
	}
}

func GenerateLogicalPlan(transforms parser.Transforms, edges parser.Edges) (*LogicalPlan, error) {
	lp := NewLogicalPlan()

	// Create all steps
	for _, transform := range transforms {
		lp.Transforms[transform.ID] = NewPlanStep(transform)
		lp.Pipeline = append(lp.Pipeline, transform.ID)
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

		parent.Children = append(parent.Children, child.Transform.ID)
		child.Parents = append(child.Parents, parent.Transform.ID)
	}

	return lp, nil
}
