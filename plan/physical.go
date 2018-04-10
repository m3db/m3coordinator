package plan

import (
	"fmt"

	"github.com/m3db/m3coordinator/parser"
	"github.com/m3db/m3coordinator/storage"
)

// PhysicalPlan represents the physical plan
type PhysicalPlan struct {
	Steps      map[parser.TransformID]*LogicalStep
	Pipeline   []parser.TransformID // Ordered list of steps to be performed
	ResultStep *LogicalStep
}

// newPhysicalPlan generates a new physical plan after cloning the logical plan so that any changes here do not update the logical plan
func newPhysicalPlan(lp *LogicalPlan) *PhysicalPlan {
	cloned := lp.Clone()
	return &PhysicalPlan{
		Steps:    cloned.Steps,
		Pipeline: cloned.Pipeline,
	}
}

// GeneratePhysicalPlan is used to generate a physical plan. Its responsibilities include creating consolidation nodes, result nodes,
// pushing down predicates, changing the ordering for nodes
func GeneratePhysicalPlan(lp *LogicalPlan, storage storage.Storage) (*PhysicalPlan, error) {
	p := newPhysicalPlan(lp)
	if err := p.createResultNode(); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *PhysicalPlan) createResultNode() error {
	leaf, err := p.leafNode()
	if err != nil {
		return err
	}

	resultNode := parser.NewTransformFromOperation(&ResultOp{}, len(p.Steps)+1)
	resultStep := &LogicalStep{
		Transform: resultNode,
		Parents:   []parser.TransformID{leaf.ID()},
		Children:  []parser.TransformID{},
	}

	p.ResultStep = resultStep
	return nil
}

func (p *PhysicalPlan) leafNode() (*LogicalStep, error) {
	var leaf *LogicalStep
	for _, transformID := range p.Pipeline {
		node, ok := p.Steps[transformID]
		if !ok {
			return nil, fmt.Errorf("transform not found, %s", transformID)
		}

		if len(node.Children) == 0 {
			if leaf != nil {
				return nil, fmt.Errorf("multiple leaf nodes found, %v - %v", leaf, node)
			}

			leaf = node
		}
	}

	return leaf, nil
}

func (p *PhysicalPlan) String() string {
	return fmt.Sprintf("Steps: %s, Pipeline: %s, Result: %s", p.Steps, p.Pipeline, p.ResultStep)
}
