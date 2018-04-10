package plan

import (
	"fmt"

	"github.com/m3db/m3coordinator/parser"
	"github.com/m3db/m3coordinator/storage"
)

// Physical represents the physical plan
type Physical struct {
	Transforms map[parser.TransformID]*LogicalStep
	Pipeline   []parser.TransformID // Ordered list of steps to be performed
	ResultStep *LogicalStep
}

// newPhysicalPlan generates a new physical plan after cloning the logical plan so that any changes here do not update the logical plan
func newPhysicalPlan(lp *LogicalPlan) *Physical {
	cloned := lp.Clone()
	return &Physical{
		Transforms: cloned.Transforms,
		Pipeline:   cloned.Pipeline,
	}
}

// PhysicalPlan is used to generate a physical plan. Its responsibilities include creating consolidation nodes, result nodes,
// pushing down predicates, changing the ordering for nodes
func PhysicalPlan(lp *LogicalPlan, storage storage.Storage) (*Physical, error) {
	p := newPhysicalPlan(lp)
	if err := p.createResultNode(); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *Physical) createResultNode() error {
	leaf, err := p.leafNode()
	if err != nil {
		return err
	}

	resultNode := parser.NewTransformFromOperation(&ResultOp{}, len(p.Transforms)+1)
	resultStep := newLogicalStep(resultNode)
	resultStep.Parents = append(resultStep.Parents, leaf.Transform.ID())
	p.ResultStep = resultStep
	return nil
}

func (p *Physical) leafNode() (*LogicalStep, error) {
	var leaf *LogicalStep
	for _, transformID := range p.Pipeline {
		node, ok := p.Transforms[transformID]
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

func (p *Physical) String() string {
	return fmt.Sprintf("Transforms: %s, Pipeline: %s, Result: %s", p.Transforms, p.Pipeline, p.ResultStep)
}
