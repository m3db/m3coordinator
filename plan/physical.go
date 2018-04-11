package plan

import (
	"fmt"

	"github.com/m3db/m3coordinator/parser"
	"github.com/m3db/m3coordinator/storage"
)

// PhysicalPlan represents the physical plan
type PhysicalPlan interface {
	fmt.Stringer
}

type physicalPlan struct {
	steps      map[parser.TransformID]*LogicalStep
	pipeline   []parser.TransformID // Ordered list of steps to be performed
	resultStep *LogicalStep
}

// NewPhysicalPlan is used to generate a physical plan. Its responsibilities include creating consolidation nodes, result nodes,
// pushing down predicates, changing the ordering for nodes
func NewPhysicalPlan(lp *LogicalPlan, storage storage.Storage) (PhysicalPlan, error) {
	// generate a new physical plan after cloning the logical plan so that any changes here do not update the logical plan
	cloned := lp.Clone()
	p := &physicalPlan{
		steps:    cloned.Steps,
		pipeline: cloned.Pipeline,
	}

	if err := p.createResultNode(); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *physicalPlan) createResultNode() error {
	leaf, err := p.leafNode()
	if err != nil {
		return err
	}

	resultNode := parser.NewTransformFromOperation(&ResultOp{}, len(p.steps)+1)
	resultStep := &LogicalStep{
		Transform: resultNode,
		Parents:   []parser.TransformID{leaf.ID()},
		Children:  []parser.TransformID{},
	}

	p.resultStep = resultStep
	return nil
}

func (p *physicalPlan) leafNode() (*LogicalStep, error) {
	var leaf *LogicalStep
	for _, transformID := range p.pipeline {
		node, ok := p.steps[transformID]
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

func (p *physicalPlan) String() string {
	return fmt.Sprintf("Steps: %s, Pipeline: %s, Result: %s", p.steps, p.pipeline, p.resultStep)
}
