package plan

import (
	"testing"

	"github.com/m3db/m3coordinator/functions"
	"github.com/m3db/m3coordinator/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/gcimporter15/testdata"
)

func TestResultNode(t *testing.T) {
	fetchTransform := parser.NewTransformFromOperation(&functions.FetchOp{}, 1)
	countTransform := parser.NewTransformFromOperation(&functions.CountOp{}, 2)
	transforms := parser.Transforms{fetchTransform, countTransform}
	edges := parser.Edges{
		&parser.Edge{
			ParentID: fetchTransform.ID(),
			ChildID:  countTransform.ID(),
		},
	}

	lp, err := NewLogicalPlan(transforms, edges)
	require.NoError(t, err)
	pl, err := NewPhysicalPlan(lp, nil)
	require.NoError(t, err)
	p := pl.(*physicalPlan)
	node, err := p.leafNode()
	require.NoError(t, err)
	assert.Equal(t, node.ID(), countTransform.ID())
	assert.Equal(t, p.resultStep.Transform.Op().OpType(), ResultType)
}
