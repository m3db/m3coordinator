package executor

import (
	"testing"

	"github.com/m3db/m3coordinator/functions"
	"github.com/m3db/m3coordinator/parser"
	"github.com/m3db/m3coordinator/plan"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidState(t *testing.T) {
	fetchTransform := parser.NewTransformFromOperation(functions.FetchOp{}, 1)
	countTransform := parser.NewTransformFromOperation(functions.CountOp{}, 2)
	transforms := parser.Transforms{fetchTransform, countTransform}
	edges := parser.Edges{
		parser.Edge{
			ParentID: fetchTransform.ID,
			ChildID:  countTransform.ID,
		},
	}

	lp, err := plan.NewLogicalPlan(transforms, edges)
	require.NoError(t, err)
	p, err := plan.NewPhysicalPlan(lp, nil)
	require.NoError(t, err)
	state, err := GenerateExecutionState(p)
	assert.NoError(t, err)
	require.Len(t, state.sources, 1)
}

func TestWithoutSources(t *testing.T) {
	countTransform := parser.NewTransformFromOperation(functions.CountOp{}, 2)
	transforms := parser.Transforms{countTransform}
	edges := parser.Edges{}
	lp, err := plan.NewLogicalPlan(transforms, edges)
	require.NoError(t, err)
	p, err := plan.NewPhysicalPlan(lp, nil)
	require.NoError(t, err)
	_, err = GenerateExecutionState(p)
	assert.Error(t, err)
}


func TestOnlySources(t *testing.T) {
	fetchTransform := parser.NewTransformFromOperation(functions.FetchOp{}, 1)
	transforms := parser.Transforms{fetchTransform}
	edges := parser.Edges{}
	lp, err := plan.NewLogicalPlan(transforms, edges)
	require.NoError(t, err)
	p, err := plan.NewPhysicalPlan(lp, nil)
	require.NoError(t, err)
	state, err := GenerateExecutionState(p)
	assert.NoError(t, err)
	require.Len(t, state.sources, 1)
}
