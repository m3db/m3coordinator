package plan

import (
	"testing"

	"github.com/m3db/m3coordinator/parser"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSingleChildParentRelation(t *testing.T) {
	fetchTransform := &parser.Transform{
		ID: parser.TransformID("1"),
	}

	sumTransform := &parser.Transform{
		ID: parser.TransformID("2"),
	}

	transforms := make(parser.Transforms, 0)
	transforms = append(transforms, fetchTransform)
	transforms = append(transforms, sumTransform)

	edges := make(parser.Edges, 0)
	edges = append(edges, &parser.Edge{
		ParentID: fetchTransform.ID,
		ChildID: sumTransform.ID,
	})

	lp, err := GenerateLogicalPlan(transforms, edges)
	require.NoError(t, err)
	assert.Equal(t, lp.Transforms[fetchTransform.ID].Children[0], sumTransform.ID)
	assert.Equal(t, lp.Transforms[sumTransform.ID].Parents[0], fetchTransform.ID)
	assert.Len(t, lp.Transforms[sumTransform.ID].Children, 0)

}
