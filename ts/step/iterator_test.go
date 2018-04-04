package step

import (
	"testing"
	"time"

	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3x/ident"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	blockOneID = "test_one"
	blockTwoID = "test_two"
)

func createM3SeriesBlocksList(ctrl *gomock.Controller, now time.Time) []SeriesBlocks {
	seriesBlocksOne := newM3SeriesBlock(blockOneID, ctrl, now)
	blocksOne := SeriesBlocks{
		Blocks: seriesBlocksOne,
		ID:     ident.StringID(blockOneID),
	}

	seriesBlocksTwo := newM3SeriesBlock(blockTwoID, ctrl, now)
	blocksTwo := SeriesBlocks{
		Blocks: seriesBlocksTwo,
		ID:     ident.StringID(blockTwoID),
	}

	seriesBlocks := []SeriesBlocks{blocksOne, blocksTwo}
	return seriesBlocks
}

func newM3SeriesBlock(id string, ctrl *gomock.Controller, now time.Time) []SeriesBlock {
	seriesIterOne := encoding.NewMockSeriesIterator(ctrl)
	seriesIterTwo := encoding.NewMockSeriesIterator(ctrl)

	seriesIterOne.EXPECT().ID().Return(ident.StringID(id))
	seriesIterTwo.EXPECT().ID().Return(ident.StringID(id))

	sOne := SeriesBlock{
		Start:          now,
		End:            now.Add(10 * time.Minute),
		SeriesIterator: seriesIterOne,
	}
	sTwo := SeriesBlock{
		Start:          now.Add(10 * time.Minute),
		End:            now.Add(20 * time.Minute),
		SeriesIterator: seriesIterTwo,
	}

	return []SeriesBlock{sOne, sTwo}
}

func TestConvertM3Blocks(t *testing.T) {
	now := time.Now()
	ctrl := gomock.NewController(t)
	seriesBlocks := createM3SeriesBlocksList(ctrl, now)
	m3CoordBlocks, err := SeriesBlockToMultiSeriesBlocks(seriesBlocks)
	require.NoError(t, err)

	assert.Equal(t, blockOneID, m3CoordBlocks[0].SeriesIterators.Iters()[0].ID().String())
	assert.Equal(t, blockTwoID, m3CoordBlocks[0].SeriesIterators.Iters()[1].ID().String())
	assert.Equal(t, now, m3CoordBlocks[0].Start)
	assert.Equal(t, now.Add(10*time.Minute), m3CoordBlocks[0].End)

	assert.Equal(t, blockOneID, m3CoordBlocks[1].SeriesIterators.Iters()[0].ID().String())
	assert.Equal(t, blockTwoID, m3CoordBlocks[1].SeriesIterators.Iters()[1].ID().String())
	assert.Equal(t, now.Add(10*time.Minute), m3CoordBlocks[1].Start)
	assert.Equal(t, now.Add(20*time.Minute), m3CoordBlocks[1].End)
}
