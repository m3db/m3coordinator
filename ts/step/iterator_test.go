// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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

func newM3SeriesBlocksList(ctrl *gomock.Controller, now time.Time) []SeriesBlocks {
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
	seriesBlocks := newM3SeriesBlocksList(ctrl, now)
	m3CoordBlocks, err := SeriesBlockToMultiSeriesBlocks(seriesBlocks, nil)
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
