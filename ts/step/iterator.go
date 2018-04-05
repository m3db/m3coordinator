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
	"errors"
	"time"

	"github.com/m3db/m3db/encoding"
)

var (
	errBlocksMisaligned = errors.New("blocks are misaligned on either start or end times")
	errNumBlocks        = errors.New("number of blocks is not uniform across SeriesBlocks")
)

// SeriesBlockToMultiSeriesBlocks converts M3DB blocks to multi series blocks
func SeriesBlockToMultiSeriesBlocks(blocks []SeriesBlocks, seriesIteratorsPool encoding.MutableSeriesIteratorsPool) ([]MultiSeriesBlock, error) {
	numBlocks := len(blocks[0].Blocks)
	if err := validateBlocks(blocks, numBlocks); err != nil {
		return []MultiSeriesBlock{}, err
	}

	multiSeriesBlocks := make([]MultiSeriesBlock, 0, numBlocks)
	for i := 0; i < numBlocks; i++ {
		numSeries := len(blocks)
		var iters encoding.MutableSeriesIterators
		if seriesIteratorsPool != nil {
			iters = seriesIteratorsPool.Get(numSeries)
			iters.Reset(numSeries)
			for _, seriesIter := range iters.Iters() {
				iters.SetAt(i, seriesIter)
			}
		} else {
			s := make([]encoding.SeriesIterator, numSeries)

			for j, block := range blocks {
				s[j] = block.Blocks[i].SeriesIterator
			}
			iters = encoding.NewSeriesIterators(s, nil)
		}

		multiSeriesBlocks = append(multiSeriesBlocks,
			MultiSeriesBlock{
				Start:           blocks[0].Blocks[i].Start,
				End:             blocks[0].Blocks[i].End,
				SeriesIterators: iters,
			})
	}
	return multiSeriesBlocks, nil
}

func validateBlocks(blocks []SeriesBlocks, checkingLen int) error {
	if err := validateBlockSize(blocks, checkingLen); err != nil {
		return err
	}
	if err := validateBlockAlignment(blocks); err != nil {
		return err
	}
	return nil
}

func validateBlockSize(blocks []SeriesBlocks, checkingLen int) error {
	for _, block := range blocks {
		if len(block.Blocks) != checkingLen {
			return errNumBlocks
		}
	}
	return nil
}

func validateBlockAlignment(blocks []SeriesBlocks) error {
	start, end := getStartAndEndTimes(blocks[0])
	for _, seriesBlock := range blocks[1:] {
		for i, block := range seriesBlock.Blocks {
			if block.Start != start[i] || block.End != end[i] {
				return errBlocksMisaligned
			}
		}
	}
	return nil
}

func getStartAndEndTimes(block SeriesBlocks) ([]time.Time, []time.Time) {
	var start, end []time.Time
	for _, block := range block.Blocks {
		start = append(start, block.Start)
		end = append(end, block.End)
	}
	return start, end
}
