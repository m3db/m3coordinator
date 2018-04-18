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

package iter

import (
	"errors"
	"time"

	"github.com/m3db/m3db/encoding"
)

var (
	errBlocksMisaligned   = errors.New("blocks are misaligned on either start or end times")
	errNumBlocks          = errors.New("number of blocks is not uniform across SeriesBlocks")
	errMultipleNamespaces = errors.New("consolidating multiple namespaces is currently not supported")
)

// SeriesBlockToMultiSeriesBlocks converts M3DB blocks to multi series blocks
func SeriesBlockToMultiSeriesBlocks(multiNamespaceSeriesList []MultiNamespaceSeries, seriesIteratorsPool encoding.MutableSeriesIteratorsPool) (MultiSeriesBlocks, error) {
	for _, multiNamespaceSeries := range multiNamespaceSeriesList {
		numBlocks := len(multiNamespaceSeries[0].Blocks)
		if err := validateBlocks(multiNamespaceSeries, numBlocks); err != nil {
			return MultiSeriesBlocks{}, err
		}
	}

	var multiSeriesBlocks MultiSeriesBlocks
	for i, multiNamespaceSeries := range multiNamespaceSeriesList {
		multiNSConsolidatedSeriesBlocks, err := newMultiNSConsolidatedSeriesBlocks(multiNamespaceSeries, seriesIteratorsPool)
		if err != nil {
			return MultiSeriesBlocks{}, err
		}
		if i == 0 {
			multiSeriesBlocks = make(MultiSeriesBlocks, len(multiNSConsolidatedSeriesBlocks))
		}
		for j, multiNSConsolidatedSeriesBlock := range multiNSConsolidatedSeriesBlocks {
			if i == 0 {
				multiSeriesBlocks[j].Start = multiNSConsolidatedSeriesBlock.Start
				multiSeriesBlocks[j].End = multiNSConsolidatedSeriesBlock.End
			}
			if multiNSConsolidatedSeriesBlock.Start != multiSeriesBlocks[j].Start || multiNSConsolidatedSeriesBlock.End != multiSeriesBlocks[j].End {
				return MultiSeriesBlocks{}, err
			}
			multiSeriesBlocks[j].Blocks = append(multiSeriesBlocks[j].Blocks, multiNSConsolidatedSeriesBlock)
		}
	}
	return multiSeriesBlocks, nil
}

func newMultiNSConsolidatedSeriesBlocks(multiNamespaceSeries MultiNamespaceSeries, seriesIteratorsPool encoding.MutableSeriesIteratorsPool) (MultiNSConsolidatedSeriesBlocks, error) {
	var multiNSConsolidatedSeriesBlocks MultiNSConsolidatedSeriesBlocks

	// todo(braskin): remove this once we support consolidating multiple namespaces
	if len(multiNamespaceSeries) > 1 {
		return multiNSConsolidatedSeriesBlocks, errMultipleNamespaces
	}

	for i, seriesBlocks := range multiNamespaceSeries {
		sliceOfConsolidatedSeriesBlocks := newConsolidatedSeriesBlocks(seriesBlocks, seriesIteratorsPool)
		if i == 0 {
			multiNSConsolidatedSeriesBlocks = make([]MultiNSConsolidatedSeriesBlock, len(sliceOfConsolidatedSeriesBlocks))
		}
		for j, consolidatedStepBlock := range sliceOfConsolidatedSeriesBlocks {
			if i == 0 {
				multiNSConsolidatedSeriesBlocks[j].Start = consolidatedStepBlock.Start
				multiNSConsolidatedSeriesBlocks[j].End = consolidatedStepBlock.End
			}
			if consolidatedStepBlock.Start != multiNSConsolidatedSeriesBlocks[j].Start || consolidatedStepBlock.End != multiNSConsolidatedSeriesBlocks[j].End {
				return MultiNSConsolidatedSeriesBlocks{}, errBlocksMisaligned
			}
			multiNSConsolidatedSeriesBlocks[j].ConsolidatedNSBlocks = append(multiNSConsolidatedSeriesBlocks[j].ConsolidatedNSBlocks, consolidatedStepBlock)
		}
	}
	return multiNSConsolidatedSeriesBlocks, nil
}

func newConsolidatedSeriesBlocks(seriesBlocks SeriesBlocks, seriesIteratorsPool encoding.MutableSeriesIteratorsPool) []ConsolidatedSeriesBlock {
	var consolidatedSeriesBlocks []ConsolidatedSeriesBlock
	namespace := seriesBlocks.Namespace
	id := seriesBlocks.ID
	for _, seriesBlock := range seriesBlocks.Blocks {
		consolidatedSeriesBlock := ConsolidatedSeriesBlock{
			Namespace: namespace,
			ID:        id,
			Start:     seriesBlock.Start,
			End:       seriesBlock.End,
		}
		s := []encoding.SeriesIterator{seriesBlock.SeriesIterator}
		// todo(braskin): figure out how many series iterators we need based on largest step size (i.e. namespace)
		// and in future copy SeriesIterators using the seriesIteratorsPool
		consolidatedSeriesBlock.SeriesIterators = encoding.NewSeriesIterators(s, nil)
		consolidatedSeriesBlocks = append(consolidatedSeriesBlocks, consolidatedSeriesBlock)
	}
	return consolidatedSeriesBlocks
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
