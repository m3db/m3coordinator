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

	"github.com/m3db/m3db/encoding"
)

var (
	errBlocksMisaligned   = errors.New("blocks are misaligned on either start or end times")
	errMultipleNamespaces = errors.New("consolidating multiple namespaces is currently not supported")
)

// SeriesBlockToMultiSeriesBlocks converts M3DB blocks to multi series blocks
func SeriesBlockToMultiSeriesBlocks(multiNamespaceSeriesList []MultiNamespaceSeries, seriesIteratorsPool encoding.MutableSeriesIteratorsPool) (MultiSeriesBlocks, error) {
	// todo(braskin): validate blocks size and aligment per namespace before creating []MultiNamespaceSeries
	var multiSeriesBlocks MultiSeriesBlocks
	for multiNamespaceSeriesIdx, multiNamespaceSeries := range multiNamespaceSeriesList {
		consolidatedSeriesBlocks, err := newConsolidatedSeriesBlocks(multiNamespaceSeries, seriesIteratorsPool)
		if err != nil {
			return MultiSeriesBlocks{}, err
		}

		if multiNamespaceSeriesIdx == 0 {
			multiSeriesBlocks = make(MultiSeriesBlocks, len(consolidatedSeriesBlocks))
		}
		for consolidatedSeriesBlockIdx, consolidatedSeriesBlock := range consolidatedSeriesBlocks {
			if multiNamespaceSeriesIdx == 0 {
				multiSeriesBlocks[consolidatedSeriesBlockIdx].Start = consolidatedSeriesBlock.Start
				multiSeriesBlocks[consolidatedSeriesBlockIdx].End = consolidatedSeriesBlock.End
			}

			if consolidatedSeriesBlock.Start != multiSeriesBlocks[consolidatedSeriesBlockIdx].Start || consolidatedSeriesBlock.End != multiSeriesBlocks[consolidatedSeriesBlockIdx].End {
				return MultiSeriesBlocks{}, err
			}
			multiSeriesBlocks[consolidatedSeriesBlockIdx].Blocks = append(multiSeriesBlocks[consolidatedSeriesBlockIdx].Blocks, consolidatedSeriesBlock)
		}
	}

	return multiSeriesBlocks, nil
}

// newConsolidatedSeriesBlocks creates consolidated blocks by timeseries across namespaces
func newConsolidatedSeriesBlocks(multiNamespaceSeries MultiNamespaceSeries, seriesIteratorsPool encoding.MutableSeriesIteratorsPool) (ConsolidatedSeriesBlocks, error) {
	var consolidatedSeriesBlocks ConsolidatedSeriesBlocks

	// todo: remove this once we support consolidating multiple namespaces
	if len(multiNamespaceSeries) > 1 {
		return consolidatedSeriesBlocks, errMultipleNamespaces
	}

	for seriesBlocksIdx, seriesBlocks := range multiNamespaceSeries {
		consolidatedNSBlocks := newConsolidatedNSBlocks(seriesBlocks, seriesIteratorsPool)
		if seriesBlocksIdx == 0 {
			consolidatedSeriesBlocks = make(ConsolidatedSeriesBlocks, len(consolidatedNSBlocks))
		}

		for consolidatedNSBlockIdx, consolidatedNSBlock := range consolidatedNSBlocks {
			if seriesBlocksIdx == 0 {
				consolidatedSeriesBlocks[consolidatedNSBlockIdx].Start = consolidatedNSBlock.Start
				consolidatedSeriesBlocks[consolidatedNSBlockIdx].End = consolidatedNSBlock.End
			}

			if consolidatedNSBlock.Start != consolidatedSeriesBlocks[consolidatedNSBlockIdx].Start || consolidatedNSBlock.End != consolidatedSeriesBlocks[consolidatedNSBlockIdx].End {
				return ConsolidatedSeriesBlocks{}, errBlocksMisaligned
			}
			consolidatedSeriesBlocks[consolidatedNSBlockIdx].ConsolidatedNSBlocks = append(consolidatedSeriesBlocks[consolidatedNSBlockIdx].ConsolidatedNSBlocks, consolidatedNSBlock)
		}
	}

	return consolidatedSeriesBlocks, nil
}

// newConsolidatedNSBlocks creates a slice of consolidated blocks per namespace for a single timeseries
func newConsolidatedNSBlocks(seriesBlocks SeriesBlocks, seriesIteratorsPool encoding.MutableSeriesIteratorsPool) []ConsolidatedNSBlock {
	var consolidatedNSBlocks []ConsolidatedNSBlock
	namespace := seriesBlocks.Namespace
	id := seriesBlocks.ID
	for _, seriesBlock := range seriesBlocks.Blocks {
		consolidatedNSBlock := ConsolidatedNSBlock{
			Namespace: namespace,
			ID:        id,
			Start:     seriesBlock.Start,
			End:       seriesBlock.End,
		}
		s := []encoding.SeriesIterator{seriesBlock.SeriesIterator}
		// todo(braskin): figure out how many series iterators we need based on largest step size (i.e. namespace)
		// and in future copy SeriesIterators using the seriesIteratorsPool
		consolidatedNSBlock.SeriesIterators = encoding.NewSeriesIterators(s, nil)
		consolidatedNSBlocks = append(consolidatedNSBlocks, consolidatedNSBlock)
	}

	return consolidatedNSBlocks
}
