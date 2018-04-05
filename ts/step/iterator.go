package step

import (
	"errors"
	"time"

	"github.com/m3db/m3db/encoding"
)

var (
	errBlocksMisaligned = errors.New("validation failed. blocks are misaligned on either start or end times")
	errNumBlocks        = errors.New("validation failed. number of blocks is not uniform across SeriesBlocks")
)

// SeriesBlockToMultiSeriesBlocks converts M3DB blocks to multi series blocks
func SeriesBlockToMultiSeriesBlocks(blocks []SeriesBlocks) ([]MultiSeriesBlock, error) {
	numBlocks := len(blocks[0].Blocks)
	if err := validateBlocks(blocks, numBlocks); err != nil {
		return []MultiSeriesBlock{}, err
	}

	multiSeriesBlocks := make([]MultiSeriesBlock, 0, numBlocks)
	for i := 0; i < numBlocks; i++ {
		numSeries := len(blocks)
		s := make([]encoding.SeriesIterator, numSeries)

		for j, block := range blocks {
			s[j] = block.Blocks[i].SeriesIterator
		}

		multiSeriesBlocks = append(multiSeriesBlocks,
			MultiSeriesBlock{
				Start:           blocks[0].Blocks[i].Start,
				End:             blocks[0].Blocks[i].End,
				SeriesIterators: encoding.NewSeriesIterators(s, nil),
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
