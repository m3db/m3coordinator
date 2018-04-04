package step

import (
	"errors"

	"github.com/m3db/m3db/encoding"
)

// SeriesBlockToMultiSeriesBlocks converts M3DB blocks to multi series blocks
func SeriesBlockToMultiSeriesBlocks(blocks []SeriesBlocks) ([]MultiSeriesBlock, error) {
	blocksLen := len(blocks[0].Blocks)
	if !validateBlockSize(blocks, blocksLen) {
		return []MultiSeriesBlock{}, errors.New("validation failed. number of blocks is not uniform across SeriesBlocks")
	}

	multiSeriesBlocks := make([]MultiSeriesBlock, 0, blocksLen)
	for i := 0; i < blocksLen; i++ {
		s := make([]encoding.SeriesIterator, 0, len(blocks))
		for _, block := range blocks {
			s = append(s, block.Blocks[i].SeriesIterator)
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

func validateBlockSize(blocks []SeriesBlocks, checkingLen int) bool {
	for _, block := range blocks {
		if len(block.Blocks) != checkingLen {
			return false
		}
	}
	return true
}
