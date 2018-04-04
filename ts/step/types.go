package step

import (
	"time"

	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3x/ident"
)

// SeriesBlock is a placeholder until it is added to M3DB
type SeriesBlock struct {
	Start          time.Time
	End            time.Time
	SeriesIterator encoding.SeriesIterator
}

// SeriesBlocks is a placeholder until it is added to M3DB
type SeriesBlocks struct {
	ID     ident.ID
	Blocks []SeriesBlock
}

// MultiSeriesBlock represents a vertically oriented block
type MultiSeriesBlock struct {
	Start           time.Time
	End             time.Time
	SeriesIterators encoding.SeriesIterators
}

// Iter iterates through an M3DBStepIterator vertically
type Iter interface {
	Next() bool
	Current() Data
}

// M3DBStepIterator implements the Iter interface
type M3DBStepIterator struct {
	MultiSeriesBlock
	firstNext bool
	closed    bool
}

// Data is the data per timestamp in a MultiSeriesBlock
type Data struct {
	timestamp time.Time
	values    []float64
}
