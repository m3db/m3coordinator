package storage

import (
	"time"

	"github.com/m3db/m3coordinator/models"
	"github.com/m3db/m3coordinator/ts"
)

type Block interface {
	Meta() BlockMetadata
	StepIter() StepIter
	SeriesIter() SeriesIter
	SeriesMeta() []SeriesMeta
	StepMeta() []StepMeta
	Id() BlockId // Block number
}

type BlockId struct {
	Id int64 // Order of the block
}

type SeriesMeta struct {
	Tags models.Tags
}

type StepMeta struct {
}

type Bounds struct {
	start    time.Time
	end      time.Time
	stepSize time.Duration
}

// SeriesIter iterates through a CompressedSeriesIterator horizontally
type SeriesIter interface {
	Next() bool
	Current() ts.Series
}

// StepIter iterates through a CompressedStepIterator vertically
type StepIter interface {
	Next() bool
	Current() Step
}

// Step can optionally implement iterator interface
type Step interface {
	Time() time.Time
	Values() []float64
	Free()
}

type BlockMetadata struct {
	Bounds Bounds
	Tags   models.Tags // Common tags across different series
}
