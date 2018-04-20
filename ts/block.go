package ts

import (
	"time"

	"github.com/m3db/m3coordinator/models"
)

type Block interface {
	Bounds() Bounds
	Tags() models.Tags // Common tags across different series
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
	Start    time.Time
	End      time.Time
	StepSize time.Duration
}

// StepIter iterates through a step
type StepIter interface {
	Next() bool
	Current() Step
}

// Step can optionally implement iterator interface
type Step interface {
	Time() time.Time
	Values() []float64
}

// SeriesIter iterates through a single series
type SeriesIter interface {
	Next() bool
	Current() Series
}
