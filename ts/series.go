package ts

import (
	"context"
	"time"
)

// A Series is the public interface to a block of timeseries values.  Each block has a start time,
// a logical number of steps, and a step size indicating the number of milliseconds represented by each point.
type Series struct {
	name      string
	startTime time.Time
	vals      []float64
	ctx       context.Context

	// The Specification is the path that was used to generate this timeseries,
	// typically either the query, or the function stack used to transform
	// specific results.
	Specification string

	// Metric tags.
	Tags map[string]string

	// options for graphing
	GraphOptions map[string]string
}

// NewSeries creates a new Series at a given start time, backed by the provided values
func NewSeries(ctx context.Context, name string, startTime time.Time, vals []float64) *Series {
	return &Series{
		name:          name,
		startTime:     startTime,
		vals:          vals,
		ctx:           ctx,
		Specification: name,
	}
}
