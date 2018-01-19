package storage

import (
	"context"
	"time"

	"github.com/m3db/m3coordinator/models"
	"github.com/m3db/m3coordinator/ts"

	xtime "github.com/m3db/m3x/time"
)

// Storage provides an interface for reading and writing to the tsdb
type Storage interface {
	Queryable
	Appender
}

// Queryable handles queries against a storage.
type Queryable interface {
	// FetchByQuery fetches timeseries data based on a query
	Fetch(
		ctx context.Context, tagMatchers []*models.Matcher, start time.Time, end time.Time) (*FetchResult, error)
}

// Appender provides batched appends against a storage.
type Appender interface {
	// Write value to the database for an ID
	Write(tags models.Tags, t time.Time, value float64, unit xtime.Unit, annotation []byte) error
}

// FetchResult provides a fetch result and meta information
type FetchResult struct {
	SeriesList []*ts.Series // The aggregated list of results across all underlying storage calls
	LocalOnly  bool
}
