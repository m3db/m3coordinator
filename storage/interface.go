package storage

import (
	"context"

	"github.com/m3db/m3coordinator/models"
	"github.com/m3db/m3coordinator/ts"
)

// Storage provides an interface for reading and writing to the tsdb
type Storage interface {
	Querier
	Appender
}

// Querier handles queries against a storage.
type Querier interface {
	// Fetch fetches timeseries data based on a query
	Fetch(
		ctx context.Context, query *models.ReadQuery) (*FetchResult, error)
}

// Appender provides batched appends against a storage.
type Appender interface {
	// Write value to the database for an ID
	Write(query *models.WriteQuery) error
}

// FetchResult provides a fetch result and meta information
type FetchResult struct {
	SeriesList []*ts.Series // The aggregated list of results across all underlying storage calls
	LocalOnly  bool
}
