package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/m3db/m3coordinator/models"
	"github.com/m3db/m3coordinator/ts"

	xcontext "github.com/m3db/m3x/context"
	xtime "github.com/m3db/m3x/time"
)

// Type describes the type of storage
type Type int

const (
	// TypeLocalDC is for storages that reside in the local datacenter
	TypeLocalDC Type = iota
	// TypeRemoteDC is for storages that reside in a remote datacenter
	TypeRemoteDC
	// TypeMultiDC is for storages that will aggregate multiple datacenters
	TypeMultiDC
)

// Storage provides an interface for reading and writing to the tsdb
type Storage interface {
	Querier
	Appender
	// Type identifies the type of the underlying storage
	Type() Type
}

// Query is an interface for a M3DB query
type Query interface {
	fmt.Stringer
	// nolint
	query()
}

func (q *FetchQuery) query() {}
func (q *WriteQuery) query() {}

// FetchQuery represents the input query which is fetched from M3DB
type FetchQuery struct {
	Raw         string
	TagMatchers models.Matchers `json:"matchers"`
	Start       time.Time       `json:"start"`
	End         time.Time       `json:"end"`
}

func (q *FetchQuery) String() string {
	return q.Raw
}

// FetchOptions represents the options for fetch query
type FetchOptions struct {
	Limit     int
	KillChan  chan struct{}
	M3Context xcontext.Context
}

// Querier handles queries against a storage.
type Querier interface {
	// Fetch fetches timeseries data based on a query
	Fetch(
		ctx context.Context, query *FetchQuery, options *FetchOptions) (*FetchResult, error)
	FetchTags(
		ctx context.Context, query *FetchQuery, options *FetchOptions) (*SearchResults, error)
}

// WriteQuery represents the input timeseries that is written to M3DB
type WriteQuery struct {
	Raw        string
	Tags       models.Tags
	Datapoints ts.Datapoints
	Unit       xtime.Unit
	Annotation []byte
}

func (q *WriteQuery) String() string {
	return q.Raw
}

// Appender provides batched appends against a storage.
type Appender interface {
	// Write value to the database for an ID
	Write(ctx context.Context, query *WriteQuery) error
}

// SearchResults is the result from a search
type SearchResults struct {
	Metrics models.Metrics
}

// FetchResult provides a fetch result and meta information
type FetchResult struct {
	SeriesList []*ts.Series // The aggregated list of results across all underlying storage calls
	LocalOnly  bool
	HasNext    bool
}

// QueryResult is the result from a query
type QueryResult struct {
	FetchResult *FetchResult
	Err         error
}
