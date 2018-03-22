package tsdb

import (
	"time"

	"github.com/m3db/m3coordinator/models"

	"github.com/m3db/m3metrics/policy"
	xtime "github.com/m3db/m3x/time"
)

// FetchRange is a fetch range.
type FetchRange struct {
	xtime.Range
	policy.StoragePolicy
}

// Equal returns whether two FetchRanges are equal.
func (r FetchRange) Equal(other FetchRange) bool {
	return r.Range.Equal(other.Range) && r.StoragePolicy == other.StoragePolicy
}

// FetchRanges is a list of fetch ranges.
type FetchRanges []*FetchRange

// FetchRequest is a request to fetch data from a source for a given id.
type FetchRequest struct {
	ID     models.CoordinatorID
	Ranges FetchRanges
}

// NewSingleRangeRequest creates a new single-range request.
func NewSingleRangeRequest(start, end time.Time, p policy.StoragePolicy) FetchRanges {
	rng := xtime.Range{Start: start, End: end}
	return []*FetchRange{
		&FetchRange{
			Range:         rng,
			StoragePolicy: p,
		},
	}
}
