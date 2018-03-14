package functions

import (
	"time"

	"github.com/m3db/m3coordinator/models"
)

// FetchType gets the series from storage
const FetchType = "fetch"

// FetchOp stores required properties for fetch
type FetchOp struct {
	Name     string
	Range    time.Duration
	Offset   time.Duration
	Matchers models.Matchers
}

// Type for the operator
func (o *FetchOp) Type() string {
	return FetchType
}
