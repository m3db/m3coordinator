package models

import (
	"time"

	xtime "github.com/m3db/m3x/time"
)

// ReadQuery represents the input query which is fetched from M3DB
type ReadQuery struct {
	TagMatchers Matchers
	Start       time.Time
	End         time.Time
}

// WriteQuery represents the input timeseries that is written to M3DB
type WriteQuery struct {
	Tags       *Tags
	Time       time.Time
	Value      float64
	Unit       xtime.Unit
	Annotation []byte
}
