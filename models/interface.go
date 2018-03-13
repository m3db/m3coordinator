package models

import (
	"fmt"

	"github.com/m3db/m3coordinator/generated/proto/prometheus/prompb"
)

// TagFormat represents a tag scheme, used for conversions
type TagFormat int

const (
	// FormatProm will convert to []*prompb.Label
	FormatProm TagFormat = iota
	// FormatRPC will convert to []*rpc.Tags
	FormatRPC
)

// Tags represents a set of metric tags
type Tags interface {
	ID() CoordinatorID
	ToFormat(f TagFormat) (interface{}, error)
}

// CoordinatorID wraps a way to get IDs out of internal types
type CoordinatorID interface {
	fmt.Stringer
}

// AscendingByKeyStringProm is a sorter that sorts prom labels by their key
type AscendingByKeyStringProm []*prompb.Label

func (s AscendingByKeyStringProm) Len() int           { return len(s) }
func (s AscendingByKeyStringProm) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s AscendingByKeyStringProm) Less(i, j int) bool { return s[i].GetName() < s[j].GetName() }
