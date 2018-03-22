package models

import (
	"fmt"

	"github.com/m3db/m3coordinator/generated/proto/m3coordinator"
	"github.com/m3db/m3coordinator/generated/proto/prometheus/prompb"
)

// Tag is a generic representation of an internal tag, with a string key and value
type Tag struct {
	Key   string
	Value string
}

// Tags represents a set of metric tags
type Tags interface {
	ID() CoordinatorID
	Len() int
	ValueAt(i int) *Tag
}

// CoordinatorID wraps a way to get IDs out of internal types
type CoordinatorID interface {
	fmt.Stringer
}

// TagsToPromLabels converts a list of tags to prometheus labels
func TagsToPromLabels(t Tags) PrometheusLabels {
	labels := make(PrometheusLabels, 0, t.Len())

	for i := 0; i < t.Len(); i++ {
		tag := t.ValueAt(i)
		labels = append(labels, &prompb.Label{
			Name:  tag.Key,
			Value: tag.Value,
		})
	}

	return labels
}

// TagsToRPCTags converts a list of tags to prometheus labels
func TagsToRPCTags(t Tags) *rpc.Tags {
	tags := make([]*rpc.Tag, 0, t.Len())

	for i := 0; i < t.Len(); i++ {
		tag := t.ValueAt(i)
		tags = append(tags, &rpc.Tag{
			Name:  tag.Key,
			Value: tag.Value,
		})
	}

	return &rpc.Tags{
		Tags: tags,
	}
}

// PrometheusLabels is a list of prometheus labels
type PrometheusLabels []*prompb.Label

func (s PrometheusLabels) Len() int           { return len(s) }
func (s PrometheusLabels) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s PrometheusLabels) Less(i, j int) bool { return s[i].GetName() < s[j].GetName() }
