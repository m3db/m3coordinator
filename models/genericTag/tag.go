package genericTag

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"

	"github.com/m3db/m3coordinator/errors"
	"github.com/m3db/m3coordinator/generated/proto/prometheus/prompb"
	"github.com/m3db/m3coordinator/models"
)

type stringID string

func (s stringID) String() string {
	return string(s)
}

type genericTags struct {
	tags []*genericTag
	once sync.Once
	id   models.CoordinatorID
}

type genericTag struct {
	key   string
	value string
}

// ToFormat casts this tag to the requested format.
func (gt *genericTags) ToFormat(f models.TagFormat) (interface{}, error) {
	if f == models.FormatProm {
		tags := gt.tags
		labels := make([]*prompb.Label, 0, len(tags))

		for _, tag := range tags {
			labels = append(labels, &prompb.Label{
				Name:  tag.key,
				Value: tag.value,
			})
		}
		return labels, nil
	}
	return nil, errors.ErrUnknownTagType
}

var _ models.Tags = &genericTags{}

func (gt *genericTags) computeID() models.CoordinatorID {
	sep := byte(',')
	eq := byte('=')
	tags := gt.tags
	var buf bytes.Buffer
	for _, k := range tags {
		buf.Write([]byte(k.key))
		buf.WriteByte(eq)
		buf.Write([]byte(k.value))
		buf.WriteByte(sep)
	}

	h := fnv.New32a()
	h.Write(buf.Bytes())
	return stringID(fmt.Sprintf("%d", h.Sum32()))
}

func (gt *genericTags) ID() models.CoordinatorID {
	// Id is immutable, only compute once
	gt.once.Do(func() {
		gt.id = gt.computeID()
	})
	return gt.id
}

// NewGenericStringTags returns a new instance of Tags
func NewGenericStringTags(tags map[string]string) models.Tags {
	tagList := make([]*genericTag, 0)
	for k, v := range tags {
		tagList = append(tagList, &genericTag{
			key:   k,
			value: v,
		})
	}
	return &genericTags{
		tags: tagList,
	}
}

// PromLabelsToGenericTags converts prometheus label list to generic tags
func PromLabelsToGenericTags(labels []*prompb.Label) models.Tags {
	t := make([]*genericTag, len(labels))
	sort.Sort(models.AscendingByKeyStringProm(labels))
	for i, label := range labels {
		t[i] = &genericTag{
			key:   label.Name,
			value: label.Value,
		}
	}
	return &genericTags{
		tags: t,
	}
}
