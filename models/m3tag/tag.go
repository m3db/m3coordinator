package m3tag

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"

	"github.com/m3db/m3coordinator/generated/proto/m3coordinator"

	"github.com/m3db/m3coordinator/errors"
	"github.com/m3db/m3coordinator/generated/proto/prometheus/prompb"
	"github.com/m3db/m3coordinator/models"

	"github.com/m3db/m3x/ident"
)

type m3ID struct {
	id ident.ID
}

func (id *m3ID) String() string {
	return id.id.String()
}

// Assert types satisfy interfaces
var _ models.CoordinatorID = &m3ID{}

// M3Tags is a specific tags type that optimizes for m3db backend
type M3Tags struct {
	tags   ident.Tags
	once   sync.Once
	id     *m3ID
	idChan chan *m3ID
}

// ToFormat casts this tag to the requested format.
func (t *M3Tags) ToFormat(f models.TagFormat) (interface{}, error) {
	if f == models.FormatProm {
		tags := t.tags
		labels := make([]*prompb.Label, 0, len(tags))

		for _, tag := range tags {
			labels = append(labels, &prompb.Label{
				Name:  tag.Name.String(),
				Value: tag.Value.String(),
			})
		}
		return labels, nil
	} else if f == models.FormatRPC {
		tags := t.tags
		labels := make([]*rpc.Tag, 0, len(tags))
		for _, tag := range tags {
			labels = append(labels, &rpc.Tag{
				Name:  tag.Name.String(),
				Value: tag.Value.String(),
			})
		}
		return &rpc.Tags{Tags: labels}, nil
	}
	return nil, errors.ErrUnknownTagType
}

// ID is coordinator id
func (t *M3Tags) ID() models.CoordinatorID {
	t.once.Do(func() {
		t.id = t.computeID()
	})
	return t.id
}

// Assert types satisfy interfaces
var _ models.Tags = &M3Tags{}

// M3ID is a specific id type that optimizes for m3db backend
func (t *M3Tags) M3ID() ident.ID {
	t.once.Do(func() {
		t.id = t.computeID()
	})
	return t.id.id
}

// GetIterator returns the tag iterator
func (t *M3Tags) GetIterator() ident.TagIterator {
	return ident.NewTagSliceIterator(t.tags)
}

// TODO: change id computation to use this when merged: https://github.com/m3db/m3db/pull/479
func (t *M3Tags) beginComputation() {
	sep := byte(',')
	eq := byte('=')
	var buf bytes.Buffer

	for _, tag := range t.tags {
		buf.Write([]byte(tag.Name.String()))
		buf.WriteByte(eq)
		buf.Write([]byte(tag.Value.String()))
		buf.WriteByte(sep)
	}

	h := fnv.New32a()
	h.Write(buf.Bytes())
	id := fmt.Sprintf("%d", h.Sum32())
	t.idChan <- &m3ID{
		id: ident.StringID(id),
	}
}

func (t *M3Tags) computeID() *m3ID {
	return <-t.idChan
}

// TagIteratorToM3Tags wraps a TagIterator into an M3Tags
func TagIteratorToM3Tags(it ident.TagIterator) *M3Tags {
	t := make([]ident.Tag, 0, it.Remaining())
	for it.Next() {
		t = append(t, it.Current())
	}
	it.Close()
	tags := &M3Tags{
		tags:   t,
		idChan: make(chan *m3ID),
	}
	go func() {
		tags.beginComputation()
	}()
	return tags
}

// RPCToM3Tags converts rpc tag list to M3Tags
func RPCToM3Tags(rpcTags *rpc.Tags) *M3Tags {
	tags := rpcTags.GetTags()
	t := make([]ident.Tag, 0, len(tags))
	for _, tag := range tags {
		t = append(t, ident.StringTag(tag.GetName(), tag.GetValue()))
	}
	m3Tags := &M3Tags{
		tags:   t,
		idChan: make(chan *m3ID),
	}
	go func() {
		m3Tags.beginComputation()
	}()
	return m3Tags
}

// PromLabelsToM3Tags converts prometheus label list to M3Tags
func PromLabelsToM3Tags(labels []*prompb.Label) *M3Tags {
	t := make([]ident.Tag, 0, len(labels))
	sort.Sort(models.AscendingByKeyStringProm(labels))
	for _, label := range labels {
		t = append(t, ident.StringTag(label.Name, label.Value))
	}
	tags := &M3Tags{
		tags:   t,
		idChan: make(chan *m3ID),
	}
	go func() {
		tags.beginComputation()
	}()
	return tags
}

type ascendingByKeyMatcher models.Matchers

func (s ascendingByKeyMatcher) Len() int           { return len(s) }
func (s ascendingByKeyMatcher) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s ascendingByKeyMatcher) Less(i, j int) bool { return s[i].Name < s[j].Name }

// MatchersToM3Tags converts Matchers to Tags
// NB (braskin): this only works for exact matches
func MatchersToM3Tags(m models.Matchers) (*M3Tags, error) {
	t := make([]ident.Tag, 0, len(m))
	sort.Sort(ascendingByKeyMatcher(m))
	for _, matcher := range m {
		if matcher.Type != models.MatchEqual {
			return nil, fmt.Errorf("illegal match type, got %v, but expecting: %v", matcher.Type, models.MatchEqual)
		}
		t = append(t, ident.StringTag(matcher.Name, matcher.Value))
	}
	tags := &M3Tags{
		tags:   t,
		idChan: make(chan *m3ID),
	}
	go func() {
		tags.beginComputation()
	}()
	return tags, nil
}
