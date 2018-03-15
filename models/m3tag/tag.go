package m3tag

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"

	"github.com/m3db/m3coordinator/generated/proto/m3coordinator"
	"github.com/m3db/m3coordinator/models"

	"github.com/m3db/m3x/ident"
)

type stringID struct {
	id string
}

func (i *stringID) String() string {
	return i.id
}

// Assert types satisfy interfaces
var _ models.CoordinatorID = &stringID{}

// M3Tags is a specific tags type that optimizes for m3db backend
type M3Tags struct {
	tags ident.Tags
	once sync.Once
	id   *stringID
}

// ID is coordinator id
func (t *M3Tags) ID() models.CoordinatorID {
	return t.computeID()
}

// Len returns the count of tags
func (t *M3Tags) Len() int {
	return len(t.tags)
}

// ValueAt returns the generic tag representation at position i
func (t *M3Tags) ValueAt(i int) *models.Tag {
	tag := t.tags[i]
	return &models.Tag{
		Key:   tag.Name.String(),
		Value: tag.Value.String(),
	}
}

// Assert types satisfy interfaces
var _ models.Tags = &M3Tags{}

// TagIterator returns a tag iterator for these tags
func (t *M3Tags) TagIterator() ident.TagIterator {
	return ident.NewTagSliceIterator(t.tags)
}

// Finalize releases the internal ident.Tags to their object pool
func (t *M3Tags) Finalize() {
	for _, tag := range t.tags {
		tag.Finalize()
	}
}

// TODO (arnikola): change id computation to use this when merged: https://github.com/m3db/m3db/pull/479
const (
	sep = byte(',')
	eq  = byte('=')
)

func (t *M3Tags) computeID() *stringID {
	t.once.Do(func() {
		var buf bytes.Buffer

		for _, tag := range t.tags {
			buf.Write([]byte(tag.Name.String()))
			buf.WriteByte(eq)
			buf.Write([]byte(tag.Value.String()))
			buf.WriteByte(sep)
		}

		h := fnv.New32a()
		h.Write(buf.Bytes())
		t.id = &stringID{
			id: fmt.Sprintf("%d", h.Sum32()),
		}
	})
	return t.id
}

// TagIteratorToM3Tags wraps a TagIterator into an M3Tags
func TagIteratorToM3Tags(it ident.TagIterator) *M3Tags {
	defer it.Close()
	t := make([]ident.Tag, 0, it.Remaining())

	for it.Next() {
		t = append(t, it.Current())
	}

	return createM3Tags(t)
}

// RPCToM3Tags converts rpc tag list to M3Tags
// nb (arnikola): tags received over RPC are assumed to be pre-sorted
func RPCToM3Tags(rpcTags *rpc.Tags) *M3Tags {
	tags := rpcTags.GetTags()
	t := make([]ident.Tag, 0, len(tags))

	for _, tag := range tags {
		t = append(t, ident.StringTag(tag.GetName(), tag.GetValue()))
	}

	return createM3Tags(t)
}

// PromLabelsToM3Tags converts prometheus label list to M3Tags
func PromLabelsToM3Tags(labels models.PrometheusLabels) *M3Tags {
	t := make([]ident.Tag, 0, len(labels))
	sort.Sort(labels)

	for _, label := range labels {
		t = append(t, ident.StringTag(label.Name, label.Value))
	}

	return createM3Tags(t)
}

// MatchersToM3Tags converts Matchers to Tags
// NB (braskin): this only works for exact matches
func MatchersToM3Tags(m models.Matchers) (*M3Tags, error) {
	t := make([]ident.Tag, 0, len(m))
	sort.Sort(m)

	for _, matcher := range m {
		if matcher.Type != models.MatchEqual {
			return nil, fmt.Errorf("illegal match type, got %v, but expecting: %v", matcher.Type, models.MatchEqual)
		}
		t = append(t, ident.StringTag(matcher.Name, matcher.Value))
	}

	return createM3Tags(t), nil
}

func createM3Tags(t []ident.Tag) *M3Tags {
	return &M3Tags{
		tags: t,
	}
}
