package models

import (
	"bytes"
	"errors"
	"fmt"
	"hash/fnv"
	"regexp"
	"sort"
	"sync"

	"github.com/m3db/m3coordinator/generated/proto/prometheus/prompb"

	"github.com/m3db/m3x/ident"
)

// Tags represents a set of metric tags
type Tags interface {
	ID() CoordinatorID
	ToPromLabels() []*prompb.Label
}

// CoordinatorID wraps a way to get IDs out of internal types
type CoordinatorID interface {
	fmt.Stringer
}

// Metric is the individual metric that gets returned from the search endpoint
type Metric struct {
	Namespace string
	ID        string
	Tags      Tags
}

// Metrics is a list of individual metrics
type Metrics []*Metric

// MatchType is an enum for label matching types.
type MatchType int

// Possible MatchTypes.
const (
	MatchEqual MatchType = iota
	MatchNotEqual
	MatchRegexp
	MatchNotRegexp
)

var (
	matchSymbols = [MatchNotRegexp + 1]string{"=", "!=", "=~", "!~"}

	// ErrInvalidMatcher is returned when an invalid matcher is constructed.
	ErrInvalidMatcher = errors.New("invalid matcher type")
)

func (m MatchType) String() string {
	return matchSymbols[m]
}

// Matcher models the matching of a label.
type Matcher struct {
	Type  MatchType `json:"type"`
	Name  string    `json:"name"`
	Value string    `json:"value"`

	re *regexp.Regexp
}

// NewMatcher returns a matcher object.
func NewMatcher(t MatchType, n, v string) (*Matcher, error) {
	if t > MatchNotRegexp || t < MatchEqual {
		return nil, ErrInvalidMatcher
	}
	m := &Matcher{
		Type:  t,
		Name:  n,
		Value: v,
	}
	if t == MatchRegexp || t == MatchNotRegexp {
		re, err := regexp.Compile("^(?:" + v + ")$")
		if err != nil {
			return nil, err
		}
		m.re = re
	}
	return m, nil
}

func (m *Matcher) String() string {
	return fmt.Sprintf("%s%s%q", m.Name, m.Type, m.Value)
}

// Matches returns whether the matcher matches the given string value.
func (m *Matcher) Matches(s string) bool {
	switch m.Type {
	case MatchEqual:
		return s == m.Value
	case MatchNotEqual:
		return s != m.Value
	case MatchRegexp:
		return m.re.MatchString(s)
	case MatchNotRegexp:
		return !m.re.MatchString(s)
	}
	panic("labels.Matcher.Matches: invalid match type")
}

// Matchers is of matchers
type Matchers []*Matcher

// ToTags converts Matchers to Tags
// NB (braskin): this only works for exact matches
func (m Matchers) ToTags() (Tags, error) {
	t := make([]*genericTag, len(m))
	for i, v := range m {
		if v.Type != MatchEqual {
			return nil, fmt.Errorf("illegal match type, got %v, but expecting: %v", v.Type, MatchEqual)
		}
		t[i] = &genericTag{
			key:   v.Name,
			value: v.Value,
		}
	}
	tags := &genericTags{
		tags: t,
	}

	return tags, nil
}

type stringID string

func (s stringID) String() string {
	return string(s)
}

type stringTags struct {
	tags string
}

var _ Tags = &stringTags{}

func (st *stringTags) ID() CoordinatorID {
	return stringID(st.tags)
}

func (st *stringTags) ToPromLabels() []*prompb.Label {
	// Should never be called
	panic("should not convert string tags to prom labels")
}

// NewStringTags returns a new insteance of Tags
func NewStringTags(tags string) Tags {
	return &stringTags{
		tags: tags,
	}
}

type genericTags struct {
	tags []*genericTag
	once sync.Once
	id   CoordinatorID
}

type genericTag struct {
	key   string
	value string
}

var _ Tags = &genericTags{}

type ascendingByKeyStringProm []*prompb.Label

func (s ascendingByKeyStringProm) Len() int           { return len(s) }
func (s ascendingByKeyStringProm) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s ascendingByKeyStringProm) Less(i, j int) bool { return s[i].GetName() < s[j].GetName() }

// TagsToPromLabels converts tags to prometheus labels
func (gt *genericTags) ToPromLabels() []*prompb.Label {
	labels := make([]*prompb.Label, 0, len(gt.tags))
	for _, tag := range gt.tags {
		labels = append(labels, &prompb.Label{
			Name:  tag.key,
			Value: tag.value,
		})
	}
	return labels
}

func (gt *genericTags) computeID() CoordinatorID {
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

func (gt *genericTags) ID() CoordinatorID {
	// Id is immutable, only compute once
	gt.once.Do(func() {
		gt.id = gt.computeID()
	})
	return gt.id
}

// NewGenericStringTags returns a new instance of Tags
func NewGenericStringTags(tags map[string]string) Tags {
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
func PromLabelsToGenericTags(labels []*prompb.Label) Tags {
	t := make([]*genericTag, len(labels))
	sort.Sort(ascendingByKeyStringProm(labels))
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

type m3ID struct {
	id ident.ID
}

func (id *m3ID) String() string {
	return id.id.String()
}

// M3Tags is a specific tags type that optimizes for m3db backend
type M3Tags struct {
	tags ident.Tags
	//ident.TagIterator
	once   sync.Once
	id     *m3ID
	idChan chan *m3ID
}

var _ CoordinatorID = &m3ID{}
var _ Tags = &M3Tags{}

// ID is coord id
func (t *M3Tags) ID() CoordinatorID {
	t.once.Do(func() {
		t.id = t.computeID()
	})
	return t.id
}

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

// ToPromLabels converts M3Tags to prometheus labels
func (t *M3Tags) ToPromLabels() []*prompb.Label {
	tags := t.tags
	// Safe to close original iterator once this is called, since beginComputation already has a valid duplicate
	labels := make([]*prompb.Label, 0, len(tags))

	for _, tag := range tags {
		labels = append(labels, &prompb.Label{
			Name:  tag.Name.String(),
			Value: tag.Value.String(),
		})
	}
	return labels
}

func (t *M3Tags) computeID() *m3ID {
	return <-t.idChan
}

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

// PromLabelsToM3Tags converts prometheus label list to M3Tags
func PromLabelsToM3Tags(labels []*prompb.Label) *M3Tags {
	t := make([]ident.Tag, 0, len(labels))
	sort.Sort(ascendingByKeyStringProm(labels))
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
