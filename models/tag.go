package models

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"regexp"
	"sort"

	"github.com/m3db/m3coordinator/generated/proto/prometheus/prompb"
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

// MatchType is an enum for label matching types.
type MatchType int

// Possible MatchTypes.
const (
	MatchEqual MatchType = iota
	MatchNotEqual
	MatchRegexp
	MatchNotRegexp
)

func (m MatchType) String() string {
	typeToStr := map[MatchType]string{
		MatchEqual:     "=",
		MatchNotEqual:  "!=",
		MatchRegexp:    "=~",
		MatchNotRegexp: "!~",
	}
	if str, ok := typeToStr[m]; ok {
		return str
	}
	panic("unknown match type")
}

// Matcher models the matching of a label.
type Matcher struct {
	Type  MatchType
	Name  string
	Value string

	re *regexp.Regexp
}

// NewMatcher returns a matcher object.
func NewMatcher(t MatchType, n, v string) (*Matcher, error) {
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
	t := make(tags, len(m))
	for _, v := range m {
		if v.Type != MatchEqual {
			return nil, fmt.Errorf("illegal match type, got %v, but expecting: %v", v.Type, MatchEqual)
		}
		t[v.Name] = v.Value
	}

	return t, nil
}

type tags map[string]string

var _ Tags = make(tags)

type stringID string

func (s stringID) String() string {
	return string(s)
}

// TagsToPromLabels converts tags to prometheus labels
func (t tags) ToPromLabels() []*prompb.Label {
	labels := make([]*prompb.Label, 0, len(t))
	for k, v := range t {
		labels = append(labels, &prompb.Label{Name: k, Value: v})
	}
	return labels
}

// ID returns a string representation of the tags
func (t tags) ID() CoordinatorID {
	sep := byte(',')
	eq := byte('=')

	var keys []string
	for k := range t {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var buf bytes.Buffer
	for _, k := range keys {
		byteKey := []byte(k)
		buf.Write(byteKey)
		buf.WriteByte(eq)
		buf.Write([]byte(t[k]))
		buf.WriteByte(sep)
	}

	h := fnv.New32a()
	h.Write(buf.Bytes())
	return stringID(fmt.Sprintf("%d", h.Sum32()))
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
}

var _ Tags = &genericTags{}

type genericTag struct {
	key   []byte
	value []byte
}

type ascendingByKey []*genericTag

func (s ascendingByKey) Len() int           { return len(s) }
func (s ascendingByKey) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s ascendingByKey) Less(i, j int) bool { return bytes.Compare(s[i].key, s[j].key) == -1 }

func (gt *genericTags) ID() CoordinatorID {
	sep := byte(',')
	eq := byte('=')
	tags := gt.tags
	sort.Sort(ascendingByKey(tags))
	var buf bytes.Buffer
	for _, k := range tags {
		buf.Write(k.key)
		buf.WriteByte(eq)
		buf.Write(k.value)
		buf.WriteByte(sep)
	}

	h := fnv.New32a()
	h.Write(buf.Bytes())
	return stringID(fmt.Sprintf("%d", h.Sum32()))
}

// PromLabelsToM3Tags does stuff
func PromLabelsToM3Tags(labels []*prompb.Label) Tags {
	t := make([]*genericTag, len(labels))
	for i, label := range labels {
		t[i] = &genericTag{
			key:   []byte(label.Name),
			value: []byte(label.Value),
		}
	}
	return &genericTags{
		tags: t,
	}
}

// TagsToPromLabels converts tags to prometheus labels
func (gt *genericTags) ToPromLabels() []*prompb.Label {
	labels := make([]*prompb.Label, 0, len(gt.tags))
	for _, tag := range gt.tags {
		labels = append(labels, &prompb.Label{
			Name:  string(tag.key),
			Value: string(tag.value),
		})
	}
	return labels
}

type genericStringTags struct {
	tags []*genericStringTag
}

type genericStringTag struct {
	key   string
	value string
}

var _ Tags = &genericStringTags{}

type ascendingByKeyString []*genericStringTag

func (s ascendingByKeyString) Len() int           { return len(s) }
func (s ascendingByKeyString) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s ascendingByKeyString) Less(i, j int) bool { return s[i].key < s[j].key }

// TagsToPromLabels converts tags to prometheus labels
func (gt *genericStringTags) ToPromLabels() []*prompb.Label {
	labels := make([]*prompb.Label, 0, len(gt.tags))
	for _, tag := range gt.tags {
		labels = append(labels, &prompb.Label{
			Name:  tag.key,
			Value: tag.value,
		})
	}
	return labels
}

func (gt *genericStringTags) ID() CoordinatorID {
	sep := byte(',')
	eq := byte('=')
	tags := gt.tags
	sort.Sort(ascendingByKeyString(tags))
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

// NewGenericStringTags returns a new insteance of Tags
func NewGenericStringTags(tags map[string]string) Tags {
	tagList := make([]*genericStringTag, 0)
	for k, v := range tags {
		tagList = append(tagList, &genericStringTag{
			key:   k,
			value: v,
		})
	}
	return &genericStringTags{
		tags: tagList,
	}
}
