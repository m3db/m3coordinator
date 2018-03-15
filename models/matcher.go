package models

import (
	"errors"
	"fmt"
	"regexp"
)

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

func (s Matchers) Len() int           { return len(s) }
func (s Matchers) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Matchers) Less(i, j int) bool { return s[i].Name < s[j].Name }
