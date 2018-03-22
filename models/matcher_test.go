package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildNewMatcher(t *testing.T, mType MatchType, value string) *Matcher {
	m, err := NewMatcher(mType, "", value)
	require.NoError(t, err)
	return m
}

func TestInvalidMatcher(t *testing.T) {
	_, err := NewMatcher(MatchType(5), "", "value")
	require.Equal(t, ErrInvalidMatcher, err)
}

func TestMatcher(t *testing.T) {
	tests := []struct {
		matcher *Matcher
		value   string
		match   bool
	}{
		{
			matcher: buildNewMatcher(t, MatchEqual, "bar"),
			value:   "bar",
			match:   true,
		},
		{
			matcher: buildNewMatcher(t, MatchEqual, "bar"),
			value:   "foo-bar",
			match:   false,
		},
		{
			matcher: buildNewMatcher(t, MatchNotEqual, "bar"),
			value:   "bar",
			match:   false,
		},
		{
			matcher: buildNewMatcher(t, MatchNotEqual, "bar"),
			value:   "foo-bar",
			match:   true,
		},
		{
			matcher: buildNewMatcher(t, MatchRegexp, "bar"),
			value:   "bar",
			match:   true,
		},
		{
			matcher: buildNewMatcher(t, MatchRegexp, "bar"),
			value:   "foo-bar",
			match:   false,
		},
		{
			matcher: buildNewMatcher(t, MatchRegexp, ".*bar"),
			value:   "foo-bar",
			match:   true,
		},
		{
			matcher: buildNewMatcher(t, MatchNotRegexp, "bar"),
			value:   "bar",
			match:   false,
		},
		{
			matcher: buildNewMatcher(t, MatchNotRegexp, "bar"),
			value:   "foo-bar",
			match:   true,
		},
		{
			matcher: buildNewMatcher(t, MatchNotRegexp, ".*bar"),
			value:   "foo-bar",
			match:   false,
		},
	}

	for _, test := range tests {
		require.Equal(t, test.match, test.matcher.Matches(test.value), "matcher %v, value %q; want %v, got %v", test.matcher, test.value, test.match, !test.match)
	}
}

func TestStringMatcher(t *testing.T) {
	matcher, err := NewMatcher(MatchType(1), "n", "v")
	require.NoError(t, err)
	assert.Equal(t, matcher.String(), "n!=\"v\"")
}

func TestMatchType(t *testing.T) {
	require.Equal(t, MatchEqual.String(), "=")
	require.Equal(t, MatchNotEqual.String(), "!=")
	require.Equal(t, MatchRegexp.String(), "=~")
	require.Equal(t, MatchNotRegexp.String(), "!~")
	defer func() {
		r := recover()
		require.NotNil(t, r)
	}()
	_ = MatchType(100).String()
}
