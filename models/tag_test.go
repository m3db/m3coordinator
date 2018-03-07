package models

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func mustNewMatcher(t *testing.T, mType MatchType, value string) *Matcher {
	m, err := NewMatcher(mType, "", value)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestMatcher(t *testing.T) {
	tests := []struct {
		matcher *Matcher
		value   string
		match   bool
	}{
		{
			matcher: mustNewMatcher(t, MatchEqual, "bar"),
			value:   "bar",
			match:   true,
		},
		{
			matcher: mustNewMatcher(t, MatchEqual, "bar"),
			value:   "foo-bar",
			match:   false,
		},
		{
			matcher: mustNewMatcher(t, MatchNotEqual, "bar"),
			value:   "bar",
			match:   false,
		},
		{
			matcher: mustNewMatcher(t, MatchNotEqual, "bar"),
			value:   "foo-bar",
			match:   true,
		},
		{
			matcher: mustNewMatcher(t, MatchRegexp, "bar"),
			value:   "bar",
			match:   true,
		},
		{
			matcher: mustNewMatcher(t, MatchRegexp, "bar"),
			value:   "foo-bar",
			match:   false,
		},
		{
			matcher: mustNewMatcher(t, MatchRegexp, ".*bar"),
			value:   "foo-bar",
			match:   true,
		},
		{
			matcher: mustNewMatcher(t, MatchNotRegexp, "bar"),
			value:   "bar",
			match:   false,
		},
		{
			matcher: mustNewMatcher(t, MatchNotRegexp, "bar"),
			value:   "foo-bar",
			match:   true,
		},
		{
			matcher: mustNewMatcher(t, MatchNotRegexp, ".*bar"),
			value:   "foo-bar",
			match:   false,
		},
	}

	for _, test := range tests {
		if test.matcher.Matches(test.value) != test.match {
			t.Fatalf("Unexpected match result for matcher %v and value %q; want %v, got %v", test.matcher, test.value, test.match, !test.match)
		}
	}
}

func TestMatchType(t *testing.T) {
	require.Equal(t, MatchEqual.String(), "=")
	require.Equal(t, MatchNotEqual.String(), "!=")
	require.Equal(t, MatchRegexp.String(), "=~")
	require.Equal(t, MatchNotRegexp.String(), "!~")
}

func BenchmarkGenericTags(b *testing.B) {
	t := make([]*genericTag, 0)
	rand.Seed(0)
	for i := 0; i < 100; i++ {
		tag := time.Now().Format(time.RFC3339Nano) + string(rand.Int()) + "long_string_representing_long_tag_name"
		t = append(t, &genericTag{
			value: tag,
			key:   tag,
		})
	}
	tags := genericTags{tags: t}
	for i := 0; i < b.N; i++ {
		tags.ID()
	}
}

func TestTagsIDCorrectToLegacy(t *testing.T) {
	key := "keyvalue_pair"
	value := "some_test_value"

	tags := &genericTags{
		tags: []*genericTag{
			&genericTag{
				key:   key,
				value: value,
			},
		},
	}

	expectedID := "2755411844"
	id := tags.ID().String()
	require.Equal(t, expectedID, id)
}
