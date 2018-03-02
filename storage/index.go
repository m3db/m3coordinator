package storage

import (
	"github.com/m3db/m3coordinator/models"

	"github.com/m3db/m3ninx/index/segment"
)

// FetchQueryToM3Query converts an m3coordinator fetch query to an M3 query
func FetchQueryToM3Query(searchQuery *FetchQuery) *segment.Query {
	return &segment.Query{
		Filters:     MatchersToFilters(searchQuery.TagMatchers),
		Conjunction: segment.AndConjunction, // & is the only conjunction supported currently
	}
}

// MatchersToFilters converts matchers to M3 filters
func MatchersToFilters(matchers models.Matchers) []segment.Filter {
	var filters []segment.Filter

	for _, matcher := range matchers {
		var (
			negate bool
			regexp bool
		)
		if matcher.Type == models.MatchNotEqual || matcher.Type == models.MatchNotRegexp {
			negate = true
		}
		if matcher.Type == models.MatchNotRegexp || matcher.Type == models.MatchRegexp {
			regexp = true
		}

		filters = append(filters, segment.Filter{
			FieldName:        []byte(matcher.Name),
			FieldValueFilter: []byte(matcher.Value),
			Negate:           negate,
			Regexp:           regexp,
		})
	}
	return filters
}
