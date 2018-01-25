package storage

import (
	"fmt"
	"time"

	"github.com/m3db/m3coordinator/generated/proto/prometheus/prompb"
	"github.com/m3db/m3coordinator/models"
	"github.com/m3db/m3coordinator/ts"
)

// PromWriteTSToM3 converts a prometheus write query to an M3 one
func PromWriteTSToM3(timeseries *prompb.TimeSeries) *models.Tags {
	tagMatchers := PromLabelsToM3Tags(timeseries.Labels)

	return tagMatchers
}

// PromLabelsToM3Tags converts Prometheus labels to M3 tags
func PromLabelsToM3Tags(labels []*prompb.Label) *models.Tags {
	tags := make(models.Tags, len(labels))
	for _, label := range labels {
		tags[label.Name] = label.Value
	}

	return &tags
}

// PromReadQueryToM3 converts a prometheus read query to m3 ready query
func PromReadQueryToM3(query *prompb.Query) (*models.ReadQuery, error) {
	tagMatchers, err := PromMatchersToM3(query.Matchers)
	if err != nil {
		return nil, err
	}

	return &models.ReadQuery{
		TagMatchers: tagMatchers,
		Start:       PromTimestampToTime(query.StartTimestampMs),
		End:         PromTimestampToTime(query.EndTimestampMs),
	}, nil
}

// PromMatchersToM3 converts prometheus label matchers to m3 matchers
func PromMatchersToM3(matchers []*prompb.LabelMatcher) (models.Matchers, error) {
	tagMatchers := make(models.Matchers, len(matchers))
	var err error
	for idx, matcher := range matchers {
		tagMatchers[idx], err = PromMatcherToM3(matcher)
		if err != nil {
			return nil, err
		}
	}

	return tagMatchers, nil
}

// PromMatcherToM3 converts a prometheus label matcher to m3 matcher
func PromMatcherToM3(matcher *prompb.LabelMatcher) (*models.Matcher, error) {
	matchType, err := PromTypeToM3(matcher.Type)
	if err != nil {
		return nil, err
	}

	return models.NewMatcher(matchType, matcher.Name, matcher.Value)
}

// PromTypeToM3 converts a prometheus label type to m3 matcher type
func PromTypeToM3(labelType prompb.LabelMatcher_Type) (models.MatchType, error) {
	switch labelType {
	case prompb.LabelMatcher_EQ:
		return models.MatchEqual, nil
	case prompb.LabelMatcher_NEQ:
		return models.MatchNotEqual, nil
	case prompb.LabelMatcher_RE:
		return models.MatchRegexp, nil
	case prompb.LabelMatcher_NRE:
		return models.MatchNotRegexp, nil

	default:
		return 0, fmt.Errorf("unknown match type %v", labelType)

	}
}

// PromTimestampToTime converts a prometheus timestamp to time.Time
func PromTimestampToTime(timestampMS int64) time.Time {
	return time.Unix(0, timestampMS*int64(time.Millisecond))
}

// FetchResultToPromResults converts fetch results from M3 to Prometheus results
func FetchResultToPromResults(result *FetchResult) ([]*prompb.QueryResult, error) {
	results := make([]*prompb.QueryResult, len(result.SeriesList))
	for idx, series := range result.SeriesList {
		result, err := SeriesToPromResult(series)
		if err != nil {
			return nil, err
		}

		results[idx] = result
	}

	return results, nil
}

// SeriesToPromResult converts a series to prometheus result
func SeriesToPromResult(series *ts.Series) (*prompb.QueryResult, error) {
	labels := TagsToPromLabels(series.Tags)
	samples := SeriesToPromSamples(series)
	timeseries := make([]*prompb.TimeSeries, 1)
	timeseries[0] = &prompb.TimeSeries{Labels: labels, Samples: samples}
	return &prompb.QueryResult{Timeseries: timeseries}, nil
}

// TagsToPromLabels converts tags to prometheus labels
func TagsToPromLabels(tags models.Tags) []*prompb.Label {
	labels := make([]*prompb.Label, 0, len(tags))
	for k, v := range tags {
		labels = append(labels, &prompb.Label{Name: k, Value: v})
	}

	return labels
}

// SeriesToPromSamples series datapoints to prometheus samples
func SeriesToPromSamples(series *ts.Series) []*prompb.Sample {
	samples := make([]*prompb.Sample, series.Len())
	for i := 0; i < series.Len(); i++ {
		samples[i] = &prompb.Sample{
			Timestamp: series.StartTimeForStep(i).UnixNano() / int64(time.Millisecond),
			Value:     series.ValueAt(i),
		}
	}
	return samples
}
