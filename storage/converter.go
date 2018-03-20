package storage

import (
	"fmt"
	"time"

	"github.com/m3db/m3coordinator/generated/proto/prometheus/prompb"
	"github.com/m3db/m3coordinator/models"
	"github.com/m3db/m3coordinator/models/m3tag"
	"github.com/m3db/m3coordinator/services/m3coordinator/options"
	"github.com/m3db/m3coordinator/ts"

	"github.com/m3db/m3x/context"
	xtime "github.com/m3db/m3x/time"
)

const (
	xTimeUnit = xtime.Millisecond
)

// PromWriteTSToM3 converts a prometheus write query to an M3 one
func PromWriteTSToM3(ctx context.Context, opts options.Options, timeseries *prompb.TimeSeries) *WriteQuery {
	tags := m3tag.PromLabelsToM3Tags(ctx, opts, timeseries.Labels)
	datapoints := PromSamplesToM3Datapoints(timeseries.Samples)

	return &WriteQuery{
		Tags:       tags,
		Datapoints: datapoints,
		Unit:       xTimeUnit,
		Annotation: nil,
	}
}

// PromSamplesToM3Datapoints converts Prometheus samples to M3 datapoints
func PromSamplesToM3Datapoints(samples []*prompb.Sample) ts.Datapoints {
	datapoints := make(ts.Datapoints, 0, len(samples))
	for _, sample := range samples {
		timestamp := TimestampToTime(sample.Timestamp)
		datapoints = append(datapoints, &ts.Datapoint{Timestamp: timestamp, Value: sample.Value})
	}

	return datapoints
}

// PromReadQueryToM3 converts a prometheus read query to m3 ready query
func PromReadQueryToM3(query *prompb.Query) (*FetchQuery, error) {
	tagMatchers, err := PromMatchersToM3(query.Matchers)
	if err != nil {
		return nil, err
	}

	return &FetchQuery{
		TagMatchers: tagMatchers,
		Start:       TimestampToTime(query.StartTimestampMs),
		End:         TimestampToTime(query.EndTimestampMs),
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

// TimestampToTime converts a prometheus timestamp to time.Time
func TimestampToTime(timestampMS int64) time.Time {
	return time.Unix(0, timestampMS*int64(time.Millisecond))
}

// TimeToTimestamp converts a time.Time to prometheus timestamp
func TimeToTimestamp(timestamp time.Time) int64 {
	return timestamp.UnixNano() / int64(time.Millisecond)
}

// FetchResultToPromResult converts fetch results from M3 to Prometheus result
func FetchResultToPromResult(result *FetchResult) (*prompb.QueryResult, error) {
	timeseries := make([]*prompb.TimeSeries, 0)

	for _, series := range result.SeriesList {
		promTs, err := SeriesToPromTS(series)
		if err != nil {
			return nil, err
		}
		timeseries = append(timeseries, promTs)
	}

	return &prompb.QueryResult{
		Timeseries: timeseries,
	}, nil
}

// SeriesToPromTS converts a series to prometheus timeseries
func SeriesToPromTS(series *ts.Series) (*prompb.TimeSeries, error) {
	labels := models.TagsToPromLabels(series.Tags)
	samples := SeriesToPromSamples(series)
	return &prompb.TimeSeries{Labels: labels, Samples: samples}, nil
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
