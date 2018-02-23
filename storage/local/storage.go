package local

import (
	"context"
	"time"

	"github.com/m3db/m3coordinator/errors"
	"github.com/m3db/m3coordinator/policy/resolver"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/ts"
	"github.com/m3db/m3coordinator/util/execution"

	"github.com/m3db/m3db/client"
)

const (
	initRawFetchAllocSize = 32
)

type localStorage struct {
	session        client.Session
	namespace      string
	policyResolver resolver.PolicyResolver
}

// NewStorage creates a new local Storage instance.
func NewStorage(session client.Session, namespace string, policyResolver resolver.PolicyResolver) storage.Storage {
	return &localStorage{session: session, namespace: namespace, policyResolver: policyResolver}
}

func (s *localStorage) Fetch(ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (*storage.FetchResult, error) {
	fetchReqs, err := s.policyResolver.Resolve(ctx, query.TagMatchers, query.Start, query.End)
	if err != nil {
		return nil, err
	}

	// Check if the query was interrupted.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-options.KillChan:
		return nil, errors.ErrQueryInterrupted
	default:
	}

	req := fetchReqs[0]
	reqRange := req.Ranges[0]
	iter, err := s.session.Fetch(s.namespace, req.ID, reqRange.Start, reqRange.End)
	if err != nil {
		return nil, err
	}

	defer iter.Close()

	result := make([]ts.Datapoint, 0, initRawFetchAllocSize)
	for iter.Next() {
		dp, _, _ := iter.Current()
		result = append(result, ts.Datapoint{Timestamp: dp.Timestamp, Value: dp.Value})
	}

	millisPerStep := int(reqRange.StoragePolicy.Resolution().Window / time.Millisecond)
	values := ts.NewValues(ctx, millisPerStep, len(result))

	// TODO: Figure out consolidation here
	for i, v := range result {
		values.SetValueAt(i, v.Value)
	}

	// TODO: Get the correct metric name
	tags, err := query.TagMatchers.ToTags()
	if err != nil {
		return nil, err
	}

	series := ts.NewSeries(ctx, tags.ID(), reqRange.Start, values, tags)
	seriesList := make([]*ts.Series, 1)
	seriesList[0] = series
	return &storage.FetchResult{
		SeriesList: seriesList,
	}, nil
}

func (s *localStorage) Write(ctx context.Context, query *storage.WriteQuery) error {
	// Check if the query was interrupted.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if query == nil {
		return errors.ErrNilQuery
	}

	id := query.Tags.ID()
	writeRequestCommon := writeRequestCommon{
		store:      s,
		writeQuery: query,
		id:         id,
	}

	requests := make([]execution.Request, len(query.Datapoints))
	for idx, datapoint := range query.Datapoints {
		requests[idx] = newWriteRequest(&writeRequestCommon, datapoint.Timestamp, datapoint.Value)
	}
	return execution.ExecuteParallel(ctx, requests)
}

func (s *localStorage) Type() storage.Type {
	return storage.TypeLocalDC
}

func (w *writeRequest) Process(ctx context.Context) error {
	localStore := w.writeRequestCommon.store
	query := w.writeRequestCommon.writeQuery
	return localStore.session.Write(localStore.namespace, w.writeRequestCommon.id, w.timestamp, w.value, query.Unit, query.Annotation)
}

type writeRequestCommon struct {
	store      *localStorage
	writeQuery *storage.WriteQuery
	id         string
}

type writeRequest struct {
	writeRequestCommon *writeRequestCommon
	timestamp          time.Time
	value              float64
}

func newWriteRequest(writeRequestCommon *writeRequestCommon, timestamp time.Time, value float64) execution.Request {
	return &writeRequest{
		writeRequestCommon: writeRequestCommon,
		timestamp:          timestamp,
		value:              value,
	}
}
