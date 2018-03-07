package local

import (
	"context"
	"time"

	"github.com/m3db/m3coordinator/models"

	"github.com/m3db/m3coordinator/errors"
	"github.com/m3db/m3coordinator/policy/resolver"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/ts"
	"github.com/m3db/m3coordinator/util/execution"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"
)

const (
	initRawFetchAllocSize = 32
)

type localStorage struct {
	session        client.Session
	namespace      ident.ID
	policyResolver resolver.PolicyResolver
}

// NewStorage creates a new local Storage instance.
func NewStorage(session client.Session, namespace string, policyResolver resolver.PolicyResolver) storage.Storage {
	return &localStorage{
		session:        session,
		namespace:      ident.StringID(namespace),
		policyResolver: policyResolver,
	}
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
	id := ident.StringID(req.ID.String())
	namespace := s.namespace
	iter, err := s.session.Fetch(namespace, id, reqRange.Start, reqRange.End)
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

	series := ts.NewSeries(ctx, tags.ID().String(), reqRange.Start, values, tags)
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
		return errors.ErrNilWriteQuery
	}

	common := &writeRequestCommon{
		store:      s,
		annotation: query.Annotation,
		unit:       query.Unit,
		tags:       query.Tags,
	}

	requests := make([]execution.Request, len(query.Datapoints))
	for idx, datapoint := range query.Datapoints {
		requests[idx] = newWriteRequest(common, datapoint.Timestamp, datapoint.Value)
	}
	return execution.ExecuteParallel(ctx, requests)
}

func (s *localStorage) Type() storage.Type {
	return storage.TypeLocalDC
}

func (w *writeRequest) Process(ctx context.Context) error {
	common := w.writeRequestCommon
	store := common.store
	var (
		it ident.TagIterator
		id ident.ID
	)
	if tags, ok := common.tags.(*models.M3Tags); !ok {
		it = tags.GetIterator()
		id = tags.M3ID()
	} else {
		return errors.ErrNoClientAddresses
	}
	return store.session.WriteTagged(store.namespace, id, it, w.timestamp, w.value, common.unit, common.annotation)
}

type writeRequestCommon struct {
	store      *localStorage
	annotation []byte
	unit       xtime.Unit
	tags       models.Tags
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
