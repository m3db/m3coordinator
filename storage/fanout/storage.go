package fanout

import (
	"context"

	"github.com/m3db/m3coordinator/errors"
	"github.com/m3db/m3coordinator/policy/filter"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/ts"
	"github.com/m3db/m3coordinator/util/execution"
)

type fanoutStorage struct {
	stores      []storage.Storage
	readFilter  filter.Storage
	writeFilter filter.Storage
}

// NewStorage creates a new remote Storage instance.
func NewStorage(stores []storage.Storage, readFilter filter.Storage, writeFilter filter.Storage) storage.Storage {
	return &fanoutStorage{stores: stores, readFilter: readFilter, writeFilter: writeFilter}
}

func (s *fanoutStorage) Fetch(ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (*storage.FetchResult, error) {
	stores := filterStores(s.stores, s.readFilter, query)
	requests := make([]execution.Request, len(stores))
	for idx, store := range stores {
		requests[idx] = newFetchRequest(ctx, store, query, options)
	}

	requestResponseChan := execution.ExecuteParallel(requests)
	return handleFetchResponses(requestResponseChan)
}

func handleFetchResponses(requestResponseChan <-chan *execution.RequestResponse) (*storage.FetchResult, error) {
	seriesList := make([]*ts.Series, 0)
	result := &storage.FetchResult{SeriesList: seriesList, LocalOnly: true}
	for reqResponse := range requestResponseChan {

		if reqResponse.Response.Err != nil {
			return nil, reqResponse.Response.Err
		}

		// This type cast can be removed if we were to create a slice of *fetchRequest instead
		fetchreq, ok := reqResponse.Request.(*fetchRequest)
		if !ok {
			return nil, errors.ErrFetchRequestType
		}

		// We can  optimize this by storing the result on the fetch request but that makes it less clean since
		// you will be both reading and writing to the same slice concurrently
		fetchResult, ok := reqResponse.Response.Value.(*storage.FetchResult)
		if !ok {
			return nil, errors.ErrInvalidFetchResult
		}

		if fetchreq.store.Type() != storage.TypeLocalDC {
			result.LocalOnly = false
		}

		result.SeriesList = append(result.SeriesList, fetchResult.SeriesList...)
	}

	return result, nil
}

func (s *fanoutStorage) Write(ctx context.Context, query *storage.WriteQuery) error {
	stores := filterStores(s.stores, s.writeFilter, query)
	requests := make([]execution.Request, len(stores))
	for idx, store := range stores {
		requests[idx] = newWriteRequest(ctx, store, query)
	}

	requestResponseChan := execution.ExecuteParallel(requests)
	// Fail on single error, assume writes are idempotent
	for reqResponse := range requestResponseChan {
		if reqResponse.Response.Err != nil {
			return reqResponse.Response.Err
		}
	}

	return nil
}

func (s *fanoutStorage) Type() storage.Type {
	return storage.TypeMultiDC
}

func filterStores(stores []storage.Storage, filterPolicy filter.Storage, query storage.Query) []storage.Storage {
	filtered := make([]storage.Storage, 0)
	for _, s := range stores {
		if filterPolicy(query, s) {
			filtered = append(filtered, s)
		}
	}
	return filtered
}

type fetchRequest struct {
	store   storage.Storage
	query   *storage.FetchQuery
	options *storage.FetchOptions
	ctx     context.Context
}

func newFetchRequest(ctx context.Context, store storage.Storage, query *storage.FetchQuery, options *storage.FetchOptions) execution.Request {
	return &fetchRequest{
		store:   store,
		ctx:     ctx,
		query:   query,
		options: options,
	}
}

func (f *fetchRequest) Process() *execution.Response {
	result, err := f.store.Fetch(f.ctx, f.query, f.options)
	return &execution.Response{
		Value: result,
		Err:   err,
	}
}

type writeRequest struct {
	store storage.Storage
	query *storage.WriteQuery
	ctx   context.Context
}

func newWriteRequest(ctx context.Context, store storage.Storage, query *storage.WriteQuery) execution.Request {
	return &writeRequest{
		store: store,
		ctx:   ctx,
		query: query,
	}
}

func (f *writeRequest) Process() *execution.Response {
	err := f.store.Write(f.ctx, f.query)
	return &execution.Response{
		Err: err,
	}
}
