package fanout

import (
	"context"
	"fmt"

	"github.com/m3db/m3coordinator/policy/filter"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/ts"
	"github.com/m3db/m3coordinator/util/execution"
)

type fanoutStorage struct {
	stores []storage.Storage
	filter filter.Storage
}

// NewStorage creates a new remote Storage instance.
func NewStorage(stores []storage.Storage, filter filter.Storage) storage.Storage {
	return &fanoutStorage{stores: stores, filter: filter}
}

func (s *fanoutStorage) Fetch(ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (*storage.FetchResult, error) {
	stores := filterStores(s.stores, s.filter, query)
	requests := make([]execution.Request, len(stores))
	for idx, store := range stores {
		requests[idx] = newFetchRequest(ctx, store, query, options, idx)
	}

	responses := execution.ExecuteParallel(requests)
	return handleFetchResponses(requests, responses)
}

func handleFetchResponses(requests []execution.Request, responses []*execution.Response) (*storage.FetchResult, error) {
	if len(requests) != len(responses) {
		return nil, fmt.Errorf("invalid response from fetch")
	}

	seriesList := make([]*ts.Series, 0, len(requests))
	result := &storage.FetchResult{SeriesList: seriesList, LocalOnly: true}
	for idx, res := range responses {
		if requests[idx].Order() != res.Order {
			return nil, fmt.Errorf("responses out of order for fetch")
		}

		// This type cast can be removed if we were to create a slice of *fetchRequest instead
		fetchreq, ok := requests[idx].(*fetchRequest)
		if !ok {
			return nil, fmt.Errorf("invalid request type")
		}

		if res.Err != nil {
			return nil, res.Err
		}

		// We can  optimize this by storing the result on the fetch request but that makes it less clean since
		// you will be both reading and writing to the same slice concurrently
		fetchResult, ok := res.Value.(*storage.FetchResult)
		if !ok {
			return nil, fmt.Errorf("invalid fetch result")
		}

		if fetchreq.store.Type() != storage.TypeLocalDC {
			result.LocalOnly = false
		}

		result.SeriesList = append(result.SeriesList, fetchResult.SeriesList...)
	}

	return result, nil
}

func (s *fanoutStorage) Write(ctx context.Context, query *storage.WriteQuery) error {
	stores := filterStores(s.stores, s.filter, query)
	requests := make([]execution.Request, len(stores))
	for idx, store := range stores {
		requests[idx] = newWriteRequest(ctx, store, query, idx)
	}

	responses := execution.ExecuteParallel(requests)
	for _, resp := range responses {
		if resp.Err != nil {
			return resp.Err
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
	order   int
	query   *storage.FetchQuery
	options *storage.FetchOptions
	ctx     context.Context
}

func newFetchRequest(ctx context.Context, store storage.Storage, query *storage.FetchQuery, options *storage.FetchOptions, order int) execution.Request {
	return &fetchRequest{
		store:   store,
		ctx:     ctx,
		query:   query,
		options: options,
		order:   order,
	}
}
func (f *fetchRequest) Order() int {
	return f.order
}

func (f *fetchRequest) Process() *execution.Response {
	result, err := f.store.Fetch(f.ctx, f.query, f.options)
	return &execution.Response{
		Value: result,
		Err:   err,
		Order: f.order,
	}
}

type writeRequest struct {
	store   storage.Storage
	order   int
	query   *storage.WriteQuery
	ctx     context.Context
}

func newWriteRequest(ctx context.Context, store storage.Storage, query *storage.WriteQuery, order int) execution.Request {
	return &writeRequest{
		store:   store,
		ctx:     ctx,
		query:   query,
		order:   order,
	}
}
func (f *writeRequest) Order() int {
	return f.order
}

func (f *writeRequest) Process() *execution.Response {
	err := f.store.Write(f.ctx, f.query)
	return &execution.Response{
		Err:   err,
		Order: f.order,
	}
}
