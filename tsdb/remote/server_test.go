package remote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	m3err "github.com/m3db/m3coordinator/errors"
	"github.com/m3db/m3coordinator/models"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/ts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
)

const (
	name = "aa"
	mps  = 5
)

var (
	startTime, _ = time.Parse(time.RFC3339, "2000-02-06T11:54:48+07:00")
	tags         = models.Tags{"1": "b", "2": "c"}
	values       = []float64{1.0, 2.0, 3.0, 4.0}
	errWrite     = errors.New("write error")
	errRead      = errors.New("read error")
	initialPort  = 17762
	testMu       sync.Mutex
)

func generateAddress() string {
	testMu.Lock()
	defer testMu.Unlock()
	address := fmt.Sprintf("localhost:%d", initialPort)
	initialPort++
	return address
}

func makeValues(ctx context.Context) ts.Values {
	vals := ts.NewValues(ctx, mps, len(values))
	for i, v := range values {
		vals.SetValueAt(i, v)
	}
	return vals
}

func makeSeries(ctx context.Context) *ts.Series {
	return ts.NewSeries(ctx, name, startTime, makeValues(ctx), tags)
}

type mockStorage struct {
	t           *testing.T
	read        *storage.FetchQuery
	write       *storage.WriteQuery
	sleepMillis int
	numPages    int
	mu          sync.Mutex
}

func (s *mockStorage) Fetch(ctx context.Context, query *storage.FetchQuery, _ *storage.FetchOptions) (*storage.FetchResult, error) {
	readQueriesAreEqual(s.t, s.read, query)

	if s.sleepMillis > 0 {
		time.Sleep(time.Millisecond * time.Duration(s.sleepMillis))
	}

	s.mu.Lock()
	s.numPages--
	hasNext := s.numPages > 0
	s.mu.Unlock()

	tsSeries := []*ts.Series{makeSeries(ctx)}
	return &storage.FetchResult{
		SeriesList: tsSeries,
		LocalOnly:  false,
		HasNext:    hasNext,
	}, nil
}

func (s *mockStorage) Write(ctx context.Context, query *storage.WriteQuery) error {
	writeQueriesAreEqual(s.t, s.write, query)
	return nil
}

func (s *mockStorage) Type() storage.Type {
	return storage.Type(0)
}

func checkMultipleRemoteFetch(t *testing.T, res *storage.FetchResult, numResults int) {
	assert.False(t, res.LocalOnly)
	require.Len(t, res.SeriesList, numResults)
	for _, s := range res.SeriesList {
		assert.Equal(t, name, s.Name())
		assert.True(t, startTime.Equal(s.StartTime()))
		assert.Equal(t, tags, s.Tags)
		assert.Equal(t, name, s.Specification)
		assert.Equal(t, len(values), s.Len())
	}
}

func checkRemoteFetch(t *testing.T, res *storage.FetchResult) {
	checkMultipleRemoteFetch(t, res, 1)
}

func startServer(t *testing.T, host string, store storage.Storage) {
	server := CreateNewGrpcServer(store)
	waitForStart := make(chan struct{})
	go func() {
		err := StartNewGrpcServer(server, host, waitForStart)
		assert.Nil(t, err)
	}()
	<-waitForStart
}

func createStorageFetchOptions() *storage.FetchOptions {
	return &storage.FetchOptions{
		KillChan: make(chan struct{}),
	}
}

func createCtxReadWriteOpts(t *testing.T) (context.Context, *storage.FetchQuery, *storage.WriteQuery, *storage.FetchOptions) {
	ctx := context.Background()
	read, _, _ := createStorageFetchQuery(t)
	write, _ := createStorageWriteQuery(t)
	readOpts := createStorageFetchOptions()
	return ctx, read, write, readOpts
}

func TestRpc(t *testing.T) {
	ctx, read, write, readOpts := createCtxReadWriteOpts(t)
	store := &mockStorage{
		t:     t,
		read:  read,
		write: write,
	}
	host := generateAddress()
	startServer(t, host, store)
	client, err := NewGrpcClient(host)
	require.Nil(t, err)
	defer func() {
		err = client.Close()
		assert.NoError(t, err)
	}()

	fetch, err := client.Fetch(ctx, read, readOpts)
	require.Nil(t, err)
	checkRemoteFetch(t, fetch)

	err = client.Write(ctx, write)
	require.Equal(t, io.EOF, err)
}

func TestRpcMultipleRead(t *testing.T) {
	ctx, read, write, readOpts := createCtxReadWriteOpts(t)
	pages := 10
	store := &mockStorage{
		t:        t,
		read:     read,
		write:    write,
		numPages: pages,
	}
	host := generateAddress()
	startServer(t, host, store)
	client, err := NewGrpcClient(host)
	defer func() {
		err = client.Close()
		assert.NoError(t, err)
	}()
	require.Nil(t, err)

	fetch, err := client.Fetch(ctx, read, readOpts)
	require.Nil(t, err)
	checkMultipleRemoteFetch(t, fetch, pages)

	err = client.Write(ctx, write)
	require.Equal(t, io.EOF, err)
}

func TestRpcStopsStreamingWhenFetchKilledOnClient(t *testing.T) {
	ctx, read, write, readOpts := createCtxReadWriteOpts(t)
	store := &mockStorage{
		t:           t,
		read:        read,
		write:       write,
		sleepMillis: 100,
		numPages:    10,
	}
	host := generateAddress()
	startServer(t, host, store)
	client, err := NewGrpcClient(host)
	defer func() {
		err = client.Close()
		assert.NoError(t, err)
	}()
	require.Nil(t, err)

	go func() {
		time.Sleep(time.Millisecond * 150)
		readOpts.KillChan <- struct{}{}
	}()

	fetch, err := client.Fetch(ctx, read, readOpts)

	assert.Nil(t, fetch)
	assert.Equal(t, err, m3err.ErrQueryInterrupted)
}

func TestMultipleClientRpc(t *testing.T) {
	ctx, read, write, readOpts := createCtxReadWriteOpts(t)
	store := &mockStorage{
		t:           t,
		read:        read,
		write:       write,
		sleepMillis: 300,
	}
	host := generateAddress()
	startServer(t, host, store)

	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client, err := NewGrpcClient(host)
			defer func() {
				err = client.Close()
				assert.NoError(t, err)
			}()
			require.Nil(t, err)

			fetch, err := client.Fetch(ctx, read, readOpts)
			require.Nil(t, err)
			checkRemoteFetch(t, fetch)

			err = client.Write(ctx, write)
			require.Equal(t, io.EOF, err)
		}()
	}

	wg.Wait()
}

type errStorage struct {
	t     *testing.T
	read  *storage.FetchQuery
	write *storage.WriteQuery
}

func (s *errStorage) Fetch(ctx context.Context, query *storage.FetchQuery, _ *storage.FetchOptions) (*storage.FetchResult, error) {
	readQueriesAreEqual(s.t, s.read, query)
	return nil, errRead
}

func (s *errStorage) Write(ctx context.Context, query *storage.WriteQuery) error {
	writeQueriesAreEqual(s.t, s.write, query)
	return errWrite
}

func (s *errStorage) Type() storage.Type {
	return storage.Type(-1)
}

func TestErrRpc(t *testing.T) {
	ctx, read, write, readOpts := createCtxReadWriteOpts(t)
	store := &errStorage{
		t:     t,
		read:  read,
		write: write,
	}
	host := generateAddress()
	startServer(t, host, store)
	client, err := NewGrpcClient(host)
	defer func() {
		err = client.Close()
		assert.NoError(t, err)
	}()
	require.Nil(t, err)

	fetch, err := client.Fetch(ctx, read, readOpts)
	assert.Nil(t, fetch)
	assert.Equal(t, errRead.Error(), grpc.ErrorDesc(err))

	err = client.Write(ctx, write)
	assert.Equal(t, errWrite.Error(), grpc.ErrorDesc(err))
}
