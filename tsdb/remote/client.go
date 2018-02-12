package remote

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"time"

	"github.com/m3db/m3coordinator/errors"
	"github.com/m3db/m3coordinator/generated/proto/m3coordinator"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/ts"
	"github.com/m3db/m3coordinator/util/logging"

	"google.golang.org/grpc"
)

const (
	maxRetries   = 10
	initialDelay = time.Millisecond * 250
)

// Client is an interface
type Client interface {
	storage.Querier
	storage.Appender
}

type grpcClient struct {
	client rpc.QueryClient
}

// NewGrpcClient creates grpc client
func NewGrpcClient(address string) (Client, *grpc.ClientConn, error) {
	cc, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, nil, err
	}
	client := rpc.NewQueryClient(cc)

	return &grpcClient{client: client}, cc, nil
}

// Fetch reads from remote client storage
func (c *grpcClient) Fetch(ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (*storage.FetchResult, error) {
	id := logging.ReadContextID(ctx)
	var (
		fetchClient rpc.Query_FetchClient
		err         error
	)
	// Attempt to fetch, with incremental backoff if the server is busy
	for delay, attempt := initialDelay, 0; ; attempt++ {
		fetchClient, err = c.client.Fetch(ctx, EncodeFetchQuery(query, id))
		if err != nil {
			if attempt > maxRetries {
				return nil, err
			}
			delay = delay * 2
			randomDelay := time.Millisecond * (time.Duration)(500.0*rand.Float32())

			time.Sleep(delay + randomDelay)
		} else {
			break
		}
	}
	defer fetchClient.CloseSend()

	tsSeries := make([]*ts.Series, 0)
	for {
		select {
		// If query is killed during gRPC streaming, close the channel
		case <-options.KillChan:
			fmt.Println("kild")

			return nil, errors.ErrQueryInterrupted
		default:
		}
		result, err := fetchClient.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("err inner", err)

			return nil, err
		}
		rpcSeries := result.GetSeries()
		tsSeries = append(tsSeries, DecodeFetchResult(ctx, rpcSeries)...)
	}

	return &storage.FetchResult{LocalOnly: false, SeriesList: tsSeries}, nil
}

// Write writes to remote client storage
func (c *grpcClient) Write(ctx context.Context, query *storage.WriteQuery) error {
	client := c.client

	writeClient, err := client.Write(ctx)
	if err != nil {
		return err
	}

	id := logging.ReadContextID(ctx)
	rpcQuery := EncodeWriteQuery(query, id)
	err = writeClient.Send(rpcQuery)
	if err != nil {
		return err
	}

	_, err = writeClient.CloseAndRecv()
	return err
}
