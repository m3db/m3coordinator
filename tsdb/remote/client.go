package remote

import (
	"context"
	"io"

	"github.com/m3db/m3coordinator/generated/proto/m3coordinator"
	"github.com/m3db/m3coordinator/ts"

	"github.com/m3db/m3coordinator/storage"
	"google.golang.org/grpc"
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
func NewGrpcClient(address string) (Client, error) {
	cc, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	client := rpc.NewQueryClient(cc)

	return &grpcClient{client: client}, nil
}

// Fetch reads from remote client storage
func (c *grpcClient) Fetch(ctx context.Context, query *storage.ReadQuery) (*storage.FetchResult, error) {
	client := c.client
	fetchClient, err := client.Fetch(ctx, EncodeReadQuery(query))
	if err != nil {
		return nil, err
	}
	defer fetchClient.CloseSend()

	tsSeries := make([]*ts.Series, 0)
	for {
		result, err := fetchClient.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
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
	rpcQuery := EncodeWriteQuery(query)
	err = writeClient.Send(rpcQuery)
	if err != nil {
		return err
	}

	_, err = writeClient.CloseAndRecv()
	return err
}
