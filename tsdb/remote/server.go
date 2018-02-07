package remote

import (
	"context"
	"fmt"
	"io"
	"net"

	"github.com/m3db/m3coordinator/generated/proto/m3coordinator"

	"github.com/m3db/m3coordinator/storage"
	"google.golang.org/grpc"
)

type grpcServer struct {
	ctx     context.Context
	storage storage.Storage
}

func newServer(ctx context.Context, store storage.Storage) *grpcServer {
	return &grpcServer{
		ctx:     ctx,
		storage: store,
	}
}

// CreateNewGrpcServer creates server, given context local storage
func CreateNewGrpcServer(ctx context.Context, store storage.Storage) *grpc.Server {
	server := grpc.NewServer()
	grpcServer := newServer(ctx, store)
	rpc.RegisterQueryServer(server, grpcServer)
	return server
}

// StartNewGrpcServer starts server on given address
func StartNewGrpcServer(server *grpc.Server, address string) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Println(err)
		return err
	}
	server.Serve(lis)
	return nil
}

// Fetch reads from local storage
func (s *grpcServer) Fetch(query *rpc.GrpcReadQuery, stream rpc.Query_FetchServer) error {
	storeQuery, err := DecodeReadQuery(query)
	if err != nil {
		return err
	}
	result, err := s.storage.Fetch(s.ctx, storeQuery)
	if err != nil {
		return err
	}
	return stream.Send(EncodeFetchResult(result))
}

// Write writes to local storage
func (s *grpcServer) Write(stream rpc.Query_WriteServer) error {
	for {
		write, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		err = s.storage.Write(s.ctx, DecodeWriteQuery(write))
		if err != nil {
			return err
		}
	}
}
