package options

import (
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
)

const (
	// defaultIdentifierPoolSize is the default identifier pool size
	defaultIdentifierPoolSize = 8192
)

var (
	// defaultIdentifierPoolBytesPoolSizes is the default bytes pool sizes for the identifier pool
	defaultIdentifierPoolBytesPoolSizes = []pool.Bucket{
		{Capacity: 256, Count: defaultIdentifierPoolSize},
	}
)

type options struct {
	identifierPool ident.Pool
	contextPool    context.Pool
}

// Options represents pooling options
type Options interface {
	IdentifierPool() ident.Pool
	ContextPool() context.Pool
}

// NewOptions creates a new set of default client options
func NewOptions() Options {
	buckets := defaultIdentifierPoolBytesPoolSizes
	bytesPool := pool.NewCheckedBytesPool(buckets, nil,
		func(sizes []pool.Bucket) pool.BytesPool {
			return pool.NewBytesPool(sizes, nil)
		})
	bytesPool.Init()
	simplePool := ident.NewPool(bytesPool, pool.NewObjectPoolOptions())

	poolOpts := pool.NewObjectPoolOptions().
		SetSize(defaultIdentifierPoolSize)

	contextPool := context.NewPool(context.NewOptions().
		SetContextPoolOptions(poolOpts).
		SetFinalizerPoolOptions(poolOpts))

	return &options{
		identifierPool: simplePool,
		contextPool:    contextPool,
	}
}

func (op *options) IdentifierPool() ident.Pool {
	return op.identifierPool
}

func (op *options) ContextPool() context.Pool {
	return op.contextPool
}
