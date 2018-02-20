package execution

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type request struct {
	order     int
	processed bool
	err       error
}

func (f *request) Process(ctx context.Context) error {
	f.processed = true
	if f.err != nil {
		return f.err
	}

	if f.order == 0 {
		time.Sleep(2 * time.Millisecond)
	}

	return nil
}

func (f *request) String() string {
	return fmt.Sprintf("%v %v %v", f.order, f.processed, f.err)
}

func TestOrderedParallel(t *testing.T) {
	requests := make([]Request, 3)
	requests[0] = &request{order: 0}
	requests[1] = &request{order: 1}
	requests[2] = &request{order: 2}

	err := ExecuteParallel(context.Background(), requests)
	require.NoError(t, err, "no error during parallel execute")
	assert.True(t, requests[0].(*request).processed, "request processed")
}
