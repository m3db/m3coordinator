package execution

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type request struct {
	order int
}

func (f *request) Order() int {
	return f.order
}

func (f *request) Process() *Response {
	if f.order == 0 {
		time.Sleep(2 * time.Millisecond)
	}

	return &Response{
		Order: f.order,
	}
}

func TestOrderedParallel(t *testing.T) {
	requests := make([]Request, 3)
	requests[0] = &request{0}
	requests[1] = &request{1}
	requests[2] = &request{2}

	responses := ExecuteParallel(requests)
	assert.Equal(t, responses[0].Order, 0, "ordered response")


}
