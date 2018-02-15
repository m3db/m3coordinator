package execution

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type request struct {
	order int
}

func (f *request) Process() *Response {
	if f.order == 0 {
		time.Sleep(2 * time.Millisecond)
	}

	return &Response{
		Value: f.order,
	}
}

func TestOrderedParallel(t *testing.T) {
	requests := make([]Request, 3)
	requests[0] = &request{0}
	requests[1] = &request{1}
	requests[2] = &request{2}

	reqResponseChan := ExecuteParallel(requests)
	responses := make([]*RequestResponse, 0)
	for reqResponse := range reqResponseChan {
		responses = append(responses, reqResponse)
	}

	assert.Len(t, responses, len(requests), "should have same number of responses as request")
	assert.NotNil(t, responses[0].Response.Value, "value should not be nil")
}
