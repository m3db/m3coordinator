package execution

import (
	"sync"
)

// Request is input for parallel execution
type Request interface {
	Process() *Response
}

// RequestResponse is used to combine both request and response in output
type RequestResponse struct {
	Request  Request
	Response *Response
}

// Response is returned from parallel execution
type Response struct {
	Value interface{}
	Err   error
}

// ExecuteParallel executes a slice of requests in parallel and returns unordered results
func ExecuteParallel(requests []Request) <-chan *RequestResponse {
	requestResponse := make(chan *RequestResponse)
	go processParallel(requests, requestResponse)
	return requestResponse
}

func processParallel(requests []Request, requestResponse chan<- *RequestResponse) {
	defer close(requestResponse)
	var wg sync.WaitGroup
	wg.Add(len(requests))
	for _, req := range requests {
		go func(r Request) {
			defer wg.Done()
			response := r.Process()
			requestResponse <- &RequestResponse{Request: r, Response: response}
		}(req)
	}
	wg.Wait()
}
