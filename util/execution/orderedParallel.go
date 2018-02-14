package execution

import (
	"sync"
)

// Request is input for parallel execution
type Request interface {
	Process() *Response
	Order() int
}

// Response is returned from parallel execution
type Response struct {
	Value interface{}
	Err   error
	Order int
}

// ExecuteParallel executes a slice of requests in parallel and returns ordered results
func ExecuteParallel(requests []Request) []*Response {
	unordered := make(chan *Response)
	go processParallel(requests, unordered)
	return orderResults(unordered, len(requests))

}

func processParallel(requests []Request, responses chan<- *Response) {
	defer close(responses)
	var wg sync.WaitGroup
	wg.Add(len(requests))
	for _, req := range requests {
		go func(r Request) {
			responses <- r.Process()
			wg.Done()
		}(req)
	}
	wg.Wait()
}

func orderResults(unordered <- chan *Response, size int) []*Response {
	orderedResponses := make([]*Response, size)
	for response := range unordered {
		orderedResponses[response.Order] = response
	}

	return orderedResponses
}
