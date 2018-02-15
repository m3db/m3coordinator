package errors

import (
	"errors"
)

var (
	// InvalidFetchResponse is returned when fetch fails from storage.
	InvalidFetchResponse = errors.New("invalid response from fetch")

	// FetchResponseOrder is returned fetch responses are not in order.
	FetchResponseOrder = errors.New("responses out of order for fetch")

	// FetchRequestType is an error returned when response from fetch has invalid type.
	FetchRequestType = errors.New("invalid request type")

	// InvalidFetchResult is an error returned when fetch result is invalid.
	InvalidFetchResult = errors.New("invalid fetch result")

)
