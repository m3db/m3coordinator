package handler

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/util/logging"

	"go.uber.org/zap"
)

const (
	// SearchURL is the url to search for metric ids
	SearchURL = "/search"
)

// SearchHandler represents a handler for the search endpoint
type SearchHandler struct {
	store storage.Storage
}

// NewSearchHandler returns a new instance of handler
func NewSearchHandler(storage storage.Storage) http.Handler {
	return &SearchHandler{store: storage}
}

func (h *SearchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger := logging.WithContext(r.Context())

	req, rErr := h.parseRequest(r)
	if rErr != nil {
		Error(w, rErr.Error(), rErr.Code())
		return
	}

	results, err := h.search(r.Context(), req, newFetchOptions())
	if err != nil {
		logger.Error("unable to fetch data", zap.Any("error", err))
		Error(w, err, http.StatusBadRequest)
		return
	}

	jsonData, err := json.Marshal(results)
	if err != nil {
		logger.Error("unable to marshal json", zap.Any("error", err))
		Error(w, err, http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonData)
}

func (h *SearchHandler) parseRequest(r *http.Request) (*storage.FetchQuery, *ParseError) {
	body, _ := ioutil.ReadAll(r.Body)

	var fetchQuery storage.FetchQuery
	if err := json.Unmarshal(body, &fetchQuery); err != nil {
		return nil, NewParseError(err, http.StatusBadRequest)
	}

	return &fetchQuery, nil
}

func (h *SearchHandler) search(reqCtx context.Context, searchReq *storage.FetchQuery, searchOpts *storage.FetchOptions) (*storage.SearchResults, error) {
	return h.store.FetchTags(reqCtx, searchReq, searchOpts)
}

func newFetchOptions() *storage.FetchOptions {
	return &storage.FetchOptions{
		Limit: 100,
	}
}
