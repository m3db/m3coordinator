package handler

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/util/logging"

	"go.uber.org/zap"
)

const (
	// SearchURL is the url to search for metric ids
	SearchURL = "/search"

	defaultLimit = 1000
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

	query, opts, rErr := h.parseRequest(r)
	if rErr != nil {
		logger.Error("unable to parse request", zap.Any("error", rErr))
		Error(w, rErr.Error(), rErr.Code())
		return
	}

	results, err := h.search(r.Context(), query, opts)
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

func (h *SearchHandler) parseRequest(r *http.Request) (*storage.FetchQuery, *storage.FetchOptions, *ParseError) {
	defer r.Body.Close()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, nil, NewParseError(err, http.StatusBadRequest)
	}

	var fetchQuery storage.FetchQuery
	if err := json.Unmarshal(body, &fetchQuery); err != nil {
		return nil, nil, NewParseError(err, http.StatusBadRequest)
	}

	limitRaw := r.URL.Query().Get("limit")
	var limit int
	if limitRaw != "" {
		limit, err = strconv.Atoi(limitRaw)
		if err != nil {
			return nil, nil, NewParseError(err, http.StatusBadRequest)
		}
	} else {
		limit = defaultLimit
	}

	fetchOptions := newFetchOptions(limit)

	return &fetchQuery, &fetchOptions, nil
}

func (h *SearchHandler) search(ctx context.Context, query *storage.FetchQuery, opts *storage.FetchOptions) (*storage.SearchResults, error) {
	return h.store.FetchTags(ctx, query, opts)
}

func newFetchOptions(limit int) storage.FetchOptions {
	return storage.FetchOptions{
		Limit: limit,
	}
}
