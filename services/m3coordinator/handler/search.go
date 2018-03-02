package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/util/logging"
)

const (
	// SearchURL is the url to search for metric ids
	SearchURL = "/search"
)

type SearchHandler struct {
	store storage.Storage
}

func NewSearchHandler(storage storage.Storage) http.Handler {
	return &SearchHandler{store: storage}
}

func (h *SearchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger := logging.WithContext(r.Context())
	logger.Info("serving search endpoint")

	req, rErr := h.parseRequest(r)
	if rErr != nil {
		Error(w, rErr.Error(), rErr.Code())
		return
	}
	fmt.Println("tags: ", req.TagMatchers)

	h.search(r.Context(), req, newFetchOptions())
	// indexQuery := storage.FetchQueryToM3Query(req)

	// results, err := h.Search(indexQuery)

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
	results, err := h.store.FetchTags(reqCtx, searchReq, searchOpts)

	return nil, nil
}

func newFetchOptions() *storage.FetchOptions {
	return &storage.FetchOptions{
		Limit: 100,
	}
}
