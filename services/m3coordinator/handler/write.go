package handler

import (
	"context"
	"net/http"

	"github.com/m3db/m3coordinator/generated/proto/prometheus/prompb"
	"github.com/m3db/m3coordinator/services/m3coordinator/options"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/util/execution"
	"github.com/m3db/m3coordinator/util/logging"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
)

const (
	// PromWriteURL is the url for the prom write handler
	PromWriteURL = "/api/v1/prom/write"
)

// PromWriteHandler represents a handler for prometheus write endpoint.
type PromWriteHandler struct {
	store   storage.Storage
	options options.Options
}

// NewPromWriteHandler returns a new instance of handler.
func NewPromWriteHandler(opts options.Options, store storage.Storage) http.Handler {
	return &PromWriteHandler{
		store:   store,
		options: opts,
	}
}

func (h *PromWriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	req, rErr := h.parseRequest(r)
	if rErr != nil {
		Error(w, rErr.Error(), rErr.Code())
		return
	}
	if err := h.write(r.Context(), h.options, req); err != nil {
		logging.WithContext(r.Context()).Error("Write error", zap.Any("err", err))
		Error(w, err, http.StatusInternalServerError)
		return
	}
}

func (h *PromWriteHandler) parseRequest(r *http.Request) (*prompb.WriteRequest, *ParseError) {
	reqBuf, err := ParsePromRequest(r)
	if err != nil {
		return nil, err
	}

	var req prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		return nil, NewParseError(err, http.StatusBadRequest)
	}

	return &req, nil
}

func (h *PromWriteHandler) write(ctx context.Context, opts options.Options, r *prompb.WriteRequest) error {
	requests := make([]execution.Request, len(r.Timeseries))
	xCtx := opts.ContextPool().Get()
	defer xCtx.Close()

	for idx, t := range r.Timeseries {
		ts := storage.PromWriteTSToM3(xCtx, opts, t)
		requests[idx] = newLocalWriteRequest(ts, h.store)
	}
	return execution.ExecuteParallel(ctx, requests)
}

func (w *localWriteRequest) Process(ctx context.Context) error {
	return w.store.Write(ctx, w.writeQuery)
}

type localWriteRequest struct {
	store      storage.Storage
	writeQuery *storage.WriteQuery
}

func newLocalWriteRequest(writeQuery *storage.WriteQuery, store storage.Storage) execution.Request {
	return &localWriteRequest{
		store:      store,
		writeQuery: writeQuery,
	}
}
