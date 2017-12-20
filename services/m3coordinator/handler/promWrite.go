package handler

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/m3db/m3coordinator/generated/proto/prometheus/prompb"
	"github.com/m3db/m3coordinator/util/logging"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"go.uber.org/zap"
)

// PromWriteHandler represents a handler for prometheus write endpoint.
type PromWriteHandler struct {
}

// NewPromWriteHandler returns a new instance of handler.
func NewPromWriteHandler() http.Handler {
	return &PromWriteHandler{}
}

func (h *PromWriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	req, err := h.parseRequest(w, r)
	if err != nil {
		return
	}

	// TODO: Actual write instead of logging
	logging.WithContext(r.Context()).Info("Write request", zap.Any("req", req))
}
func (h *PromWriteHandler) parseRequest(w http.ResponseWriter, r *http.Request) (*prompb.WriteRequest, error) {
	if r.Body == nil {
		err := fmt.Errorf("empty request body")
		Error(w, err, http.StatusBadRequest)
		return nil, err
	}

	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		Error(w, err, http.StatusInternalServerError)
		return nil, err
	}

	if len(compressed) == 0 {
		Error(w, fmt.Errorf("empty request body"), http.StatusBadRequest)
		return nil, err
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		Error(w, err, http.StatusBadRequest)
		return nil, err
	}

	var req prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		Error(w, err, http.StatusBadRequest)
		return nil, err
	}

	return &req, nil
}
