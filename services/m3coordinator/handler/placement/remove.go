// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package placement

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"

	"github.com/m3db/m3coordinator/generated/proto/admin"
	"github.com/m3db/m3coordinator/services/m3coordinator/handler"
	"github.com/m3db/m3coordinator/util/logging"

	"github.com/m3db/m3cluster/placement"

	"go.uber.org/zap"
)

const (
	// RemoveURL is the url for the placement remove handler (with the POST method).
	RemoveURL = "/placement/remove"
)

var (
	errMissingInstanceIds = errors.New("missing instance IDs")
)

// removeHandler represents a handler for placement remove endpoint.
type removeHandler Handler

// NewRemoveHandler returns a new instance of handler.
func NewRemoveHandler(service placement.Service) http.Handler {
	return &removeHandler{service: service}
}

func (h *removeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)

	req, rErr := h.parseRequest(r)
	if rErr != nil {
		handler.Error(w, rErr.Error(), rErr.Code())
		return
	}

	placement, err := h.remove(req)
	if err != nil {
		logger.Error("unable to remove placement", zap.Any("error", err))
		handler.Error(w, err, http.StatusInternalServerError)
		return
	}

	placementProto, err := placement.Proto()
	if err != nil {
		logger.Error("unable to get placement protobuf", zap.Any("error", err))
		handler.Error(w, err, http.StatusInternalServerError)
		return
	}

	resp := &admin.PlacementGetResponse{
		Placement: placementProto,
	}

	handler.WriteProtoMsgJSONResponse(w, resp, logger)
}

func (h *removeHandler) parseRequest(r *http.Request) (*admin.PlacementRemoveRequest, *handler.ParseError) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, handler.NewParseError(err, http.StatusBadRequest)
	}

	defer r.Body.Close()

	removeReq := new(admin.PlacementRemoveRequest)
	if err := json.Unmarshal(body, removeReq); err != nil {
		return nil, handler.NewParseError(err, http.StatusBadRequest)
	}

	return removeReq, nil
}

func (h *removeHandler) remove(r *admin.PlacementRemoveRequest) (placement.Placement, error) {
	if len(r.InstanceIds) == 0 {
		return nil, errMissingInstanceIds
	}

	newPlacement, err := h.service.RemoveInstances(r.InstanceIds)
	if err != nil {
		return nil, err
	}

	return newPlacement, nil
}