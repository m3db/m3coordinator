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
	"context"
	"net/http"

	"github.com/m3db/m3coordinator/services/m3coordinator/config"
	"github.com/m3db/m3coordinator/services/m3coordinator/handler"
	"github.com/m3db/m3coordinator/util/logging"

	m3clusterClient "github.com/m3db/m3cluster/client"

	"go.uber.org/zap"
)

const (
	// DeleteURL is the url for the placement delete handler (with the POST method).
	DeleteURL = "/placement/delete"

	// DeleteHTTPMethodURL is the url for the placement delete handler (with the DELETE method).
	DeleteHTTPMethodURL = "/placement"
)

// deleteHandler represents a handler for placement delete endpoint.
type deleteHandler Handler

// NewDeleteHandler returns a new instance of handler.
func NewDeleteHandler(clusterClient m3clusterClient.Client, cfg config.Configuration) http.Handler {
	return &deleteHandler{
		clusterClient: clusterClient,
		config:        cfg,
	}
}

func (h *deleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)

	if err := h.delete(ctx); err != nil {
		logger.Error("unable to delete placement", zap.Any("error", err))
		handler.Error(w, err, http.StatusInternalServerError)
	}
}

func (h *deleteHandler) delete(ctx context.Context) error {
	ps, err := PlacementService(h.clusterClient, h.config)
	if err != nil {
		return err
	}

	return ps.Delete()
}
