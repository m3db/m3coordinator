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

package namespace

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/m3db/m3coordinator/generated/proto/admin"
	"github.com/m3db/m3coordinator/services/m3coordinator/handler"
	"github.com/m3db/m3coordinator/util/logging"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3db/storage/namespace"

	"go.uber.org/zap"
)

const (
	// DeleteURL is the url for the namespace delete handler (with the POST method).
	DeleteURL = "/namespace/delete"
)

var (
	errNamespaceNotFound = errors.New("unable to find a namespace with specified name")
)

// deleteHandler represents a handler for namespace delete endpoint.
type deleteHandler Handler

// NewDeleteHandler returns a new instance of handler.
func NewDeleteHandler(store kv.Store) http.Handler {
	return &deleteHandler{store: store}
}

func (h *deleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)

	req, rErr := h.parseRequest(r)
	if rErr != nil {
		handler.Error(w, rErr.Error(), rErr.Code())
		return
	}

	err := h.delete(req)
	if err != nil {
		logger.Error("unable to delete namespace", zap.Any("error", err))
		if err == errNamespaceNotFound {
			handler.Error(w, err, http.StatusBadRequest)
		} else {
			handler.Error(w, err, http.StatusInternalServerError)
		}
	}
}

func (h *deleteHandler) parseRequest(r *http.Request) (*admin.NamespaceDeleteRequest, *handler.ParseError) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, handler.NewParseError(err, http.StatusBadRequest)
	}

	defer r.Body.Close()

	deleteReq := new(admin.NamespaceDeleteRequest)
	if err := json.Unmarshal(body, deleteReq); err != nil {
		return nil, handler.NewParseError(err, http.StatusBadRequest)
	}

	return deleteReq, nil
}

func (h *deleteHandler) delete(r *admin.NamespaceDeleteRequest) error {
	metadatas, version, err := Metadata(h.store)
	if err != nil {
		return err
	}

	mdIdx := -1
	for idx, md := range metadatas {
		if md.ID().String() == r.Name {
			mdIdx = idx
			break
		}
	}

	if mdIdx == -1 {
		return errNamespaceNotFound
	}

	// Replace the index where we found the metadata with the last element, then truncate
	metadatas[mdIdx] = metadatas[len(metadatas)-1]
	metadatas = metadatas[:len(metadatas)-1]

	// Update namespace map and set kv
	nsMap, err := namespace.NewMap(metadatas)
	if err != nil {
		return fmt.Errorf("failed to delete namespace: %v", err)
	}

	protoRegistry := namespace.ToProto(nsMap)
	_, err = h.store.CheckAndSet(M3DBNodeNamespacesKey, version, protoRegistry)
	if err != nil {
		return fmt.Errorf("failed to delete namespace: %v", err)
	}

	return nil
}
