package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/m3db/m3coordinator/generated/proto/admin"
	"github.com/m3db/m3coordinator/util/logging"

	m3clusterClient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3db/storage/namespace"

	"go.uber.org/zap"
)

const (
	// NamespaceDeleteURL is the url for the placement delete handler (with the POST method).
	NamespaceDeleteURL = "/namespace/delete"
)

// NamespaceDeleteHandler represents a handler for placement delete endpoint.
type NamespaceDeleteHandler AdminHandler

// NewNamespaceDeleteHandler returns a new instance of handler.
func NewNamespaceDeleteHandler(clusterClient m3clusterClient.Client) http.Handler {
	return &NamespaceDeleteHandler{
		clusterClient: clusterClient,
	}
}

func (h *NamespaceDeleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)

	req, rErr := h.parseRequest(r)
	if rErr != nil {
		Error(w, rErr.Error(), rErr.Code())
		return
	}

	err := h.namespaceDelete(ctx, req)
	if err != nil {
		logger.Error("unable to get namespace", zap.Any("error", err))
		Error(w, err, http.StatusInternalServerError)
		return
	}
}

func (h *NamespaceDeleteHandler) parseRequest(r *http.Request) (*admin.NamespaceDeleteRequest, *ParseError) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, NewParseError(err, http.StatusBadRequest)
	}
	defer r.Body.Close()

	deleteReq := new(admin.NamespaceDeleteRequest)
	if err := json.Unmarshal(body, deleteReq); err != nil {
		return nil, NewParseError(err, http.StatusBadRequest)
	}

	return deleteReq, nil
}

func (h *NamespaceDeleteHandler) namespaceDelete(ctx context.Context, r *admin.NamespaceDeleteRequest) error {
	kv, err := GetKV(h.clusterClient)
	if err != nil {
		return err
	}

	currentMetadata, err := currentNamespaceMetadata(kv)
	if err != nil {
		return err
	}

	newMds := []namespace.Metadata{}
	found := false
	for _, md := range currentMetadata {
		if md.ID().String() == r.Name {
			found = true
			continue
		}
		newMds = append(newMds, md)
	}

	if !found {
		return errors.New("unable to find a namespace with specified name")
	}

	// If metadatas are empty, remove the key
	if len(newMds) == 0 {
		if _, err = kv.Delete(M3DBNodeNamespacesKey); err != nil {
			return fmt.Errorf("unable to delete kv key: %v", err)
		}

		return nil
	}

	// Update namespace map and set kv
	nsMap, err := namespace.NewMap(newMds)
	if err != nil {
		return fmt.Errorf("unable to delete kv key: %v", err)
	}

	protoRegistry := namespace.ToProto(nsMap)
	_, err = kv.Set(M3DBNodeNamespacesKey, protoRegistry)
	if err != nil {
		return fmt.Errorf("unable to update kv: %v", err)
	}

	return nil
}
