package handler

import (
	"context"
	"fmt"
	"net/http"

	"github.com/m3db/m3coordinator/generated/proto/admin"
	"github.com/m3db/m3coordinator/util/logging"

	m3clusterClient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/kv"
	nsproto "github.com/m3db/m3db/generated/proto/namespace"
	"go.uber.org/zap"
)

const (
	// NamespaceGetURL is the url for the placement get handler (with the GET method).
	NamespaceGetURL = "/namespace/get"

	// NamespaceGetHTTPMethodURL is the url for the placement get handler (with the GET method).
	NamespaceGetHTTPMethodURL = "/namespace"
)

// namespaceGetHandler represents a handler for placement get endpoint.
type namespaceGetHandler AdminHandler

// NewNamespaceGetHandler returns a new instance of handler.
func NewNamespaceGetHandler(clusterClient m3clusterClient.Client) http.Handler {
	return &namespaceGetHandler{
		clusterClient: clusterClient,
	}
}

func (h *namespaceGetHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)
	nsRegistry, err := h.namespaceGet(ctx)

	if err != nil {
		logger.Error("unable to get namespace", zap.Any("error", err))
		Error(w, err, http.StatusInternalServerError)
		return
	}

	resp := &admin.NamespaceGetResponse{
		Registry: &nsRegistry,
	}

	WriteProtoMsgJSONResponse(w, resp, logger)
}

func (h *namespaceGetHandler) namespaceGet(ctx context.Context) (nsproto.Registry, error) {
	var emptyReg = nsproto.Registry{}
	store, err := GetKV(h.clusterClient)
	if err != nil {
		return emptyReg, err
	}

	value, err := store.Get(M3DBNodeNamespacesKey)
	if err == kv.ErrNotFound {
		// Having no namespace should not be treated as an error
		return emptyReg, nil
	} else if err != nil {
		return emptyReg, err
	}

	var protoRegistry nsproto.Registry

	if err := value.Unmarshal(&protoRegistry); err != nil {
		return emptyReg, fmt.Errorf("failed to parse namespace version %v: %v", value.Version(), err)
	}
	return protoRegistry, nil
}
