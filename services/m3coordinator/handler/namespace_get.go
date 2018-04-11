package handler

import (
	"context"
	"fmt"
	"net/http"

	m3clusterClient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3coordinator/generated/proto/admin"
	"github.com/m3db/m3coordinator/util/logging"
	nsproto "github.com/m3db/m3db/generated/proto/namespace"
	"go.uber.org/zap"
)

const (
	// NamespaceGetURL is the url for the placement get handler (with the GET method).
	NamespaceGetURL = "/namespace/get"

	// NamespaceGetHTTPMethodURL is the url for the placement get handler (with the GET method).
	NamespaceGetHTTPMethodURL = "/namespace"
)

// NamespaceGetHandler represents a handler for placement get endpoint.
type NamespaceGetHandler AdminHandler

// NewNamespaceGetHandler returns a new instance of handler.
func NewNamespaceGetHandler(clusterClient m3clusterClient.Client) http.Handler {
	return &NamespaceGetHandler{
		clusterClient: clusterClient,
	}
}

func (h *NamespaceGetHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
	WriteJSONResponse(w, resp, logger)
}

func (h *NamespaceGetHandler) namespaceGet(ctx context.Context) (nsproto.Registry, error) {
	var emptyReg = nsproto.Registry{}
	kv, err := GetKV(h.clusterClient)
	if err != nil {
		return emptyReg, err
	}
	value, err := kv.Get(M3DBNodeNamespacesKey)
	if err != nil {
		return emptyReg, err
	}
	var protoRegistry nsproto.Registry
	if err := value.Unmarshal(&protoRegistry); err != nil {
		return emptyReg, fmt.Errorf("failed to parse namespace version %v: %v", value.Version(), err)
	}
	return protoRegistry, nil
}
