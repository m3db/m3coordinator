package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/m3db/m3coordinator/generated/proto/admin"
	"github.com/m3db/m3coordinator/util/logging"

	m3clusterClient "github.com/m3db/m3cluster/client"
	nsproto "github.com/m3db/m3db/generated/proto/namespace"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3x/ident"

	"go.uber.org/zap"
)

const (
	// NamespaceAddURL is the url for the placement add handler (with the POST method).
	NamespaceAddURL = "/namespace/add"
)

// NamespaceAddHandler represents a handler for placement add endpoint.
type NamespaceAddHandler AdminHandler

// NewNamespaceAddHandler returns a new instance of handler.
func NewNamespaceAddHandler(clusterClient m3clusterClient.Client) http.Handler {
	return &NamespaceAddHandler{
		clusterClient: clusterClient,
	}
}

func (h *NamespaceAddHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)

	req, rErr := h.parseRequest(r)
	if rErr != nil {
		Error(w, rErr.Error(), rErr.Code())
		return
	}

	nsRegistry, err := h.namespaceAdd(ctx, req)
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

func (h *NamespaceAddHandler) parseRequest(r *http.Request) (*admin.NamespaceAddRequest, *ParseError) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, NewParseError(err, http.StatusBadRequest)
	}
	defer r.Body.Close()

	addReq := new(admin.NamespaceAddRequest)
	if err := json.Unmarshal(body, addReq); err != nil {
		return nil, NewParseError(err, http.StatusBadRequest)
	}

	return addReq, nil
}

func (h *NamespaceAddHandler) namespaceAdd(ctx context.Context, r *admin.NamespaceAddRequest) (nsproto.Registry, error) {
	var emptyReg = nsproto.Registry{}
	kv, err := GetKV(h.clusterClient)
	if err != nil {
		return emptyReg, err
	}

	currentMetadata, err := currentNamespaceMetadata(kv)
	if err != nil {
		return emptyReg, err
	}

	inputMetadata, err := metadataFromRequest(r)
	if err != nil {
		return emptyReg, err
	}

	nsMap, err := namespace.NewMap(append(currentMetadata, inputMetadata))
	if err != nil {
		return emptyReg, err
	}

	protoRegistry := namespace.ToProto(nsMap)
	version, err := kv.Set(M3DBNodeNamespacesKey, protoRegistry)
	if err != nil {
		return emptyReg, fmt.Errorf("failed to add namespace version %v: %v", version, err)
	}

	return *protoRegistry, nil
}

func metadataFromRequest(r *admin.NamespaceAddRequest) (namespace.Metadata, error) {
	blockSize, err := parseDurationWithDefault(r.BlockSize, "")
	if err != nil {
		return nil, err
	}
	retentionPeriod, err := parseDurationWithDefault(r.RetentionPeriod, "")
	if err != nil {
		return nil, err
	}
	bufferFuture, err := parseDurationWithDefault(r.BufferFuture, "")
	if err != nil {
		return nil, err
	}
	bufferPast, err := parseDurationWithDefault(r.BufferPast, "")
	if err != nil {
		return nil, err
	}
	blockDataExpiryPeriod, err := parseDurationWithDefault(r.BlockDataExpiryPeriod, "5m")
	if err != nil {
		return nil, err
	}

	ropts := retention.NewOptions().
		SetBlockSize(blockSize).
		SetRetentionPeriod(retentionPeriod).
		SetBufferFuture(bufferFuture).
		SetBufferPast(bufferPast).
		SetBlockDataExpiry(r.BlockDataExpiry).
		SetBlockDataExpiryAfterNotAccessedPeriod(blockDataExpiryPeriod)

	if err := ropts.Validate(); err != nil {
		return nil, err
	}

	opts := namespace.NewOptions().
		SetNeedsBootstrap(r.NeedsBootstrap).
		SetNeedsFlush(r.NeedsFlush).
		SetNeedsFilesetCleanup(r.NeedsFilesetCleanup).
		SetNeedsRepair(r.NeedsRepair).
		SetWritesToCommitLog(r.WritesToCommitlog).
		SetRetentionOptions(ropts)

	return namespace.NewMetadata(ident.StringID(r.Name), opts)
}
