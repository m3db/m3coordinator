package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

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

	defaultBlockDataExpiryPeriodStr = "5m"
)

// namespaceAddHandler represents a handler for placement add endpoint.
type namespaceAddHandler AdminHandler

// NewNamespaceAddHandler returns a new instance of handler.
func NewNamespaceAddHandler(clusterClient m3clusterClient.Client) http.Handler {
	return &namespaceAddHandler{
		clusterClient: clusterClient,
	}
}

func (h *namespaceAddHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)

	req, rErr := h.parseRequest(r)
	if rErr != nil {
		logger.Error("unable to parse request", zap.Any("error", rErr))
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

	WriteProtoMsgJSONResponse(w, resp, logger)
}

func (h *namespaceAddHandler) parseRequest(r *http.Request) (*admin.NamespaceAddRequest, *ParseError) {
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

func (h *namespaceAddHandler) namespaceAdd(ctx context.Context, r *admin.NamespaceAddRequest) (nsproto.Registry, error) {
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
	// Explicitly check existence of name. Other required fields are `time.Duration`s,
	// which will fail to parse as such on empty string.
	if r.Name == "" {
		return nil, errors.New("must specify namespace name")
	}
	blockSize, err := time.ParseDuration(r.BlockSize)
	if err != nil {
		return nil, err
	}
	retentionPeriod, err := time.ParseDuration(r.RetentionPeriod)
	if err != nil {
		return nil, err
	}
	bufferFuture, err := time.ParseDuration(r.BufferFuture)
	if err != nil {
		return nil, err
	}
	bufferPast, err := time.ParseDuration(r.BufferPast)
	if err != nil {
		return nil, err
	}
	blockDataExpiryPeriodStr := r.BlockDataExpiryPeriod
	if blockDataExpiryPeriodStr == "" {
		blockDataExpiryPeriodStr = defaultBlockDataExpiryPeriodStr
	}
	blockDataExpiryPeriod, err := time.ParseDuration(blockDataExpiryPeriodStr)
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
