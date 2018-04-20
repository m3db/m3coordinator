package handler

import (
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"

	"github.com/m3db/m3coordinator/generated/proto/admin"
	"github.com/m3db/m3coordinator/util/logging"

	m3clusterClient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/placement"

	"go.uber.org/zap"
)

const (
	// PlacementRemoveURL is the url for the placement remove handler (with the POST method).
	PlacementRemoveURL = "/placement/remove"
)

var (
	errMissingInstanceIds = errors.New("missing instance IDs")
)

// placementRemoveHandler represents a handler for placement remove endpoint.
type placementRemoveHandler AdminHandler

// NewPlacementRemoveHandler returns a new instance of handler.
func NewPlacementRemoveHandler(clusterClient m3clusterClient.Client) http.Handler {
	return &placementRemoveHandler{
		clusterClient: clusterClient,
	}
}

func (h *placementRemoveHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)

	req, rErr := h.parseRequest(r)
	if rErr != nil {
		Error(w, rErr.Error(), rErr.Code())
		return
	}

	placement, err := h.placementRemove(ctx, req)
	if err != nil {
		logger.Error("unable to remove placement", zap.Any("error", err))
		Error(w, err, http.StatusInternalServerError)
		return
	}

	placementProto, err := placement.Proto()
	if err != nil {
		logger.Error("unable to get placement protobuf", zap.Any("error", err))
		Error(w, err, http.StatusInternalServerError)
		return
	}

	resp := &admin.PlacementGetResponse{
		Placement: placementProto,
	}

	WriteProtoMsgJSONResponse(w, resp, logger)
}

func (h *placementRemoveHandler) parseRequest(r *http.Request) (*admin.PlacementRemoveRequest, *ParseError) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, NewParseError(err, http.StatusBadRequest)
	}
	defer r.Body.Close()

	removeReq := new(admin.PlacementRemoveRequest)
	if err := json.Unmarshal(body, removeReq); err != nil {
		return nil, NewParseError(err, http.StatusBadRequest)
	}

	return removeReq, nil
}

func (h *placementRemoveHandler) placementRemove(ctx context.Context, r *admin.PlacementRemoveRequest) (placement.Placement, error) {
	if len(r.InstanceIds) == 0 {
		return nil, errMissingInstanceIds
	}

	ps, err := PlacementService(h.clusterClient, h.config)
	if err != nil {
		return nil, err
	}

	newPlacement, err := ps.RemoveInstances(r.InstanceIds)
	if err != nil {
		return nil, err
	}

	return newPlacement, nil
}
