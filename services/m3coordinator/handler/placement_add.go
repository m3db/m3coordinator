package handler

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/m3db/m3coordinator/generated/proto/admin"
	"github.com/m3db/m3coordinator/util/logging"

	m3clusterClient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/placement"

	"go.uber.org/zap"
)

const (
	// PlacementAddURL is the url for the placement add handler (with the POST method).
	PlacementAddURL = "/placement/add"
)

// placementAddHandler represents a handler for placement add endpoint.
type placementAddHandler AdminHandler

// NewPlacementAddHandler returns a new instance of handler.
func NewPlacementAddHandler(clusterClient m3clusterClient.Client) http.Handler {
	return &placementAddHandler{
		clusterClient: clusterClient,
	}
}

func (h *placementAddHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)

	req, rErr := h.parseRequest(r)
	if rErr != nil {
		Error(w, rErr.Error(), rErr.Code())
		return
	}

	placement, err := h.placementAdd(ctx, req)
	if err != nil {
		logger.Error("unable to add placement", zap.Any("error", err))
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

func (h *placementAddHandler) parseRequest(r *http.Request) (*admin.PlacementAddRequest, *ParseError) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, NewParseError(err, http.StatusBadRequest)
	}
	defer r.Body.Close()

	addReq := new(admin.PlacementAddRequest)
	if err := json.Unmarshal(body, addReq); err != nil {
		return nil, NewParseError(err, http.StatusBadRequest)
	}

	return addReq, nil
}

func (h *placementAddHandler) placementAdd(ctx context.Context, r *admin.PlacementAddRequest) (placement.Placement, error) {
	ps, err := PlacementService(h.clusterClient, h.config)
	if err != nil {
		return nil, err
	}

	instances, err := ConvertInstancesProto(r.Instances)
	if err != nil {
		return nil, err
	}

	newPlacement, _, err := ps.AddInstances(instances)
	if err != nil {
		return nil, err
	}

	return newPlacement, nil
}
