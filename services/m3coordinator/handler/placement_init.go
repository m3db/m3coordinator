package handler

import (
	"context"
	"net/http"

	"github.com/golang/protobuf/proto"
	m3clusterClient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3coordinator/generated/proto/admin"
	"github.com/m3db/m3coordinator/util/logging"
	"go.uber.org/zap"
)

const (
	// PlacementInitURL is the url for the placement init handler.
	PlacementInitURL = "/api/v1/admin/placement/init"
)

// PlacementInitHandler represents a handler for placement init endpoint.
type PlacementInitHandler AdminHandler

// NewPlacementInitHandler returns a new instance of handler.
func NewPlacementInitHandler(clusterClient m3clusterClient.Client) http.Handler {
	return &PlacementInitHandler{
		clusterClient: clusterClient,
	}
}

func (h *PlacementInitHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	req, rErr := h.parseRequest(r)
	if rErr != nil {
		Error(w, rErr.Error(), rErr.Code())
		return
	}

	ctx := r.Context()
	logger := logging.WithContext(ctx)

	placement, err := h.placementInit(ctx, req)
	if err != nil {
		logger.Error("unable to initialize placement", zap.Any("error", err))
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

	if err := WriteMessageResponse(w, resp, logger); err != nil {
		Error(w, err, http.StatusInternalServerError)
	}
}

func (h *PlacementInitHandler) parseRequest(r *http.Request) (*admin.PlacementInitRequest, *ParseError) {
	reqBuf, err := ParsePromRequest(r)
	if err != nil {
		return nil, err
	}

	var req admin.PlacementInitRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		return nil, NewParseError(err, http.StatusBadRequest)
	}

	return &req, nil
}

func (h *PlacementInitHandler) placementInit(ctx context.Context, r *admin.PlacementInitRequest) (placement.Placement, error) {
	ps, err := GetPlacementServices(h.clusterClient)
	if err != nil {
		return nil, err
	}

	instances := ConvertInstancesProto(r.Instances)

	placement, err := ps.BuildInitialPlacement(instances, int(r.NumShards), int(r.ReplicationFactor))
	if err != nil {
		return nil, err
	}

	return placement, nil
}
