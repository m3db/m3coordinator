package handler

import (
	"context"
	"net/http"

	m3clusterClient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3coordinator/util/logging"
	"go.uber.org/zap"
)

const (
	// PlacementDeleteURL is the url for the placement delete handler (with the POST method).
	PlacementDeleteURL = "/placement/delete"

	// PlacementDeleteHTTPMethodURL is another url for the placement delete handler (with the DELETE method).
	PlacementDeleteHTTPMethodURL = "/placement"
)

// PlacementDeleteHandler represents a handler for placement delete endpoint.
type PlacementDeleteHandler AdminHandler

// NewPlacementDeleteHandler returns a new instance of handler.
func NewPlacementDeleteHandler(clusterClient m3clusterClient.Client) http.Handler {
	return &PlacementDeleteHandler{
		clusterClient: clusterClient,
	}
}

func (h *PlacementDeleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)

	if err := h.placementDelete(ctx); err != nil {
		logger.Error("unable to delete placement", zap.Any("error", err))
		Error(w, err, http.StatusInternalServerError)
		return
	}
}

func (h *PlacementDeleteHandler) placementDelete(ctx context.Context) error {
	ps, err := GetPlacementServices(h.clusterClient)
	if err != nil {
		return err
	}

	if err := ps.Delete(); err != nil {
		return err
	}

	return nil
}
