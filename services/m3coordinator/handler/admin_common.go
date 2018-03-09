package handler

import (
	m3clusterClient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/generated/proto/placementpb"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/services"
)

const (
	// DefaultServiceName is the default service ID name
	DefaultServiceName = "m3db"

	// DefaultServiceEnvironment is the default service ID environment
	DefaultServiceEnvironment = "production"

	// DefaultServiceZone is the default service ID zone
	DefaultServiceZone = "embedded"
)

// AdminHandler represents a generic handler for admin endpoints.
type AdminHandler struct {
	clusterClient m3clusterClient.Client
}

// GetPlacementServices gets a placement service from an m3cluster client
func GetPlacementServices(clusterClient m3clusterClient.Client) (placement.Service, error) {
	cs, err := clusterClient.Services(services.NewOptions())
	if err != nil {
		return nil, err
	}

	sid := services.NewServiceID().
		SetName(DefaultServiceName).
		SetEnvironment(DefaultServiceEnvironment).
		SetZone(DefaultServiceZone)

	ps, err := cs.PlacementService(sid, placement.NewOptions())
	if err != nil {
		return nil, err
	}

	return ps, nil
}

// ConvertInstancesProto converts a protobuf Instances to a placement.Instances
func ConvertInstancesProto(instancesProto []*placementpb.Instance) []placement.Instance {
	res := make([]placement.Instance, 0, len(instancesProto))

	for _, instanceProto := range instancesProto {
		instance := placement.NewInstance().
			SetEndpoint(instanceProto.Endpoint).
			SetHostname(instanceProto.Hostname).
			SetID(instanceProto.Id).
			SetPort(instanceProto.Port).
			SetRack(instanceProto.Rack).
			SetWeight(instanceProto.Weight).
			SetZone(instanceProto.Zone)

		res = append(res, instance)
	}

	return res
}
