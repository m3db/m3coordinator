package handler

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/m3db/m3cluster/generated/proto/placementpb"

	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3coordinator/util/logging"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlacementInitHandler(t *testing.T) {
	logging.InitWithCores(nil)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockClient := client.NewMockClient(ctrl)
	require.NotNil(t, mockClient)
	mockServices := services.NewMockServices(ctrl)
	require.NotNil(t, mockServices)
	mockPlacementService := placement.NewMockService(ctrl)
	require.NotNil(t, mockPlacementService)

	mockClient.EXPECT().Services(gomock.Not(nil)).Return(mockServices, nil).AnyTimes()
	mockServices.EXPECT().PlacementService(gomock.Not(nil), gomock.Not(nil)).Return(mockPlacementService, nil).AnyTimes()

	handler := NewPlacementInitHandler(mockClient)

	// Test placement init success
	w := httptest.NewRecorder()

	// Actual JSON passed in does not matter because we're mocking the response out from the PlacementService
	req := httptest.NewRequest("POST", "/placement/init", strings.NewReader("{}"))
	require.NotNil(t, req)

	mockPlacementService.EXPECT().BuildInitialPlacement(gomock.Not(nil), gomock.Any(), gomock.Any()).Return(placement.NewPlacement(), nil)
	handler.ServeHTTP(w, req)
	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "{\"placement\":{}}", string(body))

	// Test placement init success
	w = httptest.NewRecorder()

	req = httptest.NewRequest("POST", "/placement/init", strings.NewReader("{}"))
	require.NotNil(t, req)

	placementProto := &placementpb.Placement{
		Instances: map[string]*placementpb.Instance{
			"host1": &placementpb.Instance{
				Id:             "host1",
				IsolationGroup: "rack1",
				Zone:           "test",
				Weight:         1,
				Endpoint:       "http://host1:1234",
				Hostname:       "host1",
				Port:           1234,
			},
			"host2": &placementpb.Instance{
				Id:             "host2",
				IsolationGroup: "rack1",
				Zone:           "test",
				Weight:         1,
				Endpoint:       "http://host2:1234",
				Hostname:       "host2",
				Port:           1234,
			},
		},
	}
	newPlacement, err := placement.NewPlacementFromProto(placementProto)
	require.NoError(t, err)

	mockPlacementService.EXPECT().BuildInitialPlacement(gomock.Not(nil), gomock.Any(), gomock.Any()).Return(newPlacement, nil)
	handler.ServeHTTP(w, req)
	resp = w.Result()
	body, _ = ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "{\"placement\":{\"instances\":{\"host1\":{\"id\":\"host1\",\"isolation_group\":\"rack1\",\"zone\":\"test\",\"weight\":1,\"endpoint\":\"http://host1:1234\",\"hostname\":\"host1\",\"port\":1234},\"host2\":{\"id\":\"host2\",\"isolation_group\":\"rack1\",\"zone\":\"test\",\"weight\":1,\"endpoint\":\"http://host2:1234\",\"hostname\":\"host2\",\"port\":1234}}}}", string(body))
}
