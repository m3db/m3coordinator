package handler

import (
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/m3db/m3cluster/generated/proto/placementpb"
	"github.com/m3db/m3cluster/placement"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlacementInitHandler(t *testing.T) {
	mockClient, mockPlacementService := SetupPlacementTest(t)
	handler := NewPlacementInitHandler(mockClient)

	// Test placement init success
	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/placement/init", strings.NewReader("{\"instances\": [{\"id\": \"host1\",\"isolation_group\": \"rack1\",\"zone\": \"test\",\"weight\": 1,\"endpoint\": \"http://host1:1234\",\"hostname\": \"host1\",\"port\": 1234},{\"id\": \"host2\",\"isolation_group\": \"rack1\",\"zone\": \"test\",\"weight\": 1,\"endpoint\": \"http://host2:1234\",\"hostname\": \"host2\",\"port\": 1234}],\"num_shards\": 16,\"replication_factor\": 1}"))
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

	mockPlacementService.EXPECT().BuildInitialPlacement(gomock.Not(nil), 16, 1).Return(newPlacement, nil)
	handler.ServeHTTP(w, req)
	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "{\"placement\":{\"instances\":{\"host1\":{\"id\":\"host1\",\"isolationGroup\":\"rack1\",\"zone\":\"test\",\"weight\":1,\"endpoint\":\"http://host1:1234\",\"shards\":[],\"shardSetId\":0,\"hostname\":\"host1\",\"port\":1234},\"host2\":{\"id\":\"host2\",\"isolationGroup\":\"rack1\",\"zone\":\"test\",\"weight\":1,\"endpoint\":\"http://host2:1234\",\"shards\":[],\"shardSetId\":0,\"hostname\":\"host2\",\"port\":1234}},\"replicaFactor\":0,\"numShards\":0,\"isSharded\":false,\"cutoverTime\":\"0\",\"isMirrored\":false,\"maxShardSetId\":0},\"version\":0}", string(body))

	// Test error response
	w = httptest.NewRecorder()
	req = httptest.NewRequest("POST", "/placement/init", strings.NewReader("{\"instances\": [{\"id\": \"host1\",\"isolation_group\": \"rack1\",\"zone\": \"test\",\"weight\": 1,\"endpoint\": \"host1:1234\",\"hostname\": \"host1\",\"port\": 1234},{\"id\": \"host2\",\"isolation_group\": \"rack1\",\"zone\": \"test\",\"weight\": 1,\"endpoint\": \"http://host2:1234\",\"hostname\": \"host2\",\"port\": 1234}],\"num_shards\": 64,\"replication_factor\": 2}"))
	require.NotNil(t, req)

	mockPlacementService.EXPECT().BuildInitialPlacement(gomock.Not(nil), 64, 2).Return(nil, errors.New("unable to build initial placement"))
	handler.ServeHTTP(w, req)
	resp = w.Result()
	body, err = ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Equal(t, "unable to build initial placement\n", string(body))
}
