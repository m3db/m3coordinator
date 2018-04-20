package handler

import (
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/m3db/m3cluster/placement"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlacementRemoveHandler(t *testing.T) {
	mockClient, mockPlacementService := SetupPlacementTest(t)
	handler := NewPlacementRemoveHandler(mockClient)

	// Test remove success
	w := httptest.NewRecorder()
	jsonInput := `{"instance_ids": ["host1"]}`
	req := httptest.NewRequest("POST", "/placement/remove", strings.NewReader(jsonInput))
	require.NotNil(t, req)
	mockPlacementService.EXPECT().RemoveInstances([]string{"host1"}).Return(placement.NewPlacement(), nil)
	handler.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "{\"placement\":{\"instances\":{},\"replicaFactor\":0,\"numShards\":0,\"isSharded\":false,\"cutoverTime\":\"0\",\"isMirrored\":false,\"maxShardSetId\":0},\"version\":0}", string(body))

	// Test remove success with more than one instance
	w = httptest.NewRecorder()
	jsonInput = `{"instance_ids": ["host1", "host4", "host3"]}`
	req = httptest.NewRequest("POST", "/placement/remove", strings.NewReader(jsonInput))
	require.NotNil(t, req)
	mockPlacementService.EXPECT().RemoveInstances([]string{"host1", "host4", "host3"}).Return(placement.NewPlacement(), nil)
	handler.ServeHTTP(w, req)

	resp = w.Result()
	body, _ = ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "{\"placement\":{\"instances\":{},\"replicaFactor\":0,\"numShards\":0,\"isSharded\":false,\"cutoverTime\":\"0\",\"isMirrored\":false,\"maxShardSetId\":0},\"version\":0}", string(body))

	// Test remove failure
	w = httptest.NewRecorder()
	jsonInput = `{}`
	req = httptest.NewRequest("POST", "/placement/remove", strings.NewReader(jsonInput))
	require.NotNil(t, req)
	mockPlacementService.EXPECT().RemoveInstances([]string{}).Return(placement.NewPlacement(), errors.New("missing instance IDs"))
	handler.ServeHTTP(w, req)

	resp = w.Result()
	body, _ = ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Equal(t, "missing instance IDs\n", string(body))
}
