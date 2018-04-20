package handler

import (
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/m3db/m3cluster/placement"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlacementAddHandler(t *testing.T) {
	mockClient, mockPlacementService := SetupPlacementTest(t)
	handler := NewPlacementAddHandler(mockClient)

	// Test add failure
	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/placement/add", strings.NewReader("{\"instances\":[]}"))
	require.NotNil(t, req)

	mockPlacementService.EXPECT().AddInstances(gomock.Any()).Return(placement.NewPlacement(), nil, errors.New("no new instances found in the valid zone"))
	handler.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Equal(t, "no new instances found in the valid zone\n", string(body))

	// Test add success
	w = httptest.NewRecorder()
	req = httptest.NewRequest("POST", "/placement/add", strings.NewReader("{\"instances\":[{\"id\": \"host1\",\"isolation_group\": \"rack1\",\"zone\": \"test\",\"weight\": 1,\"endpoint\": \"http://host1:1234\",\"hostname\": \"host1\",\"port\": 1234}]}"))
	require.NotNil(t, req)

	mockPlacementService.EXPECT().AddInstances(gomock.Not(nil)).Return(placement.NewPlacement(), nil, nil)
	handler.ServeHTTP(w, req)

	resp = w.Result()
	body, _ = ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "{\"placement\":{\"instances\":{},\"replicaFactor\":0,\"numShards\":0,\"isSharded\":false,\"cutoverTime\":\"0\",\"isMirrored\":false,\"maxShardSetId\":0},\"version\":0}", string(body))
}
