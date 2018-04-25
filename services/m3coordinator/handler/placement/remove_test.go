// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package placement

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
	mockPlacementService := SetupPlacementTest(t)
	handler := NewRemoveHandler(mockPlacementService)

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
