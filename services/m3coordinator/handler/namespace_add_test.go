package handler

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3coordinator/util/logging"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNamespaceAddHandler(t *testing.T) {
	logging.InitWithCores(nil)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockClient := client.NewMockClient(ctrl)
	require.NotNil(t, mockClient)
	mockKV := kv.NewMockTxnStore(ctrl)
	require.NotNil(t, mockKV)
	mockPlacementService := placement.NewMockService(ctrl)
	require.NotNil(t, mockPlacementService)

	mockClient.EXPECT().KV().Return(mockKV, nil).AnyTimes()

	handler := NewNamespaceAddHandler(mockClient)
	w := httptest.NewRecorder()

	jsonInput := `
		{
			"name": "testNamespace",
			"retention_period": "48h",
			"block_size": "2h",
			"buffer_future": "10m",
			"buffer_past": "5m"
		}
	`

	req := httptest.NewRequest("POST", "/namespace/add", strings.NewReader(jsonInput))
	require.NotNil(t, req)

	mockKV.EXPECT().Get(M3DBNodeNamespacesKey).Return(nil, kv.ErrNotFound)
	mockKV.EXPECT().Set(M3DBNodeNamespacesKey, gomock.Not(nil)).Return(1, nil)
	handler.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "{\"registry\":{\"namespaces\":{\"testNamespace\":{\"retentionOptions\":{\"retentionPeriodNanos\":172800000000000,\"blockSizeNanos\":7200000000000,\"bufferFutureNanos\":600000000000,\"bufferPastNanos\":300000000000,\"blockDataExpiryAfterNotAccessPeriodNanos\":300000000000}}}}}", string(body))
}
