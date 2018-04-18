package handler

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/m3db/m3coordinator/util/logging"

	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/kv"
	nsproto "github.com/m3db/m3db/generated/proto/namespace"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func SetupNamespaceTest(t *testing.T) (*client.MockClient, *kv.MockStore, *gomock.Controller) {
	logging.InitWithCores(nil)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockClient := client.NewMockClient(ctrl)
	require.NotNil(t, mockClient)
	mockKV := kv.NewMockStore(ctrl)
	require.NotNil(t, mockKV)
	mockClient.EXPECT().KV().Return(mockKV, nil).AnyTimes()

	return mockClient, mockKV, ctrl
}

func TestNamespaceGetHandler(t *testing.T) {
	mockClient, mockKV, ctrl := SetupNamespaceTest(t)
	handler := NewNamespaceGetHandler(mockClient)

	// Test no namespace
	w := httptest.NewRecorder()

	req := httptest.NewRequest("GET", "/namespace/get", nil)
	require.NotNil(t, req)

	mockKV.EXPECT().Get(M3DBNodeNamespacesKey).Return(nil, kv.ErrNotFound)
	handler.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "{\"registry\":{\"namespaces\":{}}}", string(body))

	// Test namespace present
	w = httptest.NewRecorder()

	req = httptest.NewRequest("GET", "/namespace/get", nil)
	require.NotNil(t, req)

	registry := nsproto.Registry{
		Namespaces: map[string]*nsproto.NamespaceOptions{
			"test": &nsproto.NamespaceOptions{
				NeedsBootstrap:      true,
				NeedsFlush:          true,
				WritesToCommitLog:   true,
				NeedsFilesetCleanup: false,
				NeedsRepair:         false,
				RetentionOptions: &nsproto.RetentionOptions{
					RetentionPeriodNanos:                     172800000000000,
					BlockSizeNanos:                           7200000000000,
					BufferFutureNanos:                        600000000000,
					BufferPastNanos:                          600000000000,
					BlockDataExpiry:                          true,
					BlockDataExpiryAfterNotAccessPeriodNanos: 3600000000000,
				},
			},
		},
	}

	mockValue := kv.NewMockValue(ctrl)
	mockValue.EXPECT().Unmarshal(gomock.Any()).Return(nil).SetArg(0, registry)

	mockKV.EXPECT().Get(M3DBNodeNamespacesKey).Return(mockValue, nil)
	handler.ServeHTTP(w, req)

	resp = w.Result()
	body, _ = ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "{\"registry\":{\"namespaces\":{\"test\":{\"needsBootstrap\":true,\"needsFlush\":true,\"writesToCommitLog\":true,\"needsFilesetCleanup\":false,\"needsRepair\":false,\"retentionOptions\":{\"retentionPeriodNanos\":\"172800000000000\",\"blockSizeNanos\":\"7200000000000\",\"bufferFutureNanos\":\"600000000000\",\"bufferPastNanos\":\"600000000000\",\"blockDataExpiry\":true,\"blockDataExpiryAfterNotAccessPeriodNanos\":\"3600000000000\"}}}}}", string(body))
}
