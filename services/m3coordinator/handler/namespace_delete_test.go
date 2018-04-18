package handler

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/m3db/m3cluster/kv"
	nsproto "github.com/m3db/m3db/generated/proto/namespace"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNamespaceDeleteHandlerNotFound(t *testing.T) {
	mockClient, mockKV, _ := SetupNamespaceTest(t)
	handler := NewNamespaceDeleteHandler(mockClient)

	w := httptest.NewRecorder()
	jsonInput := `{"name": "not-present"}`

	req := httptest.NewRequest("POST", "/namespace/delete", strings.NewReader(jsonInput))
	require.NotNil(t, req)

	mockKV.EXPECT().Get(M3DBNodeNamespacesKey).Return(nil, kv.ErrNotFound)
	handler.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "unable to find a namespace with specified name\n", string(body))
}

func TestNamespaceDeleteHandlerDeleteAll(t *testing.T) {
	mockClient, mockKV, ctrl := SetupNamespaceTest(t)
	handler := NewNamespaceDeleteHandler(mockClient)

	w := httptest.NewRecorder()
	jsonInput := `{"name": "testNamespace"}`

	req := httptest.NewRequest("POST", "/namespace/delete", strings.NewReader(jsonInput))
	require.NotNil(t, req)

	registry := nsproto.Registry{
		Namespaces: map[string]*nsproto.NamespaceOptions{
			"testNamespace": &nsproto.NamespaceOptions{
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
	mockKV.EXPECT().Delete(M3DBNodeNamespacesKey).Return(nil, nil)
	handler.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "", string(body))
}

func TestNamespaceDeleteHandler(t *testing.T) {
	mockClient, mockKV, ctrl := SetupNamespaceTest(t)
	handler := NewNamespaceDeleteHandler(mockClient)

	w := httptest.NewRecorder()
	jsonInput := `{"name": "testNamespace"}`

	req := httptest.NewRequest("POST", "/namespace/delete", strings.NewReader(jsonInput))
	require.NotNil(t, req)

	registry := nsproto.Registry{
		Namespaces: map[string]*nsproto.NamespaceOptions{
			"otherNamespace": &nsproto.NamespaceOptions{
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
			"testNamespace": &nsproto.NamespaceOptions{
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
	mockKV.EXPECT().Delete(M3DBNodeNamespacesKey).Return(nil, nil)
	mockKV.EXPECT().Set(M3DBNodeNamespacesKey, gomock.Any()).Return(1, nil)
	handler.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "", string(body))
}
