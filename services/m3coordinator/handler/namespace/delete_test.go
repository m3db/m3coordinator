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

package namespace

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
	mockKV, _ := SetupNamespaceTest(t)
	deleteHandler := NewDeleteHandler(mockKV)

	w := httptest.NewRecorder()
	jsonInput := `{"name": "not-present"}`

	req := httptest.NewRequest("POST", "/namespace/delete", strings.NewReader(jsonInput))
	require.NotNil(t, req)

	mockKV.EXPECT().Get(M3DBNodeNamespacesKey).Return(nil, kv.ErrNotFound)
	deleteHandler.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "unable to find a namespace with specified name\n", string(body))
}

func TestNamespaceDeleteHandlerDeleteAll(t *testing.T) {
	mockKV, ctrl := SetupNamespaceTest(t)
	deleteHandler := NewDeleteHandler(mockKV)

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
				BootstrapEnabled:  true,
				FlushEnabled:      true,
				WritesToCommitLog: true,
				CleanupEnabled:    false,
				RepairEnabled:     false,
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
	deleteHandler.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "", string(body))
}

func TestNamespaceDeleteHandler(t *testing.T) {
	mockKV, ctrl := SetupNamespaceTest(t)
	deleteHandler := NewDeleteHandler(mockKV)

	w := httptest.NewRecorder()
	jsonInput := `{"name": "testNamespace"}`

	req := httptest.NewRequest("POST", "/namespace/delete", strings.NewReader(jsonInput))
	require.NotNil(t, req)

	registry := nsproto.Registry{
		Namespaces: map[string]*nsproto.NamespaceOptions{
			"otherNamespace": &nsproto.NamespaceOptions{
				BootstrapEnabled:  true,
				FlushEnabled:      true,
				WritesToCommitLog: true,
				CleanupEnabled:    false,
				RepairEnabled:     false,
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
				BootstrapEnabled:  true,
				FlushEnabled:      true,
				WritesToCommitLog: true,
				CleanupEnabled:    false,
				RepairEnabled:     false,
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
	deleteHandler.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "", string(body))
}
