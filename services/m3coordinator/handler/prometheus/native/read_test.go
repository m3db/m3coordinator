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

package native

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m3db/m3coordinator/executor"
	"github.com/m3db/m3coordinator/mocks"
	"github.com/m3db/m3coordinator/policy/resolver"
	"github.com/m3db/m3coordinator/services/m3coordinator/handler/prometheus"
	"github.com/m3db/m3coordinator/storage/local"
	"github.com/m3db/m3coordinator/tsdb"
	"github.com/m3db/m3coordinator/util/logging"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3metrics/policy"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/golang/snappy"
	"github.com/stretchr/testify/require"
)

var (
	promQuery = `http_requests_total{job="prometheus",group="canary"}`
)

func generatePromReadBody(t *testing.T) io.Reader {
	compressed := snappy.Encode(nil, []byte(promQuery))
	b := bytes.NewReader(compressed)
	return b
}

func setupServer(t *testing.T) *httptest.Server {
	logging.InitWithCores(nil)
	ctrl := gomock.NewController(t)
	// No calls expected on session object
	session := client.NewMockSession(ctrl)
	mockResolver := mocks.NewMockPolicyResolver(gomock.NewController(t))

	mockResolver.EXPECT().Resolve(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Do(func(_, _, _, _ interface{}) {
		time.Sleep(100 * time.Millisecond)
	}).Return([]tsdb.FetchRequest{}, nil)

	storage := local.NewStorage(session, "metrics", mockResolver)
	engine := executor.NewEngine(storage)
	promRead := &PromReadHandler{engine: engine}

	server := httptest.NewServer(promRead)
	return server
}

func TestPromReadParsing(t *testing.T) {
	logging.InitWithCores(nil)
	storage := local.NewStorage(nil, "metrics", resolver.NewStaticResolver(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour*48)))
	promRead := &PromReadHandler{engine: executor.NewEngine(storage)}
	req, _ := http.NewRequest("POST", PromReadURL, generatePromReadBody(t))

	r, err := promRead.parseRequest(req)
	require.Nil(t, err, "unable to parse request")
	require.Equal(t, promQuery, r)
}

func TestPromReadNotImplemented(t *testing.T) {
	logging.InitWithCores(nil)
	storage := local.NewStorage(nil, "metrics", resolver.NewStaticResolver(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour*48)))
	promRead := &PromReadHandler{engine: executor.NewEngine(storage)}
	req, _ := http.NewRequest("POST", PromReadURL, generatePromReadBody(t))

	r, parseErr := promRead.parseRequest(req)
	require.Nil(t, parseErr, "unable to parse request")
	_, err := promRead.read(context.TODO(), httptest.NewRecorder(), r, &prometheus.RequestParams{Timeout: time.Hour})
	require.NotNil(t, err, "not implemented")
}
