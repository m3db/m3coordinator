package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m3db/m3coordinator/models"
	"github.com/m3db/m3coordinator/policy/resolver"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/storage/local"
	"github.com/m3db/m3coordinator/util/logging"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/storage/index"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func generateSearchReq() *storage.FetchQuery {
	matchers := models.Matchers{{
		Type:  models.MatchEqual,
		Name:  "foo",
		Value: "bar",
	},
		{
			Type:  models.MatchEqual,
			Name:  "biz",
			Value: "baz",
		},
	}
	return &storage.FetchQuery{
		TagMatchers: matchers,
		Start:       time.Now().Add(-10 * time.Minute),
		End:         time.Now(),
	}
}

func generateSearchBody(t *testing.T) io.Reader {
	req := generateSearchReq()
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatal("could not marshal json request")
	}

	return bytes.NewReader(data)
}

func generateQueryResults(t *testing.T, tagsIter index.TaggedIDsIter) index.QueryResults {
	return index.QueryResults{
		Iter: tagsIter,
	}
}

func generateTag() ident.Tag {
	return ident.Tag{
		Name:  ident.StringID("foo"),
		Value: ident.StringID("bar"),
	}
}

func searchServer(t *testing.T) *httptest.Server {
	logging.InitWithCores(nil)
	ctrl := gomock.NewController(t)

	mockReturnedTagIter := ident.NewMockTagIterator(ctrl)
	mockReturnedTagIter.EXPECT().Next().Return(true).MaxTimes(1)
	mockReturnedTagIter.EXPECT().Next().Return(false)
	mockReturnedTagIter.EXPECT().Current().Return(generateTag())
	mockReturnedTagIter.EXPECT().Close()
	mockReturnedTagIter.EXPECT().Remaining().Return(0)

	mockTagIter := index.NewMockTaggedIDsIter(ctrl)
	mockTagIter.EXPECT().Next().Return(true).MaxTimes(1)
	mockTagIter.EXPECT().Next().Return(false)
	mockTagIter.EXPECT().Current().Return(ident.StringID("test_id"), ident.StringID("test_namespace"), mockReturnedTagIter)

	session := client.NewMockSession(ctrl)
	session.EXPECT().FetchTaggedIDs(gomock.Any(), gomock.Any()).Return(generateQueryResults(t, mockTagIter), nil)

	storage := local.NewStorage(session, "metrics", resolver.NewStaticResolver(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour*48)))
	search := &SearchHandler{store: storage}

	server := httptest.NewServer(search)
	return server
}

func TestSearchResponse(t *testing.T) {
	logging.InitWithCores(nil)
	searchServer(t)
	ctrl := gomock.NewController(t)
	session := client.NewMockSession(ctrl)
	session.EXPECT().FetchTaggedIDs(gomock.Any(), gomock.Any()).Return(index.QueryResults{}, nil)

	storage := local.NewStorage(session, "metrics", resolver.NewStaticResolver(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour*48)))
	searchHandler := &SearchHandler{store: storage}
	_, err := searchHandler.search(context.TODO(), generateSearchReq(), newFetchOptions())
	require.NotNil(t, err, "unable to read from storage")
}

func TestSearchEndpoint(t *testing.T) {
	server := searchServer(t)
	defer server.Close()

	req, _ := http.NewRequest("POST", server.URL, generateSearchBody(t))
	req.Header.Add("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.NotNil(t, resp)
}
