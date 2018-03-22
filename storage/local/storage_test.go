package local

import (
	"context"
	"testing"
	"time"

	"github.com/m3db/m3coordinator/services/m3coordinator/options"

	"github.com/m3db/m3coordinator/errors"
	"github.com/m3db/m3coordinator/generated/proto/prometheus/prompb"
	"github.com/m3db/m3coordinator/models"
	"github.com/m3db/m3coordinator/models/m3tag"
	"github.com/m3db/m3coordinator/policy/resolver"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/ts"
	"github.com/m3db/m3coordinator/util/logging"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/storage/index"
	m3ts "github.com/m3db/m3db/ts"
	"github.com/m3db/m3metrics/policy"
	xcontext "github.com/m3db/m3x/context"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func setup() {
	logging.InitWithCores(nil)
	logger := logging.WithContext(context.TODO())
	defer logger.Sync()
}

func newFetchReq() *storage.FetchQuery {
	matchers := models.Matchers{
		{
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

func newWriteQuery(ctx xcontext.Context, opts options.Options) *storage.WriteQuery {
	datapoints := ts.Datapoints{{
		Timestamp: time.Now(),
		Value:     1.0,
	},
		{
			Timestamp: time.Now().Add(-10 * time.Second),
		}}

	labels := models.PrometheusLabels{
		&prompb.Label{
			Name:  "name",
			Value: "value",
		},
	}
	return &storage.WriteQuery{
		Tags:       m3tag.PromLabelsToM3Tags(ctx, opts, labels),
		Unit:       xtime.Millisecond,
		Datapoints: datapoints,
	}
}

func newMockSeriesIter(ctrl *gomock.Controller) encoding.SeriesIterator {
	mockIter := encoding.NewMockSeriesIterator(ctrl)
	mockIter.EXPECT().Next().Return(true).MaxTimes(1)
	mockIter.EXPECT().Next().Return(false)
	mockIter.EXPECT().Current().Return(m3ts.Datapoint{Timestamp: time.Now(), Value: 10}, xtime.Millisecond, nil)
	mockIter.EXPECT().Close()

	return mockIter
}

func setupLocalWrite(t *testing.T) storage.Storage {
	setup()
	ctrl := gomock.NewController(t)
	session := client.NewMockSession(ctrl)
	session.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	store := NewStorage(session, "metrics", resolver.NewStaticResolver(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour*48)))
	return store
}

func TestLocalWriteEmpty(t *testing.T) {
	store := setupLocalWrite(t)
	err := store.Write(context.TODO(), nil)
	assert.Error(t, err)
}

func TestLocalWriteSuccess(t *testing.T) {
	store := setupLocalWrite(t)
	opts := options.NewOptions()
	ctx := opts.ContextPool().Get()
	defer ctx.Close()
	writeQuery := newWriteQuery(ctx, opts)
	err := store.Write(context.TODO(), writeQuery)
	assert.NoError(t, err)
}

func setupLocalRead(t *testing.T) storage.Storage {
	setup()
	ctrl := gomock.NewController(t)
	session := client.NewMockSession(ctrl)
	session.EXPECT().Fetch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(newMockSeriesIter(ctrl), nil)
	store := NewStorage(session, "metrics", resolver.NewStaticResolver(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour*48)))
	return store
}

func TestLocalRead(t *testing.T) {
	store := setupLocalRead(t)
	searchReq := newFetchReq()
	results, err := store.Fetch(context.TODO(), searchReq, &storage.FetchOptions{Limit: 100})
	assert.NoError(t, err)
	assert.Equal(t, models.Tags{"foo": "bar", "biz": "baz"}, results.SeriesList[0].Tags)
}

func setupLocalSearch(t *testing.T) storage.Storage {
	setup()
	ctrl := gomock.NewController(t)
	session := client.NewMockSession(ctrl)
	session.EXPECT().FetchTaggedIDs(gomock.Any(), gomock.Any()).Return(index.QueryResults{}, errors.ErrNotImplemented)
	store := NewStorage(session, "metrics", resolver.NewStaticResolver(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour*48)))
	return store
}

func TestLocalSearchExpectedFail(t *testing.T) {
	store := setupLocalSearch(t)
	searchReq := newFetchReq()
	_, err := store.FetchTags(context.TODO(), searchReq, &storage.FetchOptions{Limit: 100})
	assert.Error(t, err)
}
