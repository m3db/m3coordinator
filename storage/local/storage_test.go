package local

import (
	"context"
	"testing"
	"time"

	"github.com/m3db/m3coordinator/generated/proto/prometheus/prompb"
	"github.com/m3db/m3coordinator/models"

	"github.com/m3db/m3coordinator/policy/resolver"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/ts"
	"github.com/m3db/m3coordinator/util/logging"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3metrics/policy"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func setup() {
	logging.InitWithCores(nil)
	logger := logging.WithContext(context.TODO())
	defer logger.Sync()
}

func newWriteQuery() *storage.WriteQuery {
	datapoints := ts.Datapoints{{
		Timestamp: time.Now(),
		Value:     1.0,
	},
		{
			Timestamp: time.Now().Add(-10 * time.Second),
			Value:     2.0,
		}}

	labels := []*prompb.Label{
		&prompb.Label{
			Name:  "name",
			Value: "value",
		},
	}
	return &storage.WriteQuery{
		Tags:       models.PromLabelsToM3Tags(labels),
		Unit:       xtime.Millisecond,
		Datapoints: datapoints,
	}
}

func setupLocalWrite(t *testing.T) storage.Storage {
	setup()
	ctrl := gomock.NewController(t)
	session := client.NewMockSession(ctrl)
	session.EXPECT().WriteTagged(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
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
	writeQuery := newWriteQuery()
	err := store.Write(context.TODO(), writeQuery)
	assert.NoError(t, err)
}
