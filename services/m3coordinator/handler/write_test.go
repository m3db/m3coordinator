package handler

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/m3db/m3coordinator/generated/proto/prometheus/prompb"
	"github.com/m3db/m3coordinator/policy/resolver"
	"github.com/m3db/m3coordinator/storage/local"
	"github.com/m3db/m3coordinator/util/logging"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3metrics/policy"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/stretchr/testify/require"
)

func generatePromWriteRequest() *prompb.WriteRequest {
	req := &prompb.WriteRequest{
		Timeseries: []*prompb.TimeSeries{{
			Labels: []*prompb.Label{
				{Name: "foo", Value: "bar"},
				{Name: "biz", Value: "baz"},
			},
			Samples: []*prompb.Sample{
				{Value: 1.0, Timestamp: time.Now().UnixNano() / int64(time.Millisecond)},
				{Value: 2.0, Timestamp: time.Now().UnixNano() / int64(time.Millisecond)},
			},
		},
			{
				Labels: []*prompb.Label{
					{Name: "foo", Value: "qux"},
					{Name: "bar", Value: "baz"},
				},
				Samples: []*prompb.Sample{
					{Value: 3.0, Timestamp: time.Now().UnixNano() / int64(time.Millisecond)},
					{Value: 4.0, Timestamp: time.Now().UnixNano() / int64(time.Millisecond)},
				},
			}},
	}
	return req
}

func generatePromWriteBody(t *testing.T) io.Reader {
	req := generatePromWriteRequest()
	data, err := proto.Marshal(req)
	if err != nil {
		t.Fatal("couldn't marshal prometheus request")
	}

	compressed := snappy.Encode(nil, data)
	b := bytes.NewReader(compressed)
	return b

}
func TestPromWriteParsing(t *testing.T) {
	logging.InitWithCores(nil)

	storage := local.NewStorage(nil, "metrics", resolver.NewStaticResolver(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour*48)))
	promWrite := &PromWriteHandler{store: storage}

	req, _ := http.NewRequest("POST", PromWriteURL, generatePromWriteBody(t))

	r, err := promWrite.parseRequest(req)
	require.Nil(t, err, "unable to parse request")
	require.Equal(t, len(r.Timeseries), 2)
}

func TestPromWrite(t *testing.T) {
	logging.InitWithCores(nil)

	ctrl := gomock.NewController(t)
	session := client.NewMockSession(ctrl)
	session.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

	storage := local.NewStorage(session, "metrics", resolver.NewStaticResolver(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour*48)))
	promWrite := &PromWriteHandler{store: storage}

	req, _ := http.NewRequest("POST", PromWriteURL, generatePromWriteBody(t))

	r, err := promWrite.parseRequest(req)
	require.Nil(t, err, "unable to parse request")

	writeErr := promWrite.write(context.TODO(), r)
	require.NoError(t, writeErr)
}

func TestRead(t *testing.T) {
	str := []byte{182, 3, 64, 10, 216, 1, 10, 18, 10, 8, 104, 111, 115, 116, 110, 97, 109, 101, 18, 6, 1, 10, 240, 76, 95, 48, 10, 20, 10, 2, 111, 115, 18, 14, 85, 98, 117, 110, 116, 117, 49, 54, 46, 48, 52, 76, 84, 83, 10, 10, 10, 4, 114, 97, 99, 107, 18, 2, 57, 49, 10, 19, 10, 6, 114, 101, 103, 105, 111, 110, 18, 9, 115, 97, 45, 101, 97, 115, 116, 45, 49, 10, 12, 10, 7, 115, 101, 114, 118, 105, 99, 101, 18, 1, 50, 10, 33, 10, 19, 115, 101, 5, 14, 108, 95, 101, 110, 118, 105, 114, 111, 110, 109, 101, 110, 116, 18, 10, 112, 114, 111, 100, 117, 99, 116, 105, 111, 110, 10, 20, 10, 15, 13, 49, 16, 95, 118, 101, 114, 115, 1, 79, 56, 1, 48, 10, 11, 10, 4, 116, 101, 97, 109, 18, 3, 67, 72, 73, 1, 13, 116, 97, 114, 99, 104, 18, 3, 120, 54, 52, 10, 24, 10, 10, 100, 97, 116, 97, 99, 101, 110, 116, 101, 114, 18, 10, 115, 97, 45, 101, 97, 1, 122, 84, 99, 18, 16, 9, 115, 228, 91, 198, 115, 203, 86, 64, 16, 152, 155, 247, 160, 156, 44, 10, 216, 1, 54, 60, 0, 138, 108, 0, 0, 12, 190, 192, 0, 98, 144, 0, 4, 10, 18, 254, 86, 1, 53, 86, 68, 18, 16, 9, 24, 108, 50, 229, 103, 247, 65, 64, 16, 152, 155, 247, 160, 156, 44}

	r, err := http.Post("http://localhost:7201/api/v1/prom/write", "", bytes.NewReader(str))
	require.NoError(t, err)
	require.NotNil(t, r)
	b := make([]byte, r.ContentLength)
	r.Body.Read(b)
	r.Body.Close()
	fmt.Println(string(b))

	_, err = snappy.Decode(nil, str)
	require.NoError(t, err)
	// fmt.Println(string(g))
	// fmt.Println(g)

}
