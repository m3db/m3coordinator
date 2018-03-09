package handler

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"go.uber.org/zap"
)

const (
	maxTimeout     = time.Minute
	defaultTimeout = time.Second * 15
)

// RequestParams are the arguments to a call
type RequestParams struct {
	Timeout time.Duration
}

// ParsePromRequest parses a snappy compressed request from Prometheus
func ParsePromRequest(r *http.Request) ([]byte, *ParseError) {
	body := r.Body
	if r.Body == nil {
		err := fmt.Errorf("empty request body")
		return nil, NewParseError(err, http.StatusBadRequest)
	}
	defer body.Close()
	compressed, err := ioutil.ReadAll(body)

	if err != nil {
		return nil, NewParseError(err, http.StatusInternalServerError)
	}

	if len(compressed) == 0 {
		return nil, NewParseError(fmt.Errorf("empty request body"), http.StatusBadRequest)
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, NewParseError(err, http.StatusBadRequest)
	}

	return reqBuf, nil
}

// ParseRequestParams parses the input request parameters and provides useful defaults
func ParseRequestParams(r *http.Request) (*RequestParams, error) {
	var params RequestParams
	timeout := r.Header.Get("timeout")
	if timeout != "" {
		duration, err := time.ParseDuration(timeout)
		if err != nil {
			return nil, fmt.Errorf("%s: invalid 'timeout': %v", ErrInvalidParams, err)

		}

		if duration > maxTimeout {
			return nil, fmt.Errorf("%s: invalid 'timeout': greater than %v", ErrInvalidParams, maxTimeout)

		}

		params.Timeout = duration
	} else {
		params.Timeout = defaultTimeout
	}

	return &params, nil
}

// WriteMessageResponse writes a protobuf message to the ResponseWriter
func WriteMessageResponse(w http.ResponseWriter, message proto.Message, logger *zap.Logger) error {
	data, err := proto.Marshal(message)
	if err != nil {
		logger.Error("unable to marshal read results to protobuf", zap.Any("error", err))
		return err
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")

	compressed := snappy.Encode(nil, data)
	if _, err := w.Write(compressed); err != nil {
		logger.Error("unable to encode read results to snappy", zap.Any("err", err))
		return err
	}

	return nil
}
