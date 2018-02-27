package common

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/m3db/m3coordinator/generated/proto/prometheus/prompb"
	"github.com/m3db/m3coordinator/storage"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
)

const (
	// MetricsLen is used to create the objects that store the parsed metrics
	MetricsLen = 100000
)

// Metrics is the OpenTSDB style metrics
type Metrics struct {
	Name  string            `json:"metric"`
	Time  int64             `json:"timestamp"`
	Tags  map[string]string `json:"tags"`
	Value float64           `json:"value"`
}

// M3Metric is a lighterweight Metrics struct
type M3Metric struct {
	ID    string
	Time  time.Time
	Value float64
}

// ConvertToM3 parses the json file that is generated from InfluxDB's bulk_data_gen tool
func ConvertToM3(fileName string, workers int, f func(*M3Metric)) {
	metricChannel := make(chan *M3Metric, MetricsLen)
	dataChannel := make(chan []byte, MetricsLen)
	wg := new(sync.WaitGroup)
	workFunction := func() {
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go unmarshalMetrics(dataChannel, metricChannel, wg)
		}
		go func() {
			for metric := range metricChannel {
				f(metric)
			}
		}()
	}
	cleanup := func() {
		wg.Wait()
		close(metricChannel)
	}

	convertToGeneric(fileName, workers, dataChannel, workFunction, cleanup)
}

// ConvertToProm parses the json file that is generated from InfluxDB's bulk_data_gen tool into Prom format
func ConvertToProm(fileName string, workers int, batchSize int, f func(*bytes.Reader)) {
	metricChannel := make(chan *bytes.Reader, MetricsLen)
	dataChannel := make(chan []byte, MetricsLen)
	wg := new(sync.WaitGroup)
	workFunction := func() {
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go marshalTsdbToProm(dataChannel, metricChannel, batchSize, wg)
		}
		go func() {
			for metric := range metricChannel {
				f(metric)
			}
		}()
	}
	cleanup := func() {
		wg.Wait()
		close(metricChannel)
	}
	convertToGeneric(fileName, workers, dataChannel, workFunction, cleanup)
}

func convertToGeneric(fileName string, workers int, dataChannel chan<- []byte, workFunction func(), cleanup func()) {
	fd, err := os.OpenFile(fileName, os.O_RDONLY, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to read json file, got error: %v", err)
		os.Exit(1)
	}

	defer fd.Close()

	scanner := bufio.NewScanner(fd)
	workFunction()

	for scanner.Scan() {
		data := bytes.TrimSpace(scanner.Bytes())
		b := make([]byte, len(data))
		copy(b, data)
		dataChannel <- b
	}

	close(dataChannel)

	cleanup()
	if err := scanner.Err(); err != nil {
		panic(err)
	}
}

func unmarshalMetrics(dataChannel <-chan []byte, metricChannel chan<- *M3Metric, wg *sync.WaitGroup) {
	defer wg.Done()
	for data := range dataChannel {
		if len(data) != 0 {
			var m Metrics
			if err := json.Unmarshal(data, &m); err != nil {
				panic(err)
			}

			metricChannel <- &M3Metric{ID: id(m.Tags, m.Name), Time: storage.TimestampToTime(m.Time), Value: m.Value}
		}
	}
}

func id(lowerCaseTags map[string]string, name string) string {
	sortedKeys := make([]string, len(lowerCaseTags))
	var buffer = bytes.NewBuffer(nil)
	buffer.WriteString(strings.ToLower(name))

	// Generate tags in alphabetical order & write to buffer
	i := 0
	for key := range lowerCaseTags {
		sortedKeys = append(sortedKeys, key)
		i++
	}
	sort.Strings(sortedKeys)

	for i = 0; i < len(sortedKeys)-1; i++ {
		buffer.WriteString(sortedKeys[i])
		buffer.WriteString(lowerCaseTags[sortedKeys[i]])
	}

	return buffer.String()
}

func marshalTsdbToProm(dataChannel <-chan []byte, metricChannel chan<- *bytes.Reader, batchSize int, wg *sync.WaitGroup) {
	defer wg.Done()
	timeseries := make([]*prompb.TimeSeries, batchSize)
	idx := 0
	for data := range dataChannel {
		if len(data) != 0 {
			var m Metrics
			if err := json.Unmarshal(data, &m); err != nil {
				panic(err)
			}
			labels := metricsTagsToLabels(m.Tags)
			samples := metricsPointsToSamples(m.Value, m.Time)
			timeseries[idx] = &prompb.TimeSeries{
				Labels:  labels,
				Samples: samples,
			}
			idx++
			if idx == batchSize {
				metricChannel <- encodeWriteRequest(timeseries)
				idx = 0
			}
		}
	}
	if idx > 0 {
		// Send the remaining series
		metricChannel <- encodeWriteRequest(timeseries[:idx])
	}
}

func encodeWriteRequest(ts []*prompb.TimeSeries) *bytes.Reader {
	req := &prompb.WriteRequest{
		Timeseries: ts,
	}
	data, _ := proto.Marshal(req)
	compressed := snappy.Encode(nil, data)
	b := bytes.NewReader(compressed)
	return b
}

func metricsTagsToLabels(tags map[string]string) []*prompb.Label {
	labels := make([]*prompb.Label, 0, len(tags))
	for name, value := range tags {
		labels = append(labels, &prompb.Label{
			Name:  name,
			Value: value,
		})
	}
	return labels
}

func metricsPointsToSamples(value float64, timestamp int64) []*prompb.Sample {
	return []*prompb.Sample{
		&prompb.Sample{
			Value:     value,
			Timestamp: timestamp,
		},
	}
}
