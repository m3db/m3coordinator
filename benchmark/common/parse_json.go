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

	"github.com/m3db/m3coordinator/storage"

	"github.com/m3db/m3x/ident"
)

var (
	wg sync.WaitGroup
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
	ID    ident.ID
	Tags  ident.Tags
	Time  time.Time
	Value float64
}

// ConvertToM3 parses the json file that is generated from InfluxDB's bulk_data_gen tool
func ConvertToM3(fileName string, workers int, f func(*M3Metric)) {
	fd, err := os.OpenFile(fileName, os.O_RDONLY, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to read json file, got error: %v", err)
		os.Exit(1)
	}

	defer fd.Close()

	scanner := bufio.NewScanner(fd)

	dataChannel := make(chan []byte, MetricsLen)
	metricChannel := make(chan *M3Metric, MetricsLen)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go unmarshalMetrics(dataChannel, metricChannel)
	}

	go func() {
		for metric := range metricChannel {
			f(metric)
		}
	}()

	for scanner.Scan() {
		data := bytes.TrimSpace(scanner.Bytes())
		b := make([]byte, len(data))
		copy(b, data)
		dataChannel <- b
	}

	close(dataChannel)
	wg.Wait()
	close(metricChannel)
	if err := scanner.Err(); err != nil {
		panic(err)
	}
}

func unmarshalMetrics(dataChannel chan []byte, metricChannel chan *M3Metric) {
	for data := range dataChannel {
		if len(data) != 0 {
			var m Metrics
			if err := json.Unmarshal(data, &m); err != nil {
				panic(err)
			}

			id, tags := parse(m.Tags, m.Name)
			metricChannel <- &M3Metric{ID: id, Tags: tags, Time: storage.PromTimestampToTime(m.Time), Value: m.Value}
		}
	}
	wg.Done()
}

func parse(lowerCaseTags map[string]string, name string) (ident.ID, ident.Tags) {
	tags := make(ident.Tags, 0, len(lowerCaseTags))
	sortedKeys := make([]string, len(lowerCaseTags))
	var buffer = bytes.NewBuffer(nil)
	buffer.WriteString(strings.ToLower(name))

	// Generate tags in alphabetical order & write to buffer
	for key := range lowerCaseTags {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)

	for _, key := range sortedKeys {
		buffer.WriteString(key)
		buffer.WriteString(lowerCaseTags[key])
		tags = append(tags, ident.StringTag(key, lowerCaseTags[key]))
	}

	return ident.StringID(buffer.String()), tags
}
