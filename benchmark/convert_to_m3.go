package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/ts"

	xtime "github.com/m3db/m3x/time"
)

// Metrics is the OpenTSDB style metrics
type Metrics struct {
	Name  string            `json:"metric"`
	Time  int64             `json:"timestamp"`
	Tags  map[string]string `json:"tags"`
	Value float64           `json:"value"`
}

func convertToM3(fileName string) []storage.WriteQuery {
	raw, err := ioutil.ReadFile(fileName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to read json file, got error: %v", err)
		os.Exit(1)
	}

	var metrics []Metrics
	for _, line := range bytes.Split(raw, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) != 0 {
			var m Metrics
			if err := json.Unmarshal(line, &m); err != nil {
				fmt.Fprintf(os.Stderr, "Unable to unmarshal json, got error: %v", err)
				os.Exit(1)
			}
			metrics = append(metrics, m)
		}
	}

	var writeQueries []storage.WriteQuery
	for _, i := range metrics {
		var datapoint ts.Datapoint
		datapoint.Timestamp = storage.PromTimestampToTime(i.Time)
		datapoint.Value = i.Value
		writeQuery := storage.WriteQuery{
			Tags:       storage.FromMapToTags(i.Tags),
			Datapoints: ts.Datapoints{&datapoint},
			Unit:       xtime.Millisecond,
			Annotation: nil,
		}
		writeQueries = append(writeQueries, writeQuery)
	}

	return writeQueries
}
