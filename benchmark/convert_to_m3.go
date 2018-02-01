package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/m3db/m3coordinator/storage"
)

// Metrics is the OpenTSDB style metrics
type Metrics struct {
	Name  string            `json:"metric"`
	Time  int64             `json:"timestamp"`
	Tags  map[string]string `json:"tags"`
	Value float64           `json:"value"`
}

type M3Metric struct {
	ID    string
	Time  time.Time
	Value float64
}

var sortedKeys []string
var buffer = bytes.NewBuffer(nil)

func convertToM3(fileName string) []M3Metric {
	fd, err := os.OpenFile(fileName, os.O_RDONLY, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to read json file, got error: %v", err)
		os.Exit(1)
	}

	defer fd.Close()

	var (
		metrics = make([]M3Metric, 0, 100000)
		scanner = bufio.NewScanner(fd)
	)
	for scanner.Scan() {
		data := bytes.TrimSpace(scanner.Bytes())
		if len(data) != 0 {
			var m Metrics
			if err := json.Unmarshal(data, &m); err != nil {
				fmt.Fprintf(os.Stderr, "Unable to unmarshal json, got error: %v", err)
				os.Exit(1)
			}
			metrics = append(metrics, M3Metric{ID: ID(m.Tags, m.Name), Time: storage.PromTimestampToTime(m.Time), Value: m.Value})
		}
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}

	return metrics

}

func ID(lowerCaseTags map[string]string, name string) string {
	// Start generating path, write m3 prefix and name to buffer
	buffer.Truncate(0)
	buffer.WriteString(strings.ToLower(name))

	// Generate tags in alphabetical order & write to buffer
	i := 0
	for key := range lowerCaseTags {
		sortedKeys = append(sortedKeys, key)
		i++
	}
	sort.Strings(sortedKeys)

	i = 0
	for i = 0; i < len(sortedKeys)-1; i++ {
		buffer.WriteString(sortedKeys[i])
		buffer.WriteString(lowerCaseTags[sortedKeys[i]])
	}

	sortedKeys = sortedKeys[:0]
	return buffer.String()
}
