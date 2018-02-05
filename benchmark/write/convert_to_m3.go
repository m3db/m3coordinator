package main

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

func convertToM3(fileName string, workers int) []*M3Metric {
	fd, err := os.OpenFile(fileName, os.O_RDONLY, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to read json file, got error: %v", err)
		os.Exit(1)
	}

	defer fd.Close()

	var (
		metrics = make([]*M3Metric, 0, 100000)
		scanner = bufio.NewScanner(fd)
	)
	var wg sync.WaitGroup
	dataChannel := make(chan []byte, 100000)
	metricChannel := make(chan *M3Metric, 100000)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			for data := range dataChannel {
				if len(data) != 0 {
					var m Metrics
					if err := json.Unmarshal(data, &m); err != nil {
						fmt.Fprintf(os.Stderr, "Unable to unmarshal json, got error: %v", err)
						os.Exit(1)
					}
					metricChannel <- &M3Metric{ID: id(m.Tags, m.Name), Time: storage.PromTimestampToTime(m.Time), Value: m.Value}
				}
			}
			wg.Done()
		}()

	}

	go func() {
		for metric := range metricChannel {
			metrics = append(metrics, metric)
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

	return metrics
}

func id(lowerCaseTags map[string]string, name string) string {
	var sortedKeys []string
	var buffer = bytes.NewBuffer(nil)
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
