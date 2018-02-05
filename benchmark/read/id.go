package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

// Metrics is the OpenTSDB style metrics
type Metrics struct {
	Name string            `json:"metric"`
	Tags map[string]string `json:"tags"`
}

var sortedKeys []string
var buffer = bytes.NewBuffer(nil)

func getIDs(fileName string) []string {
	fd, err := os.OpenFile(fileName, os.O_RDONLY, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to read json file, got error: %v", err)
		os.Exit(1)
	}

	defer fd.Close()

	var (
		idSet = make(map[string]interface{}, 100000)

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
			idSet[id(m.Tags, m.Name)] = struct{}{}
		}
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}

	var ids = make([]string, 0, len(idSet))
	for k := range idSet {
		ids = append(ids, k)
	}
	return ids
}

func id(lowerCaseTags map[string]string, name string) string {
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
