package main

import (
	"bytes"
	"flag"
	"io"
	"log"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/m3db/m3coordinator/benchmark/common"
	"github.com/m3db/m3coordinator/generated/proto/prometheus/prompb"
	"github.com/m3db/m3coordinator/services/m3coordinator/config"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/encoding"
	xconfig "github.com/m3db/m3x/config"
	"github.com/m3db/m3x/ident"

	"github.com/golang/snappy"
)

var (
	m3dbClientCfg     string
	dataFile          string
	workers           int
	batch             int
	namespace         string
	memprofile        bool
	cpuprofile        bool
	timestampStartStr string
	timestampEndStr   string

	readEndpoint string
	coordinator  bool
)

func init() {
	flag.StringVar(&m3dbClientCfg, "m3db-client-config", "benchmark.yml", "used to create m3db client session")
	flag.StringVar(&dataFile, "data-file", "data.json", "input data for benchmark")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")
	flag.IntVar(&batch, "batch", 5000, "Batch Size")
	flag.StringVar(&namespace, "namespace", "metrics", "M3DB namespace where to store result metrics")
	flag.StringVar(&timestampStartStr, "timestamp-start", "2016-01-01T00:00:00Z", "Beginning timestamp (RFC3339).")
	flag.StringVar(&timestampEndStr, "timestamp-end", "2016-01-01T06:00:00Z", "Ending timestamp (RFC3339).")
	flag.BoolVar(&memprofile, "memprofile", false, "Enable memory profile")
	flag.BoolVar(&cpuprofile, "cpuprofile", false, "Enable cpu profile")
	flag.StringVar(&readEndpoint, "readEndpoint", "http://localhost:7201/api/v1/prom/read", "Read endpoint for m3coordinator")
	flag.BoolVar(&coordinator, "coordinator", false, "Benchmark through coordinator rather than m3db directly")
	flag.Parse()
}

func main() {
	// Parse timestamps:
	start, err := time.Parse(time.RFC3339, timestampStartStr)
	if err != nil {
		log.Fatal(err)
	}
	start = start.UTC()
	end, err := time.Parse(time.RFC3339, timestampEndStr)
	if err != nil {
		log.Fatal(err)
	}
	end = end.UTC()
	// Split on coord vs m3db

	if coordinator {
		log.Println("Benchmarking reads over http endpoint m3coordinator...")
		benchmarkCoordinator(start, end)
	} else {
		log.Println("Benchmarking reads on m3db...")
		benchmarkM3DB(start, end)
	}
}

type countFunc func() int

func benchmarkCoordinator(start, end time.Time) {
	promRead := generatePromReadBody(start, end)
	var readResponse []byte

	fetch := func() {
		r, err := common.PostEncodedSnappy(readEndpoint, promRead)
		if err != nil {
			log.Fatalf("Unable to fetch metrics from m3coordinator, got error %v\n", err)
		}
		readResponse = make([]byte, r.ContentLength)
		r.Body.Read(readResponse)
		r.Body.Close()
		if r.StatusCode != 200 {
			log.Fatalf("HTTP read failed with code %d, error: %s", r.StatusCode, string(readResponse))
		}
	}

	count := func() int {
		reqBuf, err := snappy.Decode(nil, readResponse)
		if err != nil {
			log.Fatalf("Unable to decode response, got error %v\n", err)
		}
		var req prompb.ReadResponse
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			log.Fatalf("Unable to unmarshal prompb response, got error %v\n", err)
		}
		return req.Size()
	}

	genericBenchmarker(fetch, count)
}

func benchmarkM3DB(start, end time.Time) {
	var cfg config.Configuration
	if err := xconfig.LoadFile(&cfg, m3dbClientCfg); err != nil {
		log.Fatalf("Unable to load %s: %v", m3dbClientCfg, err)
	}
	m3dbClientOpts := cfg.M3DBClientCfg
	m3dbClient, err := m3dbClientOpts.NewClient(client.ConfigurationParameters{}, func(v client.Options) client.Options {
		return v.SetWriteBatchSize(batch).SetWriteOpPoolSize(batch * 2)
	})
	if err != nil {
		log.Fatalf("Unable to create m3db client, got error %v\n", err)
	}

	session, err := m3dbClient.NewSession()
	if err != nil {
		log.Fatalf("Unable to create m3db client session, got error %v\n", err)
	}
	ids := getUniqueIds()
	var rawResults encoding.SeriesIterators

	fetch := func() {
		ns := ident.StringID(namespace)
		it := ident.NewStringIDsSliceIterator(ids)
		defer func() {
			ns.Finalize()
			it.Close()
		}()

		rawResults, err = session.FetchIDs(ns, it, start, end)
		if err != nil {
			log.Fatalf("Unable to fetch metrics from m3db, got error %v\n", err)
		}
	}

	count := func() int {
		return rawResults.Len()
	}

	genericBenchmarker(fetch, count)
}

func getUniqueIds() []string {
	ids := make([]string, 0)

	common.ConvertToM3(dataFile, workers, func(m *common.M3Metric) {
		ids = append(ids, m.ID)
	})

	return ids
}

func genericBenchmarker(fetch func(), count countFunc) {
	start := time.Now()
	log.Println("Started benchmark at:", start.Format(time.StampMilli))
	fetch()
	end := time.Now()
	log.Println("Finished benchmark at:", start.Format(time.StampMilli))
	took := end.Sub(start)
	// Counting should be done after timer has stopped in case any transforms are required
	results := count()
	rate := float64(results) / took.Seconds()

	log.Printf("Returned %d timeseries in %fsec (mean values rate %f/sec)\n", results, took.Seconds(), rate)
}

func generateMatchers() []*prompb.LabelMatcher {
	ids := getUniqueIds()
	matchers := make([]*prompb.LabelMatcher, len(ids))
	for i, id := range ids {
		matchers[i] = &prompb.LabelMatcher{
			Type:  prompb.LabelMatcher_EQ,
			Name:  "eq",
			Value: id,
		}
	}
	return matchers
}

func generatePromReadRequest(start, end time.Time) *prompb.ReadRequest {
	req := &prompb.ReadRequest{
		Queries: []*prompb.Query{{
			Matchers:         generateMatchers(),
			StartTimestampMs: start.UnixNano() / int64(time.Millisecond),
			EndTimestampMs:   end.UnixNano() / int64(time.Millisecond),
		}},
	}
	return req
}

func generatePromReadBody(start, end time.Time) io.Reader {
	req := generatePromReadRequest(start, end)
	data, err := proto.Marshal(req)
	if err != nil {
		log.Fatalf("Unable to marshal request, got error %v\n", err)
	}
	compressed := snappy.Encode(nil, data)
	b := bytes.NewReader(compressed)
	return b
}
