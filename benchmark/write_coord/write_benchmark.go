package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3coordinator/benchmark/common"
	"github.com/pkg/profile"
)

var (
	dataFile     string
	workers      int
	batchSize    int
	address      string
	benchmarkers string
	memprofile   bool
	cpuprofile   bool

	wg           sync.WaitGroup
	inputDone    chan struct{}
	itemsWritten chan int
)

func init() {
	flag.StringVar(&dataFile, "data-file", "data.json", "input data for benchmark")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")
	flag.IntVar(&batchSize, "batchSize", 100, "Number of write requests per batch.")
	flag.StringVar(&address, "address", "localhost:8888", "Address to expose benchmarker health and stats")
	flag.StringVar(&benchmarkers, "benchmarkers", "localhost:8888", "Comma separated host:ports addresses of benchmarkers to coordinate")
	flag.BoolVar(&memprofile, "memprofile", false, "Enable memory profile")
	flag.BoolVar(&cpuprofile, "cpuprofile", false, "Enable cpu profile")
	flag.Parse()
}

func main() {
	metrics := make([]*bytes.Reader, 0, common.MetricsLen/batchSize)
	common.ConvertToProm(dataFile, workers, batchSize, func(m *bytes.Reader) {
		metrics = append(metrics, m)
	})
	ch := make(chan *bytes.Reader, workers)
	inputDone = make(chan struct{})

	if cpuprofile {
		p := profile.Start(profile.CPUProfile)
		defer p.Stop()
	}

	if memprofile {
		p := profile.Start(profile.MemProfile)
		defer p.Stop()
	}

	// send over http
	itemsWritten = make(chan int)
	var waitForInit sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		waitForInit.Add(1)
		go func() {
			waitForInit.Done()
			sendToCoordinator(ch, itemsWritten)
		}()
	}

	fmt.Printf("waiting for workers to spin up...\n")
	waitForInit.Wait()
	fmt.Printf("done\n")

	b := &benchmarker{address: address, benchmarkers: benchmarkers}
	go b.serve()
	fmt.Printf("waiting for other benchmarkers to spin up...\n")
	b.waitForBenchmarkers()
	fmt.Printf("done\n")

	var (
		start          = time.Now()
		itemsRead      = addMetricsToChan(ch, metrics)
		endNanosAtomic int64
	)
	go func() {
		for {
			time.Sleep(time.Second)
			if v := atomic.LoadInt64(&endNanosAtomic); v > 0 {
				stat.setRunTimeMs(int64(time.Unix(0, v).Sub(start) / time.Millisecond))
			} else {
				stat.setRunTimeMs(int64(time.Since(start) / time.Millisecond))
			}
		}
	}()

	<-inputDone
	close(ch)

	wg.Wait()
	sum := 0
	for i := 0; i < workers; i++ {
		sum += <-itemsWritten
	}

	end := time.Now()
	took := end.Sub(start)
	atomic.StoreInt64(&endNanosAtomic, end.UnixNano())
	rate := float64(itemsRead) / took.Seconds()
	perWorker := rate / float64(workers)

	fmt.Printf("loaded %d items in %fsec with %d workers (mean values rate %f/sec); per worker %f/sec\n", itemsRead, took.Seconds(), workers, rate, perWorker)
}

func addMetricsToChan(ch chan *bytes.Reader, wq []*bytes.Reader) int {
	var items int
	for _, query := range wq {
		ch <- query
		items++
	}
	close(inputDone)
	return items
}

func sendToCoordinator(ch chan *bytes.Reader, itemsWrittenCh chan int) {
	var itemsWritten int
	for query := range ch {
		if r, err := http.Post("http://localhost:7201/api/v1/prom/write", "", query); err != nil {
			fmt.Println(err)
		} else {
			if r.StatusCode != 200 {
				b := make([]byte, r.ContentLength)
				r.Body.Read(b)
				r.Body.Close()
				fmt.Println(string(b))
			}
			stat.incWrites()
		}
		itemsWritten++
	}
	wg.Done()
	itemsWrittenCh <- itemsWritten
}

/*




REEEEE





*/
var stat = new(stats)

type stats struct {
	Writes    int64 `json:"writes"`
	RunTimeMs int64 `json:"run_time_ms"`
}

func (s *stats) getWrites() int64 {
	return atomic.LoadInt64(&s.Writes)
}

func (s *stats) incWrites() {
	atomic.AddInt64(&s.Writes, 1)
}

func (s *stats) getRunTimeMs() int64 {
	return atomic.LoadInt64(&s.Writes)
}

func (s *stats) setRunTimeMs(v int64) {
	atomic.StoreInt64(&s.RunTimeMs, v)
}

func (s *stats) snapshot() stats {
	return stats{Writes: s.getWrites(), RunTimeMs: s.getRunTimeMs()}
}

// HTTPClientOptions specify HTTP Client options.
type HTTPClientOptions struct {
	RequestTimeout      time.Duration `yaml:"requestTimeout"`
	ConnectTimeout      time.Duration `yaml:"connectTimeout"`
	KeepAlive           time.Duration `yaml:"keepAlive"`
	MaxIdleConnsPerHost int           `yaml:"maxIdleConnsPerHost"`
	DisableCompression  bool          `yaml:"disableCompression"`
}

// NewHTTPClient constructs a new HTTP Client.
func NewHTTPClient(o HTTPClientOptions) *http.Client {
	return &http.Client{
		Timeout: o.RequestTimeout,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout:   o.ConnectTimeout,
				KeepAlive: o.KeepAlive,
			}).Dial,
			TLSHandshakeTimeout: o.ConnectTimeout,
			MaxIdleConnsPerHost: o.MaxIdleConnsPerHost,
			DisableCompression:  o.DisableCompression,
		},
	}
}

// DefaultHTTPClientOptions returns default options.
func DefaultHTTPClientOptions() HTTPClientOptions {
	return HTTPClientOptions{
		RequestTimeout:      2 * time.Second,
		ConnectTimeout:      2 * time.Second,
		KeepAlive:           60 * time.Second,
		MaxIdleConnsPerHost: 20,
		DisableCompression:  true,
	}
}

type benchmarker struct {
	address      string
	benchmarkers string
}

type health struct {
	Up bool `json:"up"`
}

func (b *benchmarker) serve() {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(health{Up: true})
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(stat.snapshot())
	})
	http.ListenAndServe(b.address, mux)
	if err := http.ListenAndServe(b.address, mux); err != nil {
		fmt.Fprintf(os.Stderr, "server could not listen on %s: %v", b.address, err)
	}
}

func (b *benchmarker) allAddresses() []string {
	var all []string
	for _, addr := range strings.Split(b.benchmarkers, ",") {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		all = append(all, addr)
	}
	return all
}

func (b *benchmarker) waitForBenchmarkers() {
	client := NewHTTPClient(DefaultHTTPClientOptions())
	allUp := false
	for !allUp {
		func() {
			// To be able to use defer run in own fn
			time.Sleep(10 * time.Millisecond)
			allUp = true
			for _, addr := range b.allAddresses() {
				req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/health", addr), nil)
				if err != nil {
					panic(err)
				}

				resp, err := client.Do(req)
				if err != nil {
					allUp = false
					continue
				}

				defer resp.Body.Close()

				var r health
				if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
					fmt.Fprintf(os.Stderr, "failed to decode response from benchmarker %s: %v", addr, err)
					allUp = false
					continue
				}

				allUp = allUp && r.Up
			}
		}()
	}

	fmt.Printf("all ready, now synchronizing to nearest 10s...\n")
	sync := 5 * time.Second
	now := time.Now()
	waitFor := now.Truncate(sync).Add(sync).Sub(now)
	time.Sleep(waitFor)
}
