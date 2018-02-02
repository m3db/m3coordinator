package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

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

func (s *stats) combine(other stats) stats {
	otherSnap := other.snapshot()
	snap := s.snapshot()
	return stats{
		Writes:    snap.Writes + otherSnap.Writes,
		RunTimeMs: snap.RunTimeMs + other.RunTimeMs,
	}
}

// ClientOptions specify HTTP Client options.
type HTTPClientOptions struct {
	RequestTimeout      time.Duration `yaml:"requestTimeout"`
	ConnectTimeout      time.Duration `yaml:"connectTimeout"`
	KeepAlive           time.Duration `yaml:"keepAlive"`
	MaxIdleConnsPerHost int           `yaml:"maxIdleConnsPerHost"`
	DisableCompression  bool          `yaml:"disableCompression"`
}

// NewClient constructs a new HTTP Client.
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

// DefaultClientOptions returns default options.
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

func (b *benchmarker) queryBenchmarkers() {
	client := NewHTTPClient(DefaultHTTPClientOptions())
	for {
		func() {
			// To be able to use defer run in own fn
			var allStats []stats
			allResponded := true
			time.Sleep(10 * time.Second)
			for _, addr := range b.allAddresses() {
				req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/stats", addr), nil)
				if err != nil {
					panic(err)
				}

				resp, err := client.Do(req)
				if resp != nil {
					defer resp.Body.Close()
				}

				if err != nil {
					allResponded = false
					continue
				}

				var s stats
				if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
					fmt.Fprintf(os.Stderr, "failed to decode response from benchmarker %s: %v", addr, err)
					allResponded = false
					continue
				}

				allStats = append(allStats, s)
			}

			if allResponded {
				total := int64(0)
				sumTook := time.Duration(0)
				for _, s := range allStats {
					total += s.Writes
					sumTook += time.Duration(s.RunTimeMs) * time.Millisecond
				}
				avgTook := float64(sumTook) / float64(len(allStats))
				avgTookDuration := time.Duration(avgTook)
				rate := float64(total) / float64(avgTookDuration.Seconds())
				fmt.Printf("global stats %d items in %fsec (mean values rate %f/sec)\n",
					total, avgTookDuration.Seconds(), rate)
			}
		}()
	}
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
