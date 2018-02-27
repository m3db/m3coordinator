package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3coordinator/benchmark/common"
	"github.com/m3db/m3coordinator/services/m3coordinator/config"

	"github.com/m3db/m3db/client"
	xconfig "github.com/m3db/m3x/config"
	xtime "github.com/m3db/m3x/time"

	"github.com/pkg/profile"
)

var (
	m3dbClientCfg string
	dataFile      string
	workers       int
	batch         int
	batchSize     int
	namespace     string
	address       string
	benchmarkers  string
	memprofile    bool
	cpuprofile    bool

	coordinator bool

	// inputDone    chan struct{}
	// itemsWritten chan int
)

func init() {
	flag.StringVar(&m3dbClientCfg, "m3db-client-config", "configs/benchmark.yml", "used to create m3db client session")
	flag.StringVar(&dataFile, "data-file", "data.json", "input data for benchmark")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")
	flag.IntVar(&batch, "batch", 5000, "Batch Size")
	flag.IntVar(&batchSize, "batchSize", 100, "Number of write requests per batch.")
	flag.StringVar(&namespace, "namespace", "metrics", "M3DB namespace where to store result metrics")
	flag.StringVar(&address, "address", "localhost:8888", "Address to expose benchmarker health and stats")
	flag.StringVar(&benchmarkers, "benchmarkers", "localhost:8888", "Comma separated host:ports addresses of benchmarkers to coordinate")
	flag.BoolVar(&memprofile, "memprofile", false, "Enable memory profile")
	flag.BoolVar(&cpuprofile, "cpuprofile", false, "Enable cpu profile")
	flag.BoolVar(&coordinator, "coordinator", false, "Benchmark through coordinator rather than m3db directly")
	flag.Parse()
}

func main() {
	if coordinator {
		benchmarkCoordinator()
	} else {
		benchmarkM3DB()
	}
}

func benchmarkM3DB() {
	//Setup
	metrics := make([]*common.M3Metric, 0, common.MetricsLen)
	common.ConvertToM3(dataFile, workers, func(m *common.M3Metric) {
		metrics = append(metrics, m)
	})

	ch := make(chan *common.M3Metric, workers)
	inputDone := make(chan struct{})
	wg := new(sync.WaitGroup)

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

	itemsWritten := make(chan int)
	workerFunction := func() {
		wg.Add(1)
		writeToM3DB(session, ch, itemsWritten)
		wg.Done()
	}

	appendReadCount := func() int {
		var items int
		for _, query := range metrics {
			ch <- query
			items++
		}
		close(inputDone)
		return items
	}

	cleanup := func() {
		<-inputDone
		close(ch)
		wg.Wait()
		if err := session.Close(); err != nil {
			log.Fatalf("Unable to close m3db client session, got error %v\n", err)
		}
	}

	genericBenchmarker(itemsWritten, workerFunction, appendReadCount, cleanup)
}

func benchmarkCoordinator() {
	// Setup
	metrics := make([]*bytes.Reader, 0, common.MetricsLen/batchSize)
	common.ConvertToProm(dataFile, workers, batchSize, func(m *bytes.Reader) {
		metrics = append(metrics, m)
	})

	ch := make(chan *bytes.Reader, workers)
	itemsWritten := make(chan int)
	wg := new(sync.WaitGroup)

	workerFunction := func() {
		fmt.Println("Workerfunctioning")
		wg.Add(1)
		writeToCoordinator(ch, itemsWritten)
		fmt.Println("Wrote to coord")
		wg.Done()
	}

	inputDone := make(chan struct{})
	appendReadCount := func() int {
		var items int
		for _, query := range metrics {
			ch <- query
			items++
		}
		close(inputDone)
		return items
	}

	cleanup := func() {
		fmt.Println("predone")
		<-inputDone
		fmt.Println("done")
		close(ch)
		fmt.Println("closed")
		wg.Wait()
		fmt.Println("waited")
	}
	genericBenchmarker(itemsWritten, workerFunction, appendReadCount, cleanup)
}

func genericBenchmarker(itemsWritten <-chan int, workerFunction func(), appendReadCount func() int, cleanup func()) {
	if cpuprofile {
		p := profile.Start(profile.CPUProfile)
		defer p.Stop()
	}

	if memprofile {
		p := profile.Start(profile.MemProfile)
		defer p.Stop()
	}

	// send over http
	var waitForInit sync.WaitGroup
	for i := 0; i < workers; i++ {
		waitForInit.Add(1)
		go func() {
			waitForInit.Done()
			workerFunction()
		}()
	}

	fmt.Printf("waiting for workers to spin up...\n")
	waitForInit.Wait()
	fmt.Println("done")

	b := &benchmarker{address: address, benchmarkers: benchmarkers}
	go b.serve()
	fmt.Printf("waiting for other benchmarkers to spin up...\n")
	b.waitForBenchmarkers()
	fmt.Println("done")

	var (
		start          = time.Now()
		itemsRead      = appendReadCount()
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

	fmt.Println("cleanip")
	sumChan := make(chan int)
	go func() {
		sum := 0
		for i := 0; i < workers; i++ {
			sum += <-itemsWritten
		}
		sumChan <- sum
	}()
	cleanup()
	<-sumChan

	end := time.Now()
	took := end.Sub(start)
	atomic.StoreInt64(&endNanosAtomic, end.UnixNano())
	rate := float64(itemsRead) / took.Seconds()
	perWorker := rate / float64(workers)

	fmt.Printf("loaded %d items in %fsec with %d workers (mean values rate %f/sec); per worker %f/sec\n", itemsRead, took.Seconds(), workers, rate, perWorker)
}

func writeToCoordinator(ch <-chan *bytes.Reader, itemsWrittenCh chan<- int) {
	var itemsWritten int
	for query := range ch {
		fmt.Println("tochanAA")

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
	fmt.Println("tochan")

	itemsWrittenCh <- itemsWritten
	fmt.Println("finitop")
}

func writeToM3DB(session client.Session, ch <-chan *common.M3Metric, itemsWrittenCh chan<- int) {
	var itemsWritten int
	for query := range ch {
		id := query.ID
		if err := session.Write(namespace, id, query.Time, query.Value, xtime.Millisecond, nil); err != nil {
			fmt.Println(err)
		} else {
			stat.incWrites()
		}
		if itemsWritten > 0 && itemsWritten%10000 == 0 {
			fmt.Println(itemsWritten)
		}
		itemsWritten++
	}
	itemsWrittenCh <- itemsWritten
}
