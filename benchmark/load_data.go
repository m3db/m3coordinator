package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

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
	namespace     string
	memprofile    bool
	cpuprofile    bool

	wg           sync.WaitGroup
	inputDone    chan struct{}
	itemsWritten chan int
)

func init() {
	flag.StringVar(&m3dbClientCfg, "m3db-client-config", "benchmark.yml", "used to create m3db client session")
	flag.StringVar(&dataFile, "data-file", "data.json", "input data for benchmark")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")
	flag.IntVar(&batch, "batch", 5000, "Batch Size")
	flag.StringVar(&namespace, "namespace", "metrics", "M3DB namespace where to store result metrics")
	flag.BoolVar(&memprofile, "memprofile", false, "Enable memory profile")
	flag.BoolVar(&cpuprofile, "cpuprofile", false, "Enable cpu profile")
	flag.Parse()
}

func main() {
	metrics := convertToM3(dataFile)
	ch := make(chan M3Metric, workers)
	inputDone = make(chan struct{})

	var cfg config.Configuration
	if err := xconfig.LoadFile(&cfg, m3dbClientCfg); err != nil {
		fmt.Fprintf(os.Stderr, "Unable to load %s: %v", m3dbClientCfg, err)
		os.Exit(1)
	}

	m3dbClientOpts := cfg.M3DBClientCfg
	m3dbClient, err := m3dbClientOpts.NewClient(client.ConfigurationParameters{}, func(v client.Options) client.Options {
		return v.SetMinConnectionCount(1).SetMaxConnectionCount(1).SetWriteBatchSize(batch).SetWriteOpPoolSize(batch * 2)
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to create m3db client, got error %v\n", err)
		os.Exit(1)
	}

	session, err := m3dbClient.NewSession()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to create m3db client session, got error %v\n", err)
		os.Exit(1)
	}

	if cpuprofile {
		p := profile.Start(profile.CPUProfile)
		defer p.Stop()
	}

	if memprofile {
		p := profile.Start(profile.MemProfile)
		defer p.Stop()
	}

	itemsWritten = make(chan int)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go writeToM3DB(session, ch, itemsWritten)
	}

	start := time.Now()
	itemsRead := addMetricsToChan(ch, metrics)

	<-inputDone
	close(ch)

	wg.Wait()
	sum := 0
	for i := 0; i < workers; i++ {
		sum += <-itemsWritten
	}

	fmt.Println(sum)
	end := time.Now()
	took := end.Sub(start)
	rate := float64(itemsRead) / float64(took.Seconds())

	fmt.Printf("loaded %d items in %fsec with %d workers (mean values rate %f/sec)\n", itemsRead, took.Seconds(), workers, rate)

	if err := session.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "Unable to close m3db client session, got error %v\n", err)
	}
}

func addMetricsToChan(ch chan M3Metric, wq []M3Metric) int {
	var items int
	for _, query := range wq {
		ch <- query
		items++
	}
	close(inputDone)
	return items
}

func writeToM3DB(session client.Session, ch chan M3Metric, itemsWrittenCh chan int) {
	var itemsWritten int
	// var otherWG sync.WaitGroup
	for query := range ch {
		id := query.ID
		if err := session.Write(namespace, id, query.Time, query.Value, xtime.Millisecond, nil); err != nil {
			fmt.Println(err)
		}
		// otherWG.Add(1)
		// go func() {
		// 	session.Write(namespace, id, query.Datapoints[0].Timestamp, query.Datapoints[0].Value, xtime.Millisecond, nil)
		// 	otherWG.Done()
		// }()
		if itemsWritten > 0 && itemsWritten%10000 == 0 {
			fmt.Println(itemsWritten)
		}
		itemsWritten++
	}
	// otherWG.Wait()
	wg.Done()
	itemsWrittenCh <- itemsWritten
}
