package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/m3db/m3coordinator/services/m3coordinator/config"
	"github.com/m3db/m3coordinator/storage"

	"github.com/m3db/m3db/client"
	xconfig "github.com/m3db/m3x/config"
	xtime "github.com/m3db/m3x/time"
)

var (
	m3dbClientCfg string
	workers       int
	namespace     string

	wg           sync.WaitGroup
	inputDone    chan struct{}
	itemsWritten chan int
)

func init() {
	flag.StringVar(&m3dbClientCfg, "m3db-client-config", "benchmark.yml", "used to create m3db client session")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")
	flag.StringVar(&namespace, "namespace", "metrics", "M3DB namespace where to store result metrics")
	flag.Parse()
}

func main() {
	metrics := convertToM3("benchmark_data.json")
	ch := make(chan storage.WriteQuery, 1000000)
	inputDone = make(chan struct{})

	var cfg config.Configuration
	if err := xconfig.LoadFile(&cfg, m3dbClientCfg); err != nil {
		fmt.Fprintf(os.Stderr, "Unable to load %s: %v", m3dbClientCfg, err)
		os.Exit(1)
	}

	m3dbClientOpts := cfg.M3DBClientCfg
	m3dbClient, err := m3dbClientOpts.NewClient(client.ConfigurationParameters{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to create m3db client, got error %v\n", err)
		os.Exit(1)
	}

	session, err := m3dbClient.NewSession()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to create m3db client session, got error %v\n", err)
		os.Exit(1)
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

func addMetricsToChan(ch chan storage.WriteQuery, wq []storage.WriteQuery) int {
	var items int
	for _, query := range wq {
		ch <- query
		items++
	}
	close(inputDone)
	return items
}

func writeToM3DB(session client.Session, ch chan storage.WriteQuery, itemsWrittenCh chan int) {
	var itemsWritten int
	var otherWG sync.WaitGroup
	for query := range ch {
		id := query.Tags.ID()
		go session.Write(namespace, id, query.Datapoints[0].Timestamp, query.Datapoints[0].Value, xtime.Millisecond, nil)
		otherWG.Add(1)
		if itemsWritten%10000 == 0 {
			fmt.Println(itemsWritten)
		}
		itemsWritten++
	}
	otherWG.Done()
	wg.Done()
	itemsWrittenCh <- itemsWritten
}
