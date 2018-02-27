#!/bin/sh

# Ingests from data_gen.sh to telegraf, which is then ingested to Prometheus
#exec $GOPATH/src/github.com/m3db/m3coordinator/benchmark/data/data_gen.sh
cat $GOPATH/src/github.com/influxdata/influxdb-comparisons/cmd/bulk_data_gen/benchmark_influx
