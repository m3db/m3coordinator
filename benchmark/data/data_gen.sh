#!/bin/sh

export GOPATH=/home/nikunj/code

timeNow=$(date +%s)
start=`expr $timeNow - 1000`
end=`expr $timeNow - 10`
seedOne=519129548
seedTwo=925110754
seedThree=228803099
goodStart=`date -d @$start +"+%FT%T%Z" | sed s/+// | sed s/UTC/Z/`
goodEnd=`date -d @$end +"+%FT%T%Z" | sed s/+// | sed s/UTC/Z/`

seed=$seedOne
echo "start: $goodStart"
echo "end: $goodEnd"

$GOPATH/src/github.com/influxdata/influxdb-comparisons/cmd/bulk_data_gen/bulk_data_gen -timestamp-start=$goodStart -timestamp-end=$goodEnd -scale-var=1000 -seed=$seed > $GOPATH/src/github.com/influxdata/influxdb-comparisons/cmd/bulk_data_gen/benc
hmark_influx && $GOPATH/src/github.com/influxdata/influxdb-comparisons/cmd/bulk_data_gen/bulk_data_gen -format=opentsdb -timestamp-start=$goodStart -timestamp-end=$goodEnd -scale-var=1000 -seed=$seed > $GOPATH/src/github.com/influxdata/influxdb-c
omparisons/cmd/bulk_data_gen/benchmark_opentsdb

./benchmark -workers=2000 -data-file=$GOPATH/src/github.com/influxdata/influxdb-comparisons/cmd/bulk_data_gen/benchmark_opentsdb -cpuprofile=false -batch=5000 -address="0.0.0.0:8000" -benchmarkers="10.142.0.2:8000,10.142.0.4:8000,10.142.0.5:8000"
