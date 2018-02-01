## WARNING: This is Alpha software and not intended for use until a stable release.

# M3Coordinator [![GoDoc][doc-img]][doc] [![Build Status][ci-img]][ci] [![Coverage Status][cov-img]][cov]

M3Coordinator is a service which provides APIs for reading/writing to [M3DB](https://github.com/m3db/m3db) at a global and placement specific level.
It also acts as a bridge between [Prometheus](https://github.com/prometheus/prometheus) and [M3DB](https://github.com/m3db/m3db). Using this bridge, [M3DB](https://github.com/m3db/m3db) acts as a long term storage for [Prometheus](https://github.com/prometheus/prometheus) using the [remote read/write endpoints](https://github.com/prometheus/prometheus/blob/master/prompb/remote.proto).
A detailed explanation of setting up long term storage for Prometheus can be found [here](https://schd.ws/hosted_files/cloudnativeeu2017/73/Integrating%20Long-Term%20Storage%20with%20Prometheus%20-%20CloudNativeCon%20Berlin%2C%20March%2030%2C%202017.pdf).

### Running in Docker

> Note that all commands are run within the root of the m3coordinator directory except where specified.

**Running both m3coordinator and Prometheus in containers:**

You can launch a Prometheus and m3coordinator container using `docker-compose`. However, you must first build the m3coordinator Docker image.

To do so, you will need the m3coordinator binary:

    $ GOOS=linux GOARCH=amd64 CGO_ENABLED=0 make services

Once you have the binary, you can run the following to make the Docker image:

    $ docker build -t m3coordinator -f docker/Dockerfile .

Finally, you can spin up the two containers using `docker-compose` within the `docker/` directory:

    $ docker-compose up

> Note: The default local ports for Prometheus and m3coordinator are `9090` and `7201`, respectively, and the default `prometheus.yml` file is `docker/prometheus.yml`
>
>If you want to override these, you can pass in the following environment variables to the `docker-compose` command:
>
> `LOCAL_PROM_PORT`
>
> `LOCAL_M3COORD_PORT`
>
> `LOCAL_PROM_YML`
>
> (e.g. `$ LOCAL_PROM_PORT=XXXX LOCAL_M3COORD_PORT=XXXX LOCAL_PROM_YML=/path/to/yml docker-compose up`)

**Running m3coordinator locally (on mac only) and Prometheus in Docker container (for development):**

Build m3coordinator binary:

    $ make services

Run m3coordinator binary:

    $ ./bin/m3coordinator --config.file docker/coordinator.yml

Run Prometheus Docker image:

    $ docker run -p 9090:9090 -v $GOPATH/src/github.com/m3db/m3coordinator/docker/prometheus-mac.yml:/etc/prometheus/prometheus.yml quay.io/prometheus/prometheus

### Benchmarking

To benchmark m3db using m3coordinator.

1) Pull down `braskin/benchmark` branch in m3coordinator
2) Create metrics using InfluxDB's data gen tool:

    $ git clone https://github.com/benraskin92/influxdb-comparisons.git
    $ cd cmd/bulk_data_gen
    $ go build
    $ ./bulk_data_gen -format=opentsdb -timestamp-start=2018-02-01T15:18:00Z -timestamp-end=2018-02-01T15:28:00Z -scale-var=20 -seed=504570971 > $GOPATH/src/github.com/m3db/m3coordinator/benchmark/benchmark_data.json

> Note: The timestamp start and end must be within the `buffer_past` config that is set for m3db, otherwise you will get datapoint too far in the past errors.

3) Start m3db. You can use the config that is in `github.com/m3db/m3coordinator/benchmark/`

4) Build and run the benchmark tool in m3coordinator

    $ cd $GOPATH/src/github.com/m3db/m3coordinator/benchmark/
    $ go build && ./benchmark -workers=2

<hr>

This project is released under the [MIT License](LICENSE.md).

[doc-img]: https://godoc.org/github.com/m3db/m3coordinator?status.svg
[doc]: https://godoc.org/github.com/m3db/m3coordinator
[ci-img]: https://travis-ci.org/m3db/m3coordinator.svg?branch=master
[ci]: https://travis-ci.org/m3db/m3coordinator
[cov-img]: https://coveralls.io/repos/github/m3db/m3coordinator/badge.svg?branch=master&service=github
[cov]: https://coveralls.io/github/m3db/m3coordinator?branch=master
