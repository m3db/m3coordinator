# Roadmap

## Launch M3Coordinator as a bridge for the Read/Write path of M3DB into open source (Q1 2018) 

### V1 (late Q4 2017 - early Q1 2018)
* Create a gRPC/Protobuf service
* Handlers for Prometheus remote read/write endpoints
* Perform fanout to M3DB nodes
* Tooling to set up M3Coordinator alongside Prometheus

### V2 (late Q1 2018)
* Support cross datacenter calls with remote aggregations
* Benchmark the performance of the coordinator and M3DB using popular datasets
* Support for multiple M3DB clusters


## Dashboards can directly interact with the M3Coordinator to get data from M3DB and optionally other stores (Q2 2018)

* Write a PromQL parser.
* Block based data model which reduces the memory footprint and also reduces the latency for queries.
# Better cross DC support.
# Port all Prometheus functions to the new data model
* Provide advanced query tracking to figure out bottlenecks.

## Optimizations for the M3Coordinator (Q3 2018)
* Write the current M3QL interfaces to conform to the common DAG structure.
* Suggest auto aggregation rules.
* Support authentication and rate limiting
* Cost accounting per query and memory management to prevent M3Coordinator from going OOM
* Push computation to storage nodes whenever possible
* Execution state manager to keep track of running queries
* Port the distributed computation to the M3 query service
