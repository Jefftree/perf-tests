# RangeStream and ConcurrentWatchObjectDecode at 5k

Three 5,000-node runs on `pull-kubernetes-gce-master-scale-performance-5000-experimental`, straddling the 1.37 default-on flips for `EtcdRangeStream` and `ConcurrentWatchObjectDecode`.

| | build | date | RangeStream | CWOD |
|---|---|---|---|---|
| **OFF** | [2071777797344333824](https://prow.k8s.io/view/gs/kubernetes-ci-logs/pr-logs/pull/139476/pull-kubernetes-gce-master-scale-performance-5000-experimental/2071777797344333824) | 06-30 | 0 | 0 |
| **OFF** | [2075579658681716736](https://prow.k8s.io/view/gs/kubernetes-ci-logs/pr-logs/pull/139476/pull-kubernetes-gce-master-scale-performance-5000-experimental/2075579658681716736) | 07-10 | 0 | 0 |
| **ON** | [2079551721146683392](https://prow.k8s.io/view/gs/kubernetes-ci-logs/pr-logs/pull/139476/pull-kubernetes-gce-master-scale-performance-5000-experimental/2079551721146683392) | 07-21 | 1 | 1 |

**Two things clearly got better:**

- **Pods watch-cache init** went from 11.0 s to 4.2 s, 2.6x faster.
- **etcd peak heap** came down from 0.81 to 0.67 GB, a 17% drop, and memory is steadier.

Everything else moved less than the two OFF runs move against each other, so treat it as run-to-run noise rather than an effect of the gates.

## Watch-cache init, restarted apiserver only

| resource | OFF 06-30 | OFF 07-10 | ON 07-21 | ON vs OFF mean | OFF spread |
| --- | --- | --- | --- | --- | --- |
| **pods** | 10.945 s | 11.094 s | **4.249 s** | **2.59x faster** | 1.4% |
| deployments | 0.914 s | 0.913 s | 0.417 s | 2.19x faster | 0.0% |
| replicasets | 0.900 s | 0.913 s | 0.470 s | 1.93x faster | 1.4% |

CL2 `Watch Cache Initialization Duration`, restarted instance only.

## etcd

| Go heap, 3 members summed | OFF 06-30 | OFF 07-10 | ON 07-21 | ON vs OFF | OFF spread |
| --- | --- | --- | --- | --- | --- |
| **peak** reachable (`next_gc/2`) | 0.80 GB | 0.81 GB | 0.67 GB | -17.5% | 1.4% |
| **peak** heap in-use | 1.31 GB | 1.43 GB | 1.18 GB | -13.8% | 9.5% |
| peak CPU, busiest member | 2.38 cores | 2.88 cores | 3.21 cores | +22.3% | 20.7% |

## etcd heap

Worst of the three members, across the full window. etcd registers only the legacy Go collector, so `go_gc_heap_live_bytes` does not exist there and `next_gc/2` stands in for reachable heap. On the apiserver, which exports both, that proxy runs 4.2 to 4.9% above true live heap.

| metric | run | typical | peak | swing |
| --- | --- | --- | --- | --- |
| next_gc/2 | OFF 06-30 | 0.15 GB | 0.39 GB | 2.6x |
|  | OFF 07-10 | 0.16 GB | 0.40 GB | 2.5x |
|  | **ON 07-21** | 0.16 GB | 0.23 GB | 1.4x |
| heap in-use | OFF 06-30 | 0.26 GB | 0.70 GB | 2.7x |
|  | OFF 07-10 | 0.28 GB | 0.76 GB | 2.7x |
|  | **ON 07-21** | 0.29 GB | 0.43 GB | 1.5x |

Same idle level in every run, on both metrics. The OFF runs spike to 2.5 to 2.7x it, the ON run to 1.4 to 1.5x. Same on the post-restart window alone: 1.8 and 1.9x against 1.1x.

`<placeholder>`

`<placeholder>`

`<placeholder>`

## apiserver CPU

| metric | OFF 06-30 | OFF 07-10 | ON 07-21 | ON vs OFF mean | OFF spread |
| --- | --- | --- | --- | --- | --- |
| window CPU, restart -30 to +35 min | 101,511 cpu-s | 91,154 cpu-s | 106,301 cpu-s | +10.3% | 11.4% |

`<placeholder>`

## apiserver memory

| metric, 3 masters summed at one instant | OFF 06-30 | OFF 07-10 | ON 07-21 | ON vs OFF | OFF spread |
| --- | --- | --- | --- | --- | --- |
| **reachable heap** (`go_gc_heap_live_bytes`) | 52.35 GB | 50.53 GB | 50.59 GB | -1.7% | 3.6% |
| heap in-use, includes uncollected garbage | 103.07 GB | 95.24 GB | 98.37 GB | -0.8% | 8.2% |
| RSS | 120.11 GB | 116.35 GB | 115.64 GB | -2.2% | 3.2% |
| goroutines | 411,192 | 393,802 | 402,448 | -0.0% | 4.4% |
| cluster WATCH | 186,251 | 177,616 | 181,489 | -0.2% | 4.9% |
| *busiest master's RSS* | 46.92 GB | 45.05 GB | 43.94 GB | -4.4% | 4.2% |
| *busiest master's WATCH* | 93,271 | 96,462 | 96,825 | +2.1% | 3.4% |

apiserver memory footprint is unchanged.

`<placeholder>`

`<placeholder>`

## RangeStream invocation

Whole-run counter maxima.

| counter | OFF 06-30 | OFF 07-10 | ON 07-21 |
| --- | --- | --- | --- |
| apiserver `etcd_requests_total{operation="list"}` | 6,427 | 6,791 | **0, series absent** |
| apiserver `operation="listStream"` | series absent | series absent | **6,853** |
| of which pods | - | - | 368 |
| of which apiServerIPInfo | - | - | 2,206 |
| etcd `grpc_server_started_total{grpc_method="RangeStream"}` | 0 | 0 | **7,184** |

## Latency

CL2 `APIResponsivenessPrometheus` simple, p99 in ms, `n` in parentheses.

| row | OFF 06-30 | OFF 07-10 | ON 07-21 | ON vs OFF mean | OFF spread |
| --- | --- | --- | --- | --- | --- |
| WATCHLIST configmaps, resource | 3,682 (232,586) | 2,588 (237,117) | 1,817 (237,699) | -42.1% | 42.3% |
| WATCHLIST secrets, resource | 3,590 (133,281) | 2,445 (141,149) | 1,758 (138,583) | -41.7% | 46.8% |
| LIST pods, namespace | 2,140 (86) | 1,515 (97) | 7,280 (172) | +298.3% | 41.2% |
| LIST pods, cluster | 9,920 (4) | 5,960 (4) | 592 (4) | -92.5% | 66.4% |
| WATCHLIST pods, cluster | 60,000 (29) | 39,605 (36) | 58,750 (25) | +18.0% | 51.5% |
