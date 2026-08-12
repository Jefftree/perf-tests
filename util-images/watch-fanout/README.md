# watch-fanout

Reproduces the apiserver load of a 5000-node cluster on a single machine,
without the cluster.

The load is not driven by node count. On gce-5k run `2080704005197008896`,
115,739 EndpointSlice events over 103 minutes fanned out to 5,205 watchers to
produce 602.4M deliveries. That is **~19 writes per second** (about 33/s at
storm peak). So what has to be simulated is the *clients* and the *writes*, not
the nodes: no kubelets, no scheduler, no VMs.

## What it does

| command | role |
| --- | --- |
| `preload` | creates the population a load test leaves behind: 50 namespaces x (149 small + 12 medium + 1 big) = 8,100 Services and 8,200 EndpointSlices, plus 500 ConfigMaps for `--idle-watches` |
| `write` | drives EndpointSlice and Service writes at a fixed rate, stamping the write time into each object |
| `drain` | runs N independent watch clients, one simulated node each, with the selectors kube-proxy uses |
| `pods` | creates N pods shaped like `CL2_REALISTIC_POD`. They are never scheduled; they exist to occupy the watch cache and to be LISTed |
| `list` | issues cluster-scoped `rv=0` pod LISTs, streaming and discarding the body |
| `kubelet` | runs N simulated kubelets, each with its own identity, renewing its own Lease |

## Quick start

```bash
# one-time: build kube-apiserver (etcd comes from third_party/etcd)
cd ~/workspace/kubernetes && go build -o _output/bin/kube-apiserver ./cmd/kube-apiserver

# terminal 1 -- etcd + apiserver as plain processes, no docker, no sudo
./hack/local-rig.sh

# terminal 2
go build -o /tmp/wf-bin/watch-fanout .
KC=/tmp/watch-fanout/kubeconfig
/tmp/wf-bin/watch-fanout preload --kubeconfig=$KC
/tmp/wf-bin/watch-fanout write   --kubeconfig=$KC --eps-rate=33 --svc-rate=2.6

# terminal 3
/tmp/wf-bin/watch-fanout drain --kubeconfig=$KC --clients=5000 --mode=raw
/tmp/wf-bin/watch-fanout drain --kubeconfig=$KC --clients=200 --mode=decode --metrics-addr=:9113
```

## Testing a custom Kubernetes build

Nothing here is compiled against Kubernetes. This module is an ordinary
client-go program, and the apiserver under test is simply whatever binary the
rig launches, so any build works.

| variable | default | purpose |
| --- | --- | --- |
| `K8S_ROOT` | `~/workspace/kubernetes` | where to find etcd and kube-apiserver |
| `APISERVER_BIN` | `$K8S_ROOT/_output/bin/kube-apiserver` | exact binary to run |
| `ETCD_BIN` | `$K8S_ROOT/third_party/etcd/etcd` | etcd binary |
| `APISERVER_EXTRA_FLAGS` | empty | extra apiserver flags, e.g. feature gates |
| `APISERVER_GOMAXPROCS` | `0` (all cores) | apiserver GOMAXPROCS |
| `RUN_DIR` | `/tmp/watch-fanout` | etcd data, certs, kubeconfig, logs |
| `SECURE_PORT` / `ETCD_PORT` | `6443` / `2379` | ports, for running two rigs at once |
| `APISERVER_V` | `2` | klog verbosity |

Point it at a branch build:

```bash
cd ~/workspace/kubernetes && git checkout my-fix
go build -o /tmp/apiserver-fix ./cmd/kube-apiserver
APISERVER_BIN=/tmp/apiserver-fix \
  APISERVER_EXTRA_FLAGS="--feature-gates=MyGate=true" \
  ./hack/local-rig.sh
```

### A/B-ing a change

Build both binaries first, then swap only the apiserver between runs. etcd data
lives in `$RUN_DIR`, so the object population survives a restart and
`preload` / `pods` only need to run once:

```bash
cd ~/workspace/kubernetes
git checkout master  && go build -o /tmp/apiserver-base ./cmd/kube-apiserver
git checkout my-fix  && go build -o /tmp/apiserver-fix  ./cmd/kube-apiserver

APISERVER_BIN=/tmp/apiserver-base ./hack/local-rig.sh   # preload once, measure
APISERVER_BIN=/tmp/apiserver-fix  ./hack/local-rig.sh   # restart, measure again
```

Compare `wf_delivery_latency_seconds` between runs. Interleave the arms rather
than running all of one then all of the other: on this workload the p99 drifts
enough between runs that a single A-then-B comparison is not trustworthy.

Two binaries can also run side by side on different `SECURE_PORT` / `ETCD_PORT`
/ `RUN_DIR` values, at the cost of splitting the machine's cores between them.

## Measuring delivery latency

`wf_delivery_latency_seconds` is the reason this rig exists. The writer stamps
`watch-fanout/write-ts` into the object immediately before `Update`, and
`--mode=decode` clients record `time.Since` on receipt.

The apiserver exposes **no per-resource watch delivery latency metric**, so this
number cannot be obtained on a real cluster at all. Here both ends share one
host and one clock, so it is exact. Run a large `--mode=raw` pool to carry the
fan-out and a small `--mode=decode` pool as the probe: raw clients are 3x
cheaper, and the probes stay fast enough to measure the apiserver rather than
themselves.

## One connection per simulated node

This is the detail that makes or breaks the rig. client-go caches transports on
a key that includes the `*DialHolder` pointer
(`client-go/transport/cache.go:264-271`), and `rest.Config` only allocates a
DialHolder when `Dial` is non-nil (`client-go/rest/transport.go:116-118`).
Clients that leave `Dial` nil all share one transport, and HTTP/2 then
multiplexes every watch onto `ceil(N/250)` connections -- 5,000 clients would
quietly run on 20 sockets while looking like they were working.

`perClientConfig` gives every client its own dial closure. Verify it:

```bash
ss -tn state established | grep -c 127.0.0.1:6443   # 2x N on loopback
```

## Exercising APF

By default every request here authenticates as `system:masters`, which is
**exempt from flow control**, so no priority level is ever exercised and APF
behaviour cannot be observed. `kubelet` fixes that by giving each simulated node
its own `system:node:<name>` identity in group `system:nodes`, which routes to
the `node-high` priority level via the `system-node-high` flow schema:

```bash
watch-fanout kubelet --write-tokens=/tmp/watch-fanout/certs/tokens.csv \
  --admin-token=$TOKEN --nodes=5000     # then restart the apiserver
watch-fanout kubelet --kubeconfig=$KC --nodes=5000 --renew=10s
```

Distinct identities matter: APF's flow distinguisher shards queues by user, so a
single shared identity would collapse 5,000 kubelets into one queue.

## Drain modes

Comparing CPU across modes decomposes what a real kube-proxy spends its
storm-time CPU on.

| mode | what it does |
| --- | --- |
| `raw` | reads the watch stream, discards bytes, never decodes |
| `decode` | decodes each event into a typed object and drops it |
| `informer` | real SharedInformer with a store and a no-op handler -- the hollow-proxy equivalent |

200 clients, 35.6 events/s each:

| mode | mCPU/client | us/event | RSS/client |
| --- | --- | --- | --- |
| `raw` | 0.68 | -- | 0.5 MB |
| `decode` | 2.02 | 56.8 | 0.5 MB |
| `informer` (no resync) | 4.22 | 118.5 | 119 MB |
| real kube-proxy, gce-5k at storm peak | 244 | -- | 259 MB |

The gap between `informer` and real kube-proxy is proxier work -- endpointsMap
and serviceMap diffing, rule synthesis -- not watch handling. **Watch handling
is ~2% of a kube-proxy's storm CPU.**

`--idle-watches=N` adds N single-object ConfigMap watches per node, copying
kubelet's watch-based manager
(`pkg/kubelet/util/manager/watch_based_manager.go:222-241`). Those objects are
never written, so the flag raises goroutine population without adding fan-out.

## What moves delivery latency, and what does not

Measured at 5,200 watchers and 33 events/s on a 16-core workstation.

**Write throughput dominates.** Holding fan-out fixed and varying only kubelet
lease writes -- writes that reach almost no watchers:

| lease writes/s | mean | >250ms | >1s | apiserver cores |
| --- | --- | --- | --- | --- |
| 0 | 21.3ms | 0.90% | 0.000% | 8.39 |
| 503 | 63.7ms | 7.88% | 0.397% | 9.05 |
| 1,434 | 669ms | 96.5% | 11.6% | 9.53 |
| 2,095 | 778ms | 96.2% | 23.5% | 10.02 |

Apiserver CPU rises 19% across that range, so this is not CPU exhaustion;
`go_sched_latencies` mean goes 408us to 1,195us over the same span.

**Fan-out width matters, less sharply.** 200 to 5,200 watchers at a constant
event rate moves p50 from 3.9ms to 24.4ms.

**These do not move it**: goroutine population (38.5k to 121.7k apiserver
goroutines changed nothing), apiserver `GOMAXPROCS` (flat from 16 to 96), heap
size, LIST concurrency, and slow clients -- crippling the client pool produced
5,000 terminated watchers while healthy clients got *faster*.

## Gotchas

- `--drain-delay` sleeps per **read**, not per event. A 32KB read carries
  several events, so small values understate the intended slowdown.
- `apiserver_watch_cache_events_dispatched_total` counts **events**, not
  deliveries (`cacher.go:946`), and increments even for bookmarks that are never
  dispatched. There is no delivery counter; derive deliveries as
  events x watchers.
- Creating pods is fast on an idle box (~2,400/s) and very slow under fan-out
  load, which starves the write path.
- `list` streams and discards on purpose. Decoding a 150k-pod LIST materialises
  every pod client-side, which is gigabytes per worker.

## Scope

This rig isolates the apiserver's watch and request paths. It has no
kube-controller-manager, so namespace deletion does not reap objects. It runs
over loopback, so anything bound by real network behaviour will not appear.
Never quote an SLO number from it: use it for mechanism and for relative A/B of
a change, and use a real scale run for pass/fail.
