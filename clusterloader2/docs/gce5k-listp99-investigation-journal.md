# gce-5k cluster LIST-pods p99 SLO failures

Job: `ci-kubernetes-e2e-gce-scale-performance-5000` (testgrid `gce-master-scale-performance-5000`), the main release-blocking GCE 5,000-node periodic. Not the `-experimental`, `resource-size`, or presubmit variants.

Window: 2026-04-15 to 06-21.

---

## Summary

- The job intermittently fails one SLO: cluster-wide pod LIST p99 over 30s (the `APIResponsivenessPrometheusSimple` measurement). Failing runs read 35 to 58s, passing runs 19 to 30s.
- When it fails, the apiserver is barely busy: 14 to 16 of 96 cores in use, about 80 idle, GC 0.3% of CPU. It is not CPU-bound, not GC, not etcd, not a code change.
- The cores sit idle because the apiserver is buried in goroutine wakeups from watch traffic. One endpointslice change wakes every client that watches them, about 5,300 here: a kube-proxy on every node plus a coredns pod per 16 nodes. Over a run that is 261K endpointslice changes turning into 680M wakeups, 77% of all watch wakeups. The Go scheduler can't get goroutines onto cores fast enough, so runnable work piles up while cores are free.
- A single cluster-wide pod LIST (about 150k pods) gets pushed off the CPU hundreds to thousands of times while it waits in that backlog, so about 10s of real work stretches to 30 to 50s. Pods aren't the cause: a pod change wakes only about 3 watchers (each kubelet watches its own node). The pod LIST is just the request that gets measured.
- Pass or fail comes down to how long a run stays in this slow state, not how heavy the load is. Every run hits it; passing runs recover in a few minutes, failing runs stay slow for 20 to 30 minutes.
- `CL2_REALISTIC_POD` (on this job since 04-27) made it worse, not new. It raised endpointslice changes from about 64K to 90 to 125K per run. Failures predate it (a 37s LIST fail on 04-17), and pod size did not change (about 6.5KB before and after).

## The failures

p99 lands in three bands:

- about 19.5 to 22.3s: 7 runs, all pass.
- about 28.3 to 30.6s: 15 runs, sitting just under the 30s line. This was the steady level all April.
- 35 to 58.3s: 12 runs, all fail.

Failing runs also take much longer end to end: 190 to 231 min vs 155 to 180 min for passing runs. Run length and p99 move together (correlation 0.91), two views of the same slowdown.

Timeline: about 29s, just under the line, through 04-29. Starting 05-01 some runs jump into the 35 to 58s band. Before that, zero LIST failures in about 26 April runs; after, about half fail. June was worse: 7 of 11 failed, four in a row from 06-15 to 06-21. All on etcd 3.6.12.

The clearest pass/fail signal in the data is forced watcher closes on endpointslices: 39 to 84k on failing runs, under 27k on passing ones. Per-run numbers are in the appendix.

## Go scheduler wake storm

One endpointslice change wakes every client watching it, 5,317 here: a kube-proxy on every node (about 5,000) plus a coredns pod per 16 nodes (about 313). Each is parked on its own goroutine:

```
etcd event → cacher (1 dispatcher goroutine)
           → up to 5,317 watcher input channels  [sends]
           → up to 5,317 watcher goroutines wake  [wakeups]
           → encode the endpointslice, write each stream
           → up to 5,317 http2 serve goroutines   [more wakeups]
           → sockets
```

Each wakeup does almost no work (encode one small object, one write). The cost is the wakeup itself, and there are 261K endpointslice changes per run, which produce 680M wakeups, 77% of all watch wakeups on the apiserver.

That buries the Go scheduler:

- `go_sched_goroutines_runnable` about 1,800 to 2,200: that many goroutines are ready to run and waiting for a core, while only 14 to 16 of 96 cores are running anything and about 80 sit idle. The idle cores stay idle because of how Go hands out work. To run a ready goroutine on a free core, the runtime has to wake a parked OS thread and pair it with that core, and that handoff goes through a single global scheduler lock. At this wakeup rate the lock is the bottleneck, so goroutines pile into the run queue faster than the runtime can place them on the idle cores.
- CPU profile: `selectgo` 46%, lock contention 28%, actual watch work about 10%, http2 about 7%, GC 0.3%. The cycles go to parking and waking goroutines, not to serving requests. Not CPU-bound, not GC.
- Queue wait `go_sched_latencies_seconds` (mean): about 800 to 1,060µs failing vs about 200 to 540µs passing, tail tens of ms.

The pod LIST the SLO measures is the casualty, not the cause. A cluster-wide LIST returns 150 to 232MB and gets pushed off the CPU hundreds to thousands of times while it waits in that queue (HTTP/2 window stalls plus about 10ms preemption slices), tens of ms lost each time, so about 10s of real work stretches to about 44s. A small GET takes the same path but requeues only a few times.

What drives the wakeups, per resource (from the end-of-run metrics dump):

| resource | changes | wakeups | wakeups per change | share of wakeups |
|---|---|---|---|---|
| endpointslices | 261K | 680M | about 2,600 | about 77% |
| services | 35K | 169M | about 4,870 | about 19% |
| pods | 3.8M | 11.6M | about 3 | about 1.3% |
| configmaps | 89K | 7.6M | about 86 | <1% |
| leases | 9.3M | 4.7M | about 0.5 | <1% |
| nodes | 464K | 3.9M | about 8 | <1% |

Endpointslices is the driver: every kube-proxy (one per node) and coredns pod (one per 16 nodes) watches all of them, unfiltered, 5,317 watchers in all, so one change wakes thousands. Both counts scale with the cluster. (The per-change average in the table is about 2,600 rather than the full 5,317 because these clients connect gradually as nodes come up.) Services looks high but that is almost all periodic keepalive traffic, not real changes, so it doesn't drive the storm. Pods change the most (3.8M) but each wakes only about 3 watchers because kubelets filter to their own node, which is why the pod LIST is the victim stuck behind the endpointslice traffic, not the cause.

Why some runs fail and others don't: every run enters this slow state. What decides the outcome is how long it stays, and that is set by a feedback loop:

1. slow apiserver, so the controller manager's calls take 4 to 9x longer (replicaset work p99 9.0s when failing vs 0.98s when passing)
2. so it creates pods slower (48 to 73/s vs 94 to 164/s)
3. so clusterloader2 waits longer for all about 16,150 deployments to come up before it starts tearing down
4. so the big teardown delete, the thing that would clear the watch traffic, happens about 32 min later
5. so the watches that feed the slowdown stay up longer, and the slowdown keeps itself going

A passing run breaks out in a few minutes; a failing one stays stuck 20 to 30 min. Most of that extra time is the loop, not extra load.

## CL2_REALISTIC_POD

Turned on for this job on 04-27 (PR #36900).

It did not make pods bigger. Per-pod size is about 6.5KB before and after. The largest total pod data is on a passing run (1604 MiB) and the worst failing run has the smallest (1432 MiB), so a bigger LIST is not it.

It did step up apiserver heap, even at flat size. Peak heap (`go_memstats_heap_inuse_bytes`, the top of the GC sawtooth) was about 45 to 48GB before the flag (44.7, 45.7, 45.4, 46.2, 47.7GB on 04-17 to 04-25) and about 53 to 61GB after, a roughly 10GB step that holds on both passing and failing runs (peak heap does not split pass/fail, see the appendix). The cause is the object graph, not the bytes. Comparing a before heap profile against an after one, the growth is all structural: the old 6KB env-var padding decoded into one blob (EnvVar unmarshal down about 2.0GB of live heap), while the realistic pod's 4 containers (2 init, main, sidecar) decode into many small objects (Container, ObjectMeta, ContainerStatus, ResourceRequirements, PodStatus, VolumeMountStatus, EnvVarSource all up). Same serialized size, more live Go objects, so more to allocate and the sawtooth peaks higher. The init containers also add status transitions, so each pod passes through the apiserver watch-decode path more times: that path (watchChan.serialProcessEvents) went from 25 to 36% of the live heap.

What it changed: the pod now has 2 init containers (each sleeps 1s) and a sidecar, so pods take longer to go ready and flip ready/not-ready more often. That is more endpointslice changes. Measured, endpointslice changes went from about 64K to 90 to 125K per run, stepping up exactly when the flag turned on (04-27 on this job, 04-20 on the resource-size job, each on its own date, so it is the flag and not a Kubernetes change). More endpointslice changes means more wakeups, which lengthens the slow state.

The extra endpointslice writes on failing runs (about 1.8x) are mostly a side effect of the slow state, not extra real work. The real endpoint changes are about equal (62k failing vs 68k passing), but the slow apiserver makes the controller write them in more, smaller updates (1.05 endpoints per update vs 2.89). So the slow state partly manufactures its own extra traffic.

## Appendix: per-run numbers (04-25 to 06-21, 2026)

`eps watch events` is endpointslice watch events delivered after fan-out. `all watch events` is the same summed over every resource (endpointslices is most of it). `eps relists` is forced watcher closes on endpointslices. `sched lat` is mean scheduler latency. `peak heap` is the max of `go_memstats_heap_inuse_bytes` over the run (apiserver, from the prometheus snapshot), the top of the GC sawtooth, which lands mid-load during the create storm. The other counts are cumulative and failing runs run about 1.3x longer, so trust the trend within a column, not the absolute totals. They come from a single end-of-run scrape of one of the 3 masters.

| date | result | dur (min) | eps PUT | eps watch events | all watch events | watch 429s | pod relists | eps relists | sched lat (µs) | mutex wait (s) | peak heap GB |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 2026-04-25 | PASS | 163 | 47.3k | 334.1M | 544.5M | 57.8k | 467 | 6.2k | 520 | 414.1k | 47.7 |
| 2026-04-27 | PASS | 165 | 59.9k | 400.4M | 608.2M | 78.9k | 485 | 6.8k | 423 | 574.2k | 53.5 |
| 2026-04-29 | PASS | 175 | 77.1k | 492.4M | 700.5M | 124.8k | 328 | 5.3k | 536 | 833.7k | 53.7 |
| 2026-05-01 | FAIL | 218 | 103.1k | 628.4M | 843.7M | 505.6k | 640 | 49.8k | 1033 | 2.3M | 56.9 |
| 2026-05-03 | FAIL | 193 | 101.0k | 616.5M | 828.8M | 274.9k | 471 | 46.1k | 787 | 1.5M | 55.8 |
| 2026-05-05 | FAIL | 205 | 109.2k | 660.3M | 875.7M | 258.6k | 709 | 41.6k | 877 | 1.7M | 53.9 |
| 2026-05-07 | PASS | 181 | 84.7k | 530.4M | 739.2M | 141.8k | 461 | 27.2k | 638 | 1.1M | 60.7 |
| 2026-05-09 | PASS | 156 | 55.3k | 374.4M | 585.6M | 48.3k | 283 | 4.4k | 229 | 380.4k | 56.2 |
| 2026-05-11 | PASS | 176 | 76.1k | 486.9M | 695.6M | 112.4k | 286 | 10.9k | 512 | 813.6k | 53.4 |
| 2026-05-13 | FAIL | — | — | — | — | — | — | — | — | — | — |
| 2026-05-15 | PASS | 160 | 46.4k | 327.3M | 539.3M | 40.8k | 388 | 78 | 210 | 348.8k | 55.1 |
| 2026-05-17 | PASS | 172 | 72.9k | 468.4M | 675.8M | 115.0k | 488 | 13.9k | 565 | 836.7k | 57.9 |
| 2026-05-19 | PASS | 156 | 48.8k | 340.3M | 551.9M | 45.2k | 319 | 1.9k | 230 | 356.3k | 54.8 |
| 2026-05-21 | PASS | 171 | 63.3k | 418.6M | 626.2M | 96.6k | 647 | 18.8k | 533 | 732.6k | 59.9 |
| 2026-05-23 | FAIL | 206 | 111.0k | 669.9M | 886.2M | 445.4k | 654 | 68.7k | 873 | 1.9M | 56.0 |
| 2026-05-25 | FAIL | 203 | 115.1k | 690.7M | 907.1M | 346.7k | 633 | 39.2k | 881 | 1.9M | 54.8 |
| 2026-05-27 | PASS | 154 | 48.6k | 339.3M | 550.9M | 42.3k | 345 | 1 | 204 | 342.3k | 54.5 |
| 2026-05-29 | PASS | 158 | 48.2k | 337.1M | 548.8M | 41.8k | 318 | 1.6k | 215 | 351.7k | 54.1 |
| 2026-05-31 | PASS | 164 | 62.9k | 416.9M | 624.6M | 91.9k | 513 | 18.2k | 520 | 729.1k | 59.8 |
| 2026-06-01 | PASS | 158 | 49.5k | 343.7M | 555.8M | 40.8k | 324 | 1.7k | 213 | 353.4k | 53.6 |
| 2026-06-03 | FAIL | 208 | 111.8k | 673.5M | 890.5M | 269.6k | 628 | 83.8k | 846 | 2.0M | 56.0 |
| 2026-06-05 | PASS | 170 | 65.7k | 430.8M | 638.2M | 97.2k | 635 | 17.9k | 508 | 763.7k | 60.1 |
| 2026-06-07 | PASS | 154 | 47.7k | 334.3M | 545.6M | 43.9k | 356 | 1.7k | 217 | 370.2k | 54.7 |
| 2026-06-09 | FAIL | 231 | 106.7k | 646.8M | 866.0M | 618.8k | 620 | 71.7k | 1063 | 2.7M | 58.0 |
| 2026-06-11 | FAIL | 180 | 78.3k | 498.6M | 706.3M | 129.5k | 508 | 41.0k | 603 | 1.1M | 61.4 |
| 2026-06-13 | PASS | 169 | 73.4k | 473.0M | 680.0M | 107.0k | 283 | 4.8k | 524 | 763.8k | 56.6 |
| 2026-06-15 | FAIL | 205 | 110.9k | 668.7M | 882.5M | 277.5k | 677 | 53.0k | 875 | 1.9M | 58.8 |
| 2026-06-17 | FAIL | 196 | 89.0k | 555.1M | 765.5M | 374.1k | 855 | 46.1k | 896 | 1.6M | 60.1 |
| 2026-06-19 | FAIL | 205 | 113.1k | 680.0M | 881.7M | 500.9k | 305 | 56.8k | 927 | 2.2M | 58.5 |
| 2026-06-21 | FAIL | 202 | 115.1k | 691.1M | 892.4M | 442.2k | 422 | 52.5k | 868 | 2.1M | 55.8 |
