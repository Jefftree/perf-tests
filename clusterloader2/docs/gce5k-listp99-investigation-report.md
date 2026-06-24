# gce-5k cluster-LIST-pods p99 SLO: investigation report

**A/B straddling the realistic-pod flip (04-27, PR #36900):** main `ci-kubernetes-e2e-gce-scale-performance-5000`, single master, 96 cores, etcd 3.6.12.
**CONTROL** = 04-25 build `2048085020685373440` (realistic-pod OFF, no init containers, PASS).
**PASS** = 04-29 build `2049534553801887744` (realistic ON, +2 days, PASS).
**FAIL** = 05-01 build `2050259337653260288` (realistic ON, +4 days, FAIL).
**Question:** why does the cluster-LIST-pods p99 > 30s SLO fail on one run and pass on the other when the inputs are nearly identical — and when did it start? The flip flipped a stable test into a bistable one within days; the failure mode here is the same one still firing in June.

---

## 1. The number the gate reads is inflated near the threshold

The gate is `APIResponsivenessPrometheusSimple` (enableViolations=true), p99 of `apiserver_request_duration_seconds{verb=LIST,resource=pods,scope=cluster}`, threshold 30s, allowedSlowCalls=1.

The SLI histogram has buckets **30 -> 45 -> 60s with no edge in between**, so a graded p99 cannot resolve to the second inside those 15s-wide gaps. The clean, unambiguous discriminator at the gate is **slowCount** (count above the le=30 bucket):

- **04-29 PASS: graded p99 29.8s, slowCount 4.** Sits right on the 30s line.
- **05-01 FAIL: graded p99 49.7s, slowCount 53.** The metric timeline reaches the **60s top bucket** — a genuine deep fail, not interpolation.

That the metric inflates *near* 30s was proven directly on the marginal June fail 06-21: httplog showed a 44.8s metric reading was really **30.93s true (zero requests >=45s)** — pure bucket interpolation. 05-01 is the opposite end: its slow lists genuinely span 45-60s. Either way, p99 is the wrong gate number; **slowCount (4 vs 53) separates cleanly.** Read logs, not the metric, for true latency.

---

## 2. Every signal we examined (selector -> numbers -> evidence -> verdict)

One subsection per metric. Each gives the exact selector or log grep, the three-run numbers (04-25 CONTROL / 04-29 PASS / 05-01 FAIL), a granular evidence line, and a verdict: **discriminates** (separates pass from fail), **detects** (sees the regime but present in both), or **ruled out**.

*Provenance: PromQL selectors and counter deltas below are exact and re-runnable against the loaded snapshots. Inputs are reported as matched-instant RATES (cumulative counters are dwell-inflated for the FAIL, which ran ~1 hr longer — flagged where it matters). Per-request `httplog`/`Trace` lines are shown in their real apiserver format; the line-level latency ground truth (2.3, 2.19) is from the 06-21 httplog grep (the only intact line-level capture; 05-01's apiserver.log is 9.7 GiB, not grepped). Ask to grep verbatim.*

### 2.1 cluster LIST-pods p99 (metric) — the gate, near-threshold-inflated
`histogram_quantile(0.99, sum by(le)(rate(apiserver_request_duration_seconds_bucket{verb="LIST",resource="pods",scope="cluster"}[5m])))`
**Graded p99: 04-25 29.1s · 04-29 29.8s · 05-01 49.7s** (05-01 timeline touches the 60s top bucket).
Evidence: 04-29's slow lists cluster at 30.0-30.9s (interpolated up); 05-01's spread into the (45,60] bucket.
Verdict: **discriminates** but the *value* is bucket-quantized; gate on slowCount (2.2), not this.

### 2.2 slowCount (>30s tail) — the honest discriminator
`sum(...le="+Inf") - sum(...le="30")` at end of run.
**04-25 3 · 04-29 4 · 05-01 53.** totalLISTcount 561 / 567 / 529.
Evidence (PromQL, actual): gate passes when slowCount<=1 OR p99<=30s; 04-25/04-29 pass on p99 (~29-30s), 05-01 fails on both. 05-01's 53 matches the historical SLO artifact.
Verdict: **discriminates** cleanly — the number to gate on.

### 2.3 LIST latency decomposition — where the ~31s goes
Source: apiserver `Trace` on the slow `watch-list` LIST (served from watch cache, `Cacher.GetList`). Run-invariant (serving path; same 991 MB list in all three).
**83-88% of the ~31s is the gzip-compressed write** (133 MB wire / ~1.4 GB serialized protobuf); ~3.4s marshal; deep-copy per pod.
Evidence (Trace, format real; latencies from the 06-21 grep):
```
Trace "List" verb=LIST URI=/api/v1/pods?limit=500&resourceVersion=0 userAgent=watch-list:
  "Listing from storage done"        3.4s
  "Writing http response" 133MB     27.1s   (88%)
  total 30.9s
```
Verdict: explains **why** the probe LIST is slow; not a pass/fail discriminator.

### 2.4 LIST response size — population proxy, with a dilution trap
mean = `apiserver_response_sizes_sum / _count{...}`; true peak = p99 of `apiserver_response_sizes_bucket`.
**Mean peak (diluted): ~56 / ~32 / ~31 MB.** **p99 (true): 991 / 989 / 991 MB.**
Evidence: label-selected scheduler-throughput LISTs dilute the mean; p99 pins at ~990 MB in every run = same full unfiltered list.
Verdict: **ruled out as a difference** (identical ~990 MB). Do NOT use the mean for population; use p99 or pod count (2.5).

### 2.5 pod count -> time at maxed pod size — the exposure window
`sum(apiserver_resource_objects{resource="pods"})` over time; duration at >=95% of peak. (`apiserver_storage_objects`/`etcd_object_counts` are empty here.)
**Peak 166k / 168k / 170k. Time at >=95% peak: 13 min / 14 min / 24 min.**
Evidence (PromQL range, actual): peaks at `18:36 / 18:42 / 19:04 (late)`; FAIL still >=95% at 19:20.
Verdict: **discriminates** PASS vs FAIL (slow LISTs need maxed size; FAIL dwells there ~10 min longer). Does NOT separate CONTROL from PASS (both ~13-14 min).

### 2.6 open watches — convoy population
`sum(apiserver_longrunning_requests{verb="WATCH"})`.
**Peak 193k / 178k / 175k.** Baseline ~51.9k.
Evidence: CONTROL ramps to 193k then drains by 18:50; FAIL holds ~172-175k from 18:50-19:22.
Verdict: **detects** the regime; CONTROL has the *most* watches yet passes -> convoy size is not destiny (see 2.10).

### 2.7 endpointslice churn (events_received) — the wake-storm driver
`sum(rate(apiserver_watch_cache_events_received_total{resource="endpointslices"}[2m]))`; cumulative = counter delta over load window.
**Cumulative: 63.5K / 86.5K / 107.2K.** Matched-instant peak RATE: ~38 / ~44 / ~34 /s.
Evidence (counter delta vs `increase()` cross-check, no apiserver restart): `eps raw-delta=107.2K vs increase()=107.3K` (agree to 0.1K).
Verdict: **regime driver** (realistic-pod init-container readiness flapping steps it ~30% at the flip). The FAIL's higher *cumulative* is partly dwell feedback (slow->resync->more eps); at matched instants its RATE is the *lowest* (34/s). Does NOT separate pass from fail by input.

### 2.8 pod churn (events_received)
`sum(apiserver_watch_cache_events_received_total{resource="pods"})` delta.
**1.90M / 1.8M / 1.8M.**
Evidence: ~1.8-1.9M in all three once writes land; not a discriminator (the June FAIL's apparent low pod churn was a window-truncation artifact, recovered here because 05-01 ran long).
Verdict: **ruled out** — no input difference.

### 2.9 services churn (events_received)
`sum(apiserver_watch_cache_events_received_total{resource="services"})` delta.
**16.2K / 9.4K / 4.2K.**
Evidence: CONTROL has the most; FAIL the least.
Verdict: **minor / ruled out** — does not track pass/fail.

### 2.10 total fanout dispatch (watch_events_total) — wake-storm magnitude
`sum(apiserver_watch_events_total)` delta; matched-instant rate for the true comparison.
**Cumulative: 534.7M / 581.9M / 640.0M.** Matched-instant RATE: ~210k / ~228k / ~164k /s.
Evidence: by *rate* the FAIL fans out the **least** (164k/s vs 228k/s); its bigger cumulative is purely the ~1 hr longer dwell.
Verdict: **ruled out as the difference** — magnitude is not destiny; the FAIL's instantaneous storm is the smallest. Dynamics are.

### 2.11 go scheduler latency (mean) — convoy depth
`1000 * rate(go_sched_latencies_seconds_sum{job="master",endpoint="apiserver"}[2m]) / rate(..._count[2m])` (ms).
**0.4-0.86 ms / 0.4-0.68 ms / 1.0-1.25 ms sustained.**
Evidence (matched instants): PASS 0.43-0.68 ms vs FAIL 1.07-1.19 ms with strictly lower input.
Verdict: **discriminates** — ~2x sched delay with lower input is the core anomaly (mechanism, not "stochastic").

### 2.12 runnable backlog — goroutines waiting for a P
`max(go_sched_goroutines_runnable_goroutines{job="master",endpoint="apiserver"})`.
**Peak ~7.7k (PASS) vs ~9.0k (FAIL); median 1665 vs 2120** over the convoy window.
Evidence: spiky gauge; FAIL's distribution sits higher, but use sched latency (2.11) as the robust convoy signal — a single-instant runnable read is noisy.
Verdict: **detects** (FAIL modestly higher); lean on 2.11.

### 2.13 go_sched_latencies histogram (p99) — a caveat, not a tail
`histogram_quantile(0.99, sum by(le)(rate(go_sched_latencies_seconds_bucket[2m])))`.
Coarse 8-bucket histogram, top finite `le=117ms`; p99 sits in the 10.5-117ms mega-bucket, flat stall-vs-recovery.
Evidence: caps ~117 ms in all runs; cannot resolve the multi-second tail.
Verdict: **caveat** — cite the mean (2.11), not this p99.

### 2.14 CPU + block pprof — parked, scheduler-bound, GC refuted
`cpu-attr.sh <run> --vs <pass>` over the `kube-apiserver_CPUProfile_load` artifact.
**cores-busy 13.0/96 (04-29) vs 15.8/96 (05-01); scheduler 101.6s vs 109.6s; GC 0.7% vs 0.5%; selectgo cum 45% vs 39%.**
Evidence (cpu-attr.sh, actual, 05-01): `SCHEDULER (wake-storm) 109.61s 23.1%  GC 2.38s 0.5%  cores-busy 15.8/96  selectgo cum 38.65%`.
Verdict: **proves the mechanism + refutes GC**; both parked (~13-16/96), same signature -> does NOT separate pass/fail.

### 2.15 APF system-PL inqueue — the backlog gauge
`sum(apiserver_flowcontrol_current_inqueue_requests{priority_level="system"})`; saturation = duration above 1000.
**~0 (one 2.9k spike) / saturated ~19 min then drains / saturated ~46 min sustained.**
Evidence: both PASS and FAIL queue during the create ramp; only the FAIL stays saturated (46 vs 19 min).
Verdict: **discriminates** — saturation *duration* is the cleanest collapse signal (plot vs admit, 2.16).

### 2.16 APF system-PL admit rate — the service rate / the collapse
`sum(rate(apiserver_flowcontrol_dispatched_requests_total{priority_level="system"}[2m]))`. Arrival ~1900/s.
**burst 4787 / burst 6864 then drains / stuck ~1300, never bursts.**
Evidence: PASS admit > arrival -> drains; FAIL admit < arrival -> self-sustains.
Verdict: **discriminates** — service rate crossing below arrival IS the congestion collapse.

### 2.17 APF rejections (429) — overflow, in both but FAIL sustains
`sum(rate(apiserver_flowcontrol_rejected_requests_total{priority_level="system"}[2m]))`; per-request `resp=429`.
**~0 / ramp burst to 1010/s then stops / burst to 2084/s sustained.**
Evidence (httplog, format real):
```
verb=PATCH URI=/api/v1/nodes/<node>/status apf_pl=system apf_fs=system-nodes
  resp=429 latency=2.4s  reason="request queue full, rejected"  Retry-After=1
```
Verdict: **discriminates by duration** — both reject during the ramp; the FAIL's 429s persist and the retries re-arrive, deepening the queue (positive feedback).

### 2.18 APF seat-hold + concurrency — turnover-starved, not seat-starved
`sum(apiserver_flowcontrol_current_executing_requests{priority_level=~"system.*"})` (concurrency) and `request_execution_seconds`; seat-hold = concurrency / admit.
**Concurrency 482 (PASS) vs 446 (FAIL) — same.** Seat-hold **146 -> 337 ms** (Little) and exec metric **27.1 -> 77.6 ms** — both ~2.3-2.9x.
Evidence (PromQL, actual @ matched peak): `04-29: exec 27.1ms conc 482 admit 3312/s` ; `05-01: exec 77.6ms conc 446 admit 1322/s`.
Verdict: **discriminates the mechanism** — same seats / ~2.5x hold = ~2.5x lower admit. The extra hold is wall-clock descheduling, not CPU.

### 2.19 APF wait-vs-exec split — proof it is queue wait, not work
Aggregate: `request_wait_duration_seconds` vs `request_execution_seconds` (system PL). Per-request httplog: `fl_priorityandfairness` (`filterlatency.go:70`) vs `apf_execution_time` (`apf_filter.go:189`).
**Wait is 97% (04-29) / 96% (05-01) of (wait+exec).** Queue wait 855 ms (PASS) vs **1943 ms** (FAIL); exec 27-78 ms.
Evidence (httplog, format real; latencies from the 06-21 grep):
```
verb=PATCH URI=/api/v1/namespaces/.../pods/<pod>/status apf_pl=system apf_fs=system-nodes
  fl_priorityandfairness=2.351s  apf_execution_time=48ms  latency=2.4s
```
Verdict: **proves** the latency is APF queue wait, not execution -> the throttled class is kubelet writes, not the LIST.

### 2.20 workload-high (KCM pod-creates) — the control that is NOT saturated
`sum(apiserver_flowcontrol_current_executing_requests{priority_level="workload-high"})` vs nominal 138.
**8 (PASS) / 10 (FAIL) of 138 seats.**
Evidence: pod-create POSTs are never seat-starved in either run.
Verdict: **control** — isolates the throttle to `system` (kubelet status), not creates.

### 2.21 heap inuse — realistic-pod step, ruled out as cause
`max(go_memstats_heap_inuse_bytes{job="master",endpoint="apiserver"})/1e9`.
**Peak 43 / 51 / 52 GB.**
Evidence: the ~8 GB CONTROL->June step is realistic-pod object-graph fragmentation; PASS and FAIL are ~equal.
Verdict: **ruled out** — heap does not separate pass from fail.

### 2.22 GC rate / assist — ruled out
`rate(go_gc_duration_seconds_count[2m])`; assist from pprof.
**~0.5-0.7% of CPU in both** (2.14).
Evidence: identical PASS vs FAIL; GC-assist thesis refuted at metric and CPU level.
Verdict: **ruled out**.

### 2.23 etcd request latency (apiserver-side) — convoy-confounded, not the driver
`histogram_quantile(0.99, sum by(le)(rate(etcd_request_duration_seconds_bucket[2m])))` (client-side; etcd server not scraped).
**p99 84 ms (PASS) vs 305 ms (FAIL)** at matched peak.
Evidence: the FAIL's higher number is the apiserver's own etcd-client goroutine descheduled by the same convoy, not etcd disk; etcd.log apply stayed healthy in the June A/B.
Verdict: **ruled out as driver** — a symptom of the convoy, same as everything else client-side.

### 2.24 co-tenant CPU (KCM / scheduler / etcd) — ruled out
`rate(process_cpu_seconds_total{endpoint=~"kube-controller-manager|kube-scheduler"}[2m])`.
**KCM 2.2 / 2.9 cores; scheduler 3.0 / 1.9 cores;** apiserver parked ~13-16/96 in both.
Evidence: no co-tenant hogs the box; ~80 cores idle next to the parked apiserver.
Verdict: **ruled out** — not a noisy-neighbor story.

### 2.25 KCM create throughput + replicaset workqueue — the mediator, downstream of APF
`sum(rate(rest_client_requests_total{endpoint="kube-controller-manager",method="POST",code="201"}[2m]))`; `workqueue_work_duration_seconds` p99 for `replicaset`.
**Create POST-201: 143/s (PASS) vs 54/s (FAIL); replicaset sync p99: 1.0s (PASS) vs 3.8s (FAIL).** KCM CPU ~2-3 cores both (idle-waiting).
Evidence: KCM is idle-WAITING on the throttled apiserver, not CPU-bound -> a *victim* of the APF throttle, not a driver.
Verdict: **downstream** — the create-completion barrier clears late because kubelet status is throttled (the feedback loop, 6).

---

## 3. What the slow LIST actually is

Every slow cluster LIST-pods is the `CL2_ENABLE_INFORMER_LATENCY_TEST` **watch-list probe**: userAgent `watch-list`, full **unpaginated rv=0** `/api/v1/pods?limit=500&resourceVersion=0`. This is what old controllers do (pagination stays off the table). It is a single informer pod (`replicasPerNamespace: 1`, `watch-list --count=1 --resource=pods`), so it is a strictly serial relist loop ("pod relist rate" is just 60/latency, not an independent driver, and there is never >1 identical list in flight).

Clusterloader's own measurement LISTs are label-selected (`group=scheduler-throughput`, APF-exempt) and run ~255ms. They are not the slow ones.

Trace breakdown of the ~31s probe LIST (served from the watch cache, `Cacher.GetList`):
- **83-88% is the gzip-compressed write** (133 MB wire from ~1.4 GB serialized protobuf)
- deep-copies every pod, re-marshals, re-gzips on **every** request
- no list-level response cache exists; gzip is already level-1/BestSpeed (least CPU)

The probe LIST is slow because it runs on one CPU-starved goroutine while the wake-storm convoy holds the scheduler.

## 4. Ruled out as the pass/fail cause (matched-instant inputs)

At matched full-population instants, the **FAIL run has the lower / calmer inputs** (rates, not dwell-inflated cumulatives):

| signal (matched instant) | FAIL (05-01) | PASS (04-29) |
|---|---|---|
| eps churn rate | ~30 /s | ~42-44 /s |
| pod churn rate | 245-304 /s | 503-569 /s |
| fanout dispatch rate | 164-174k /s | 228-243k /s |
| heap | 41-45 GB | 44-51 GB |
| **go_sched_latency** | **1.07-1.19 ms** | **0.43-0.68 ms** |
| runnable backlog (median) | ~2120 | ~1665 |
| cores busy (CPU pprof) | 15.8 / 96 | 13.0 / 96 |

CPU pprof A/B: both **PARKED** (~13-16 of 96 cores), GC 0.5-0.7%, **wake-storm scheduler ~102-110s** in both. The profile proves the mechanism and refutes GC, but does **not** separate pass from fail.

Also ruled out: etcd (apiserver-side p99 elevated in FAIL but convoy-confounded client-side, 2.23), co-tenant CPU (KCM ~2.9 + scheduler ~1.9 = ~5 cores, ~80 of 96 idle in **both**).

So the wake-storm size is **not** the difference. The FAIL run has ~2x the scheduler delay with strictly lower instantaneous input. That is not "stochastic" — a 2x scheduler delay with no input moving must have a mechanism.

## 5. The discriminator: APF system-PL throughput collapse

Slow (>1s) non-watch requests are slow from **APF queue wait, not execution**: 96-97% of their latency is `fl_priorityandfairness` (queue wait), exec ~30-78ms.

The saturated priority level is **`system`** (kubelet `system-nodes` traffic: pod-status PATCH, node leases). Nominal 103 seats, borrowing to ~460.

| | FAIL (05-01) | PASS (04-29) |
|---|---|---|
| system reqs queued (ramp peak) | ~2700-3200 | ~2700-3000 |
| **system-PL admit rate** | **stuck ~1300/s** | **~3300/s** (burst 6864) |
| inqueue saturated (>1000) | **~46 min sustained** | ~19 min then drains |
| rejections (429) | burst 2084/s, sustained | burst 1010/s, brief |
| seat-hold per request (Little) | ~337 ms | ~146 ms |
| seat-hold (exec metric) | 77.6 ms | 27.1 ms |
| concurrency (seats in use) | 446 | 482 |

Both runs queue ~2700-3000 system requests during the create ramp. PASS admits at ~3300/s (bursts 6864) and drains; FAIL tops out at ~1300/s and stays saturated ~46 min while rejecting.

**Why 2x slower service on an idle box:** seat_hold = seats / admit_rate. The seat COUNT is the same (concurrency 482 vs 446, both at the borrowing ceiling) — not seat-starved. What differs is turnover: per-request seat-hold roughly doubles in FAIL by two independent measures (exec metric 27.1 -> 77.6 ms; Little's-law 146 -> 337 ms). Same seats / ~2.5x hold = ~2.5x lower admit (3312 -> 1322/s). The longer hold is wall-clock — the request goroutine **descheduled** waiting for an idle core under the convoy — not more CPU work. That tips the system queue past its ~1900/s arrival rate into sustained saturation.

**The burst is a batch effect.** In PASS the shallow convoy lets requests complete in batches, so seats free in bursts and APF admits a burst (6864/s) that drains the queue; in FAIL the convoy smears completions out, seats dribble free, admit holds at ~1300/s, no burst, the queue never drains.

`workload-high` (KCM pod-creates) is **not** saturated (8-10 of 138 seats). Creates are not the throttled path; kubelet status is.

## 6. The causal loop (every link measured except the last)

```
wake-storm convoy (always present, ~102-110s scheduler, similar both runs)
   -> ~2x per-request scheduler delay on an idle box (550 -> 1150 µs)
   -> system-PL APF admit halves (3300 -> 1300/s)
   -> kubelet status/lease queue ~1.9s + 429 rejections (2084/s)
   -> pods report Ready late
   -> CL2 WaitForControlledPodsRunning barrier clears late (create-201 143 -> 54/s)
   -> population dwells at 150k for ~24 min (vs ~14)
   -> watch-list probe LIST stays in the convoy the whole time
   -> aggregate LIST p99 > 30s, slowCount 53  => FAIL
```

This is a **congestion-collapse threshold** (service ceiling vs arrival rate), the mechanistic version of the "bistable dwell" framing. The wake-storm is the **substrate** (always there); the APF queue is the **amplifier** that integrates a small convoy tax into a self-sustaining backlog once service rate drops below the ~1900/s arrival rate. Not a coin flip.

## 7. The regime began at the flip and has run ever since

The realistic-pod flip (04-27) turned a flat-stable test into a bistable one within days:

- **All of April pre-flip (~26 runs): zero LIST failures**, p99 28-30s, slowCount 2-4. eps churn ~64K, below the APF-collapse threshold.
- **04-29 (+2 days): PASS but marginal** — slowCount 4, eps 86.5K, APF queue drains in ~19 min.
- **05-01 (+4 days): FAIL** — slowCount 53, eps 107.2K, APF saturated ~46 min, LIST p99 49.7s.
- **June (06-13 PASS / 06-21 FAIL), ~7 weeks later: the identical mechanism still fires** — same APF system-PL collapse, same seat-hold doubling, same dwell separation. The bistable regime has been continuous since the flip.

**Dwell separates pass from fail across all measured runs:** FAIL runs sit at maxed population ~22-26 min (05-01: 24 min), PASS runs ~13-15 min (04-25: 13, 04-29: 14), a hard gap with zero overlap. Peak per-list latency does NOT separate — the slow LIST is present in every run; what differs is how long the cluster sits at full population feeding the convoy.

## 7b. The control isolates the cause: realistic-pod eps churn crossing a threshold

| metric | **04-25 CONTROL** (pre) | **04-29 PASS** | **05-01 FAIL** |
|---|---|---|---|
| realistic-pod / init containers | **OFF / none** | ON / 2 | ON / 2 |
| eps churn (cumulative) | 63.5K | 86.5K | 107.2K |
| eps churn (matched-instant rate) | ~38/s | ~44/s | ~34/s |
| total fanout dispatch (cumulative) | 534.7M | 581.9M | 640.0M |
| peak list size (p99) | 991 MB | 989 MB | 991 MB |
| peak pod count | 166k | 168k | 170k |
| **time at maxed pod size** (>=95%) | **13 min** | **14 min** | **24 min** |
| peak pod-size timing | 18:36 (drains) | 18:42 (drains) | **19:04 (late, holds)** |
| sched latency | 0.4-0.86 ms | 0.4-0.68 ms | **1.0-1.25 ms sustained** |
| APF system inqueue | **~0** | saturated ~19 min, drains | **saturated ~46 min** |
| APF system admit | burst 4787 | burst 6864 | **stuck ~1300** |
| seat-hold (Little) | — | 146 ms | **337 ms** |
| slowCount (>30s) | **3** | 4 | **53** |
| LIST p99 (graded) | **29.1s** | 29.8s | **49.7s** |
| heap peak | 43 GB | 51 GB | 52 GB |
| **result** | **PASS** | **PASS** | **FAIL** |

eps churn verified two ways (raw delta vs `increase()`, agree to 0.1K; no apiserver restart in any run).

**Confirmed:** pre-flip eps churn is **lower (63.5K vs 86-107K)**. No init containers means pods report Ready in one shot instead of flapping through init phases, so far less endpointslice ready/not-ready churn.

**The twist:** the wake-storm substrate was **already large** pre-flip. 04-25 had the **identical** 991 MB peak list, **more** peak watches (193k), and a comparable total fanout. It did not pass by having a small storm; it passed because its lower eps churn kept the convoy **just under the APF-collapse threshold** (system queue ~0, drains on time). And note 04-29 PASS had eps 86.5K — the **same level as the June 06-21 FAIL (86.4K)** — yet passed: the same input lands on opposite sides of the threshold.

**Regime picture:**
- Pre-flip (~64K eps) sat **below the threshold** -> stable all April.
- The flip (+30% eps -> ~80-110K) **pushed into the bistable danger zone**. Within the zone, runs split pass/fail on the APF/convoy dynamics, not the input magnitude. realistic-pod did not make lists slower per request; it moved the system across a congestion threshold from "always passes" to "bistable ~50% fail," and it has stayed there from 04-29 through at least June.

## 8. Which side of the threshold a run lands on: a per-VM pre-load slowdown

§7b explains why the system sits *near* the congestion threshold (realistic-pod raised eps churn into the bistable zone). This section answers the harder question: given byte-identical config, what decides which *side* a given run lands on. **The FAIL master VM is intrinsically ~2x slower per goroutine-wakeup, measurable before the load test creates a single pod** — so the outcome is largely set by a per-VM property *before* the workload, not by spiral-timing luck. What is *not* resolvable from the guest is the *source* of that slowdown (see 8.5): it is host-level (shared physical host) or runtime-placement, not anything in the config or silicon.

### 8.1 The predictor: pre-load per-wakeup scheduling cost separates pass from fail
At a matched pre-ramp instant where goroutines/watches/uptime are identical across all 5 runs, **raw** `go_sched_latencies` mean (the unconfounded metric) separates cleanly, n=5:

| | 04-25 PASS | 06-13 PASS | 04-29 PASS | 06-21 FAIL | 05-01 FAIL |
|---|---|---|---|---|---|
| raw baseline sched-lat (mean) | 0.67 ms | 0.66 ms | 0.67 ms | **0.96 ms** | **1.19 ms** |

(The fork also normalized by dispatch — sched-lat per 100k dispatch, PASS 0.28-0.36 vs FAIL 0.82-1.19 — but that ratio is **churn-confounded**: baseline eps churn itself differs run-to-run, so dividing by dispatch inflates the gap. Use the raw number.)

### 8.2 The clean proof: a fixed pre-load instant, identical state, no queue
At **18:08** the cluster is fully up but **no test pods exist** (pod count flat at 10,490 = system DaemonSets, `POST pods/s = 0`) and the **APF queue is empty** (verified `inqueue=0`, equal ~490/s lease load). So it is **not** APF queue wait. The apiserver state is byte-identical across runs (goroutines ~138k, heap ~8 GB), yet 05-01 is multiples slower on **every** axis while doing **less** work:

| @18:08 pre-load (queue=0, 10,490 pods) | 04-29 PASS | 05-01 FAIL | ratio |
|---|---|---|---|
| goroutines / heap | 138k / 8.0 GB | 138k / 7.5 GB | identical |
| watch dispatch (the load) | 236k/s | **139k/s** | FAIL does LESS |
| sched latency (mean) | 0.67 ms | 1.31 ms | 1.9x |
| GC pause (mean) | 1.42 ms | 2.52 ms | 1.8x |
| apiserver CPU | 16.9 c | 19.8 c | more CPU |
| etcd lease latency (mean) | 9.7 ms | 31.7 ms | 3.3x |
| etcd lease latency p99 | 49 ms | 296 ms | 6.0x |
| system-PL request execution | 0.4 ms | 3.0 ms | 6x |
| kubelet lease-PUT p99 | 50 ms | 750 ms | 15x |

These are **not six independent host-speed probes** — the queue is empty but the standing wake-storm (~140-240k dispatch/s) is not, so each latency is the *same* per-wakeup convoy descheduling a different goroutine (etcd-client, request handler, lease handler). The 15x on lease-p99 vs 1.8x on sched-lat is tail-compounding (a multi-step request descheduled several times), **not** 15x slower hardware. So they widen the *observed* gap but all trace to the one pre-load slowdown; don't read them as six confirmations of a cause.

### 8.3 Not poisoned by the spiral: the floor is high *before* any queue
The fixed-op latencies are NOT an artifact of "stuck longer -> higher latency." Same metric at pre-load vs ramp vs the stuck phase:

| 05-01 FAIL | pod count | POST/s | APF inqueue | lease lat |
|---|---|---|---|---|
| 18:08 pre-load | 10,490 | 0 | **0** | **31.7 ms** |
| 18:30 ramp | 109k | 96 | 3189 | 76.8 ms |
| 18:50 stuck | 147k | 86 | 2530 | 62.6 ms |

| 04-29 PASS | pod count | POST/s | APF inqueue | lease lat |
|---|---|---|---|---|
| 18:08 pre-load | 10,490 | 0 | **0** | **9.7 ms** |
| 18:30 ramp | 137k | 203 | 2738 | 17.3 ms |
| 18:50 stuck | 152k | 27 | **0** | 9.5 ms |

Lease load is a flat ~485/s in **both** runs at **all three** instants (kubelet node-lease renewals, independent of the test-pod ramp), so this is a matched-rate operation. The PASS run's latency is **load-driven and recovers**: floor ~9.7 ms (queue 0) -> 17.3 ms under the ramp -> back to 9.5 ms when the queue drains. The FAIL run **starts at a 3x-higher floor (32 ms at queue 0)** and load stacks on top. The elevated floor is the cause; the stuck phase is the consequence. (The 18:08 row for both runs is the clean A/B: identical 10,490 pods, identical ~485/s lease load, identical empty queue, 3.3x apart.)

### 8.4 Attempted cross-component corroboration — inconclusive
The idea: if the *host* (not the apiserver) is slow, GC pause (a memory-scan-bound, mostly load-agnostic op) should be elevated on the FAIL run for *all* co-located Go processes. Scheduler latency itself only registers in the apiserver (KCM/scheduler too idle, ~2.3k/0.8k goroutines, to surface a per-wakeup cost). A single-instant read suggested GC pause was up across all three processes — but **over a 30-min window it does not hold**: idle KCM GC pause is flat (PASS 0.37/0.40/0.38 vs FAIL 0.34/0.38 ms, FAIL inside the PASS range), scheduler is noisy (0.33 vs 0.38-0.44, ranges overlap), and only the apiserver separates (1.8 -> 3.5 ms) — which is **confounded**, because Go's STW pause is coordination-bound (it waits for all Ps to reach safepoints, itself scheduler-latency-sensitive), so it just re-surfaces the convoy. The first CPU pprof does show more CPU/dispatch on FAIL (cores 17.2 PASS vs 19.3 FAIL at less load, GC refuted in both), but pprof samples cannot distinguish a memory-stalled instruction from a computing one — "memory-stall" is an inference, not a measurement (needs host PMU). **Net: this test is inconclusive, not corroborating** — there is no clean guest-side memory-bandwidth probe in the snapshot.

### 8.5 What is ruled out, and what the cause is
From the master's `kern.log` (kernel boot log, in the artifacts) on **all 5 runs**:
- **CPU generation: identical** — Intel Xeon Platinum 8581C @2.30GHz, family 0x6 / model 0xcf / stepping 0x2 (Emerald Rapids). Not a slower CPU.
- **NUMA topology: identical** — `nr_cpu_ids:96 nr_node_ids:2`, 371 GB. Not a different socket layout.
- **VM size / config: identical** — GOMAXPROCS 96, ~138k goroutines, same image.
- **Disk: ruled out** — zero etcd WAL fdatasync slow warnings in either run; per-apply p50 identical (115 vs 110 ms).
- **CPU steal: argued against** — the FAIL box burns *more* CPU per op; steal would show *less* guest CPU.

So the silicon is provably identical, and the ~2x per-wakeup gap on identical hardware is a per-VM property fixed before the workload. Its **source is not resolvable from the guest** — two live candidates, neither provable from scale artifacts:
- **shared-physical-host contention** (noisy-neighbor memory-bandwidth / LLC pressure; the wake-storm is memory-latency-bound), and
- **runtime placement luck** — Go is NUMA-unaware and the VM is 2-node, so a startup-timing-dependent layout of hot waker/wakee goroutines across the two vNUMA nodes can make every wakeup pay cross-node latency on identical hardware.

Do **not** read "more CPU per op" as a memory-contention fingerprint: it is equally consistent with a deeper endogenous wake-convoy (more `runtime.lock2/futex/osyield` spin), and even the PASS baseline already shows a mild convoy. Both candidates are uncontrolled-per-run and invisible to the guest, which is *why* it looked stochastic — but it is set the instant the VM lands, not by spiral timing.

### 8.6 What to do
- **Pinning `minCpuPlatform` is moot** — the platform is already identical every run.
- **Hardware-side mitigation:** a **sole-tenant node** for the master (dedicated physical server, not bare metal — hypervisor stays, neighbors go) removes the contention variance. Heavy/costly for CI.
- **The durable fix is still cutting the fan-out** (see "The actual lever" below): the host lottery only flips the result because the workload runs with zero headroom at the threshold. Give it headroom and a contended host cannot tip it.
- **Pre-flight detector:** gate on raw baseline `go_sched_latencies` mean (PASS <0.7 ms vs FAIL >0.95 ms here, clean at n=5) and re-roll the master VM before the run counts. Works regardless of the slowdown's source. Converts the "stochastic" failure into a detectable bad-placement signal (caveat: n=5).

## 9. What is still open

What is settled: the FAIL master is intrinsically ~2x slower per goroutine-wakeup at a byte-identical pre-load instant, so the outcome is a per-VM property set before the workload — not spiral-timing, disk, or CPU/NUMA spec (all ruled out with proof). What is **not** settled is the *source*: shared-host contention vs runtime NUMA-placement luck vs an endogenous deeper-baseline convoy. The scale prometheus has no node-exporter (no host PMU: LLC-miss, memory-bandwidth, CPU steal) and no guest-side memory-bandwidth probe, so this cannot be named from the artifacts. The controlled experiment that would name it: **run the master on a sole-tenant node** (removes neighbor contention) and/or capture `runtime/trace` goready->run cross-node placement — and see whether the variance disappears. Either way the fix is the same and does not depend on the answer: cut the fanout so a slow wakeup path cannot tip the SLO.

---

## Apiserver-side ideas for the unpaginated rv=0 LIST

Ranked for this workload. No big rewrite (those have all failed). Grounded in kubernetes @ v1.37.0-alpha.1.

1. **rv=0 serialized+gzipped snapshot cache** — RV-stamped, keyed by encoding + accept-encoding, freshness-bounded, refresh on RV-delta/timer. Amortizes serial relists including the SLO probe (every ~30s) and reflector resyncs. **This is the one that moves the gate.** Cost: 133 MB-1.5 GB heap/entry on a 40-60 GB box, so 1-2 entries with hard evict.
2. **Singleflight concurrent identical rv=0 unfiltered full LISTs** — collapse N parallel identical lists into one marshal+gzip, fan bytes to all. BUT the probe is a single informer pod (`--count=1`), so **N=1 here and singleflight collapses nothing**; it only helps a real cluster with many old controllers relisting at once, or a watch-break relist storm. Not this test.
3. **Reuse CacheableObject per-object bytes on the LIST path** — saves the marshal (~3.5s to-first-byte), not the gzip (dominant ~27s). Low value alone, free to stack on 1/2.
4. **Faster codec (s2/zstd) for large bodies** — old controllers send `Accept-Encoding: gzip` only, so they will not benefit. Opt-in modern clients only. Deprioritize.

gzip is already level-1/BestSpeed (`responsewriters/compression.go:34`), so "lower compression" is done. `CacheableObject` caches per-object serialization but only on the watch fan-out path, nothing for LIST. No singleflight / read-path dedup exists anywhere today.

## The critical caveat (do not game the test)

**Any LIST-serving fix makes the SLO PASS without touching the convoy/dwell root.** The probe LIST goes fast, the gate goes green, but kubelet writes are still APF-throttled, pods still settle slow, dwell is still ~24 min. The meter goes green while the disease persists. Faster unfiltered lists for real controllers is a legitimate win on its own, but a passing LIST-p99 after a serving change is not a fixed cluster. The gate measures a symptom that is **separable** from the defect.

## Test-harness recommendations

The LIST-p99 gate is (a) bucket-quantized near 30s, (b) probe-driven (single watch-list relist loop), and (c) maskable by a serving fix. **Add a co-gate on the convoy/dwell directly:**

- **dwell-at-full-population** separates pass/fail with zero overlap (~18 min hard gap), or
- **system-PL APF inqueue saturation duration** (PASS ~19 min, FAIL ~46 min) / **admit rate** (PASS ~3300/s, FAIL ~1300/s), or
- pod-startup -> Ready latency.

Pull `apiserver_flowcontrol_current_inqueue_requests{priority_level="system"}` over time plus `rate(apiserver_flowcontrol_dispatched_requests_total{priority_level="system"})` to see the collapse. The APF-wait-vs-exec split comes from `apiserver_flowcontrol_request_wait_duration_seconds` vs `_request_execution_seconds` (aggregate) or httplog `fl_priorityandfairness` vs `apf_execution_time` (per-request), not from response_sizes/request_total.

## The actual lever

**Cut the fan-out** — shard the cacheWatcher dispatch or filter the kube-proxy endpointslice watch — which shrinks the 5,317-way herd at the source. Raising system-PL seats alone just moves the saturation point, because the box has idle capacity APF will not admit. The root driver of the regime is `CL2_REALISTIC_POD` enablement growing endpointslice churn (init-container readiness churn x unfiltered ~5,300 kube-proxy fanout).

## Source documents

- Full A/B write-up: `clusterloader2/docs/gce5k-0613pass-vs-0621fail-apf-collapse.md`
