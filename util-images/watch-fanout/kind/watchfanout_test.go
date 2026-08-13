/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kindharness

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes"
)

// go test entry point for the watch fan-out benchmark on kind.
//
//	go test ./kind/ -run TestWatchFanout -v -timeout 40m
//	WF_WATCHERS=2000 WF_KEEP=1 go test ./kind/ -run TestWatchFanout -v -timeout 40m
//
// WF_KEEP=1 leaves the cluster up for a follow-up run, which is what makes an
// A/B affordable: bring-up and the object population are paid once.

var (
	warmup = flag.Duration("wf-warmup", 90*time.Second, "settling time before the measurement window")
	window = flag.Duration("wf-window", 120*time.Second, "measurement window")
)

const eps = `resource="endpointslices"`

func TestWatchFanout(t *testing.T) {
	if os.Getenv("WF_RUN") == "" && testing.Short() {
		t.Skip("watch fan-out benchmark: set WF_RUN=1 or drop -short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()

	opts := DefaultOptions()
	opts.Watchers = Env("WF_WATCHERS", opts.Watchers)
	opts.DecodeProbes = Env("WF_PROBES", opts.DecodeProbes)
	opts.Kubelets = Env("WF_KUBELETS", opts.Kubelets)
	opts.Pods = Env("WF_PODS", opts.Pods)

	srcDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	srcDir += "/.." // the module root, where the Dockerfile and main package live

	exists, err := ClusterExists(ctx)
	if err != nil {
		t.Fatalf("kind get clusters: %v", err)
	}
	if !exists {
		t.Logf("creating kind cluster %q with %d node identities", ClusterName, opts.Kubelets)
		if err := CreateCluster(ctx, srcDir, opts.Kubelets); err != nil {
			t.Fatalf("create cluster: %v", err)
		}
	} else {
		t.Logf("reusing existing kind cluster %q", ClusterName)
	}
	if os.Getenv("WF_KEEP") == "" {
		t.Cleanup(func() {
			cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer ccancel()
			if err := DeleteCluster(cctx); err != nil {
				t.Logf("delete cluster: %v", err)
			}
		})
	}

	if err := BuildAndLoadImage(ctx, srcDir); err != nil {
		t.Fatalf("build/load image: %v", err)
	}
	c, err := Client(ctx)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	if err := Setup(ctx, c); err != nil {
		t.Fatalf("setup: %v", err)
	}

	// Any load left running by a previous WF_KEEP run has to go before the
	// population jobs: creating pods under live fan-out is both very slow and a
	// different experiment from creating them on a quiet cluster.
	if err := TeardownRoles(ctx, c, "write", "drain-raw", "drain-decode", "kubelet"); err != nil {
		t.Fatalf("teardown previous roles: %v", err)
	}

	// One-shot population. Both are idempotent, so a reused cluster skips the
	// cost on the second run.
	t.Log("preloading services and endpointslices")
	if err := RunJob(ctx, c, "preload", []string{"preload"}, 10*time.Minute); err != nil {
		t.Fatalf("preload: %v", err)
	}
	if opts.Pods > 0 {
		t.Logf("creating %d pods bound to %d nodes", opts.Pods, opts.Kubelets)
		if err := RunJob(ctx, c, "pods", []string{"pods",
			"--count=" + strconv.Itoa(opts.Pods),
			"--namespaces=" + strconv.Itoa(opts.PodNamespaces),
			"--bind-to-nodes=" + strconv.Itoa(opts.Kubelets),
		}, 20*time.Minute); err != nil {
			t.Fatalf("pods: %v", err)
		}
	}

	roles := []Role{
		{Name: "write", MetricsPort: 9119, Args: []string{"write",
			fmt.Sprintf("--eps-rate=%g", opts.EPSRate), fmt.Sprintf("--svc-rate=%g", opts.SVCRate),
			"--concurrency=64", "--metrics-addr=:9119"}},
		{Name: "drain-raw", MetricsPort: 9112, Args: []string{"drain",
			"--clients=" + strconv.Itoa(opts.Watchers), "--mode=raw", "--metrics-addr=:9112"}},
		{Name: "drain-decode", MetricsPort: 9113, Args: []string{"drain",
			"--clients=" + strconv.Itoa(opts.DecodeProbes), "--mode=decode", "--metrics-addr=:9113"}},
		{Name: "kubelet", MetricsPort: 9115, Args: []string{"kubelet",
			"--nodes=" + strconv.Itoa(opts.Kubelets), "--renew=10s",
			"--node-status-period=5m", "--metrics-addr=:9115"}},
	}
	var names []string
	for _, r := range roles {
		if err := StartRole(ctx, c, r); err != nil {
			t.Fatalf("start %s: %v", r.Name, err)
		}
		names = append(names, r.Name)
	}
	if err := WaitReady(ctx, c, 5*time.Minute, names...); err != nil {
		t.Fatalf("roles not ready: %v", err)
	}

	t.Logf("warming up for %s", *warmup)
	sleepCtx(ctx, *warmup)

	before, err := snapshot(ctx, c)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	t.Logf("measuring for %s", *window)
	sleepCtx(ctx, *window)
	after, err := snapshot(ctx, c)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	report(t, before, after, opts, window.Seconds())
}

type snap struct {
	apiserver, decode, raw, kubelet string
}

func snapshot(ctx context.Context, c kubernetes.Interface) (snap, error) {
	var s snap
	var err error
	if s.apiserver, err = ScrapeAPIServer(ctx, c); err != nil {
		return s, fmt.Errorf("apiserver metrics: %w", err)
	}
	if s.decode, err = Scrape(ctx, 9113); err != nil {
		return s, fmt.Errorf("decode metrics: %w", err)
	}
	if s.raw, err = Scrape(ctx, 9112); err != nil {
		return s, fmt.Errorf("raw metrics: %w", err)
	}
	// The kubelet role is optional in small configs; a failure here should not
	// lose the rest of the measurement.
	s.kubelet, _ = Scrape(ctx, 9115)
	return s, nil
}

// report prints the stable metrics first. Delivery latency is printed but
// deliberately not framed as a gate: an identical configuration produced 15.7s
// and 32.5s on consecutive runs of the bash rig, so a single-run percentile
// carries no signal.
func report(t *testing.T, before, after snap, opts Options, secs float64) {
	watchers := float64(opts.Watchers + opts.DecodeProbes)

	ingested := Rate(before.apiserver, after.apiserver, "apiserver_watch_cache_events_received_total", secs, eps)
	kills := Rate(before.apiserver, after.apiserver, "apiserver_terminated_watchers_total", secs, `resource="endpointslices"`)
	recon := Rate(before.raw, after.raw, "wf_watch_restarts_total", secs, eps)
	leases := Rate(before.kubelet, after.kubelet, "wf_lease_writes_total", secs, `outcome="ok"`)
	throttled := Rate(before.kubelet, after.kubelet, "wf_lease_writes_total", secs, `outcome="throttled"`)
	nodeStatus := Rate(before.kubelet, after.kubelet, "wf_node_status_writes_total", secs, `outcome="ok"`)

	delivered, meanMs := HistDelta(before.decode, after.decode, "wf_delivery_latency_seconds", eps)
	p50 := Quantile(before.decode, after.decode, "wf_delivery_latency_seconds", 0.50, eps)
	p99 := Quantile(before.decode, after.decode, "wf_delivery_latency_seconds", 0.99, eps)
	over1s := TailFraction(before.decode, after.decode, "wf_delivery_latency_seconds", 1.0, eps)

	// Completeness: what a probe received over what the watch cache ingested.
	// Below 1.0 means events were lost, which under gate-off behaviour happens
	// when a terminated watcher reconnects past the gap.
	var completeness float64
	if ingested > 0 && opts.DecodeProbes > 0 {
		completeness = delivered / float64(opts.DecodeProbes) / (ingested * secs)
	}

	t.Log("=== watch fan-out ===")
	t.Logf("  watchers                 %.0f", watchers)
	t.Logf("  events ingested/s        %.1f", ingested)
	t.Logf("  deliveries/s             %.0f", ingested*watchers)
	t.Logf("  terminated watchers/s    %.2f", kills)
	t.Logf("  client reconnects/s      %.2f", recon)
	t.Logf("  delivery completeness    %.3f", completeness)
	t.Logf("  lease writes/s           %.1f  (throttled %.2f/s)", leases, throttled)
	t.Logf("  node status writes/s     %.2f", nodeStatus)
	t.Logf("  delivery p50 / mean / p99  %.0f / %.0f / %.0f ms   (>1s %.2f%%)",
		p50, meanMs, p99, over1s*100)

	if delivered == 0 {
		t.Error("no events delivered to the decode probes: the load did not run")
	}
	if ingested == 0 {
		t.Error("watch cache ingested no endpointslice events: the writer did not run")
	}
}

func sleepCtx(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
