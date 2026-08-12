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

package main

import (
	"context"
	"flag"
	"math/rand"
	"strconv"
	"sync"
	"time"

	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

// Write rates measured on gce-5k run 2080704005197008896 (103 min):
//
//	EndpointSlices: 115,739 events / 6180s = 18.7/s run average, ~33/s at storm peak
//	Services:        16,205 events / 6180s =  2.6/s
//
// Both were recovered from the fan-out identity deliveries = watchers x events
// (5,205 eps watchers x 115,739 = 602.4M deliveries, matching the measured total).
const (
	defaultEPSRate = 33.0
	defaultSVCRate = 2.6
)

func runWrite(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("write", flag.ExitOnError)
	klog.InitFlags(fs)
	var (
		kubeconfig  = fs.String("kubeconfig", "", "path to kubeconfig")
		server      = fs.String("server", "", "apiserver URL for anonymous auth (skips TLS verify)")
		epsRate     = fs.Float64("eps-rate", defaultEPSRate, "EndpointSlice writes per second")
		svcRate     = fs.Float64("svc-rate", defaultSVCRate, "Service writes per second")
		metricsAddr = fs.String("metrics-addr", ":9111", "address for /metrics, empty to disable")
		qps         = fs.Float64("qps", 500, "client QPS")
		concurrency = fs.Int("concurrency", 64, "in-flight writes per resource; must exceed rate x round-trip to hit the requested rate")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := baseConfig(*kubeconfig, *server, float32(*qps))
	if err != nil {
		return err
	}
	client, err := newClient(cfg)
	if err != nil {
		return err
	}
	serveMetrics(ctx, *metricsAddr)

	slices, err := listSlices(ctx, client)
	if err != nil {
		return err
	}
	if len(slices) == 0 {
		klog.Warning("no EndpointSlices found; run `watch-fanout preload` first")
		return nil
	}
	klog.Infof("writing to %d EndpointSlices at %.1f/s (services at %.1f/s)", len(slices), *epsRate, *svcRate)

	go writeLoop(ctx, *epsRate, *concurrency, "endpointslices", func(i int) {
		ref := slices[rand.Intn(len(slices))]
		if err := touchSlice(ctx, client, ref.ns, ref.name); err != nil {
			writeErrors.WithLabelValues("endpointslices").Inc()
			klog.V(3).Infof("touch slice %s/%s: %v", ref.ns, ref.name, err)
			return
		}
		writesTotal.WithLabelValues("endpointslices").Inc()
	})

	if *svcRate > 0 {
		svcs, err := listServices(ctx, client)
		if err != nil {
			return err
		}
		go writeLoop(ctx, *svcRate, *concurrency, "services", func(i int) {
			ref := svcs[rand.Intn(len(svcs))]
			if err := touchService(ctx, client, ref.ns, ref.name); err != nil {
				writeErrors.WithLabelValues("services").Inc()
				return
			}
			writesTotal.WithLabelValues("services").Inc()
		})
	}

	<-ctx.Done()
	return nil
}

// writeLoop ticks at the requested rate and hands each tick to a worker pool.
//
// The pool is what makes rates above production reachable. Calling fn inline
// caps the loop at one write per round-trip -- ~35/s against a loopback
// apiserver -- so a requested 300/s silently delivered 35/s, with nothing in
// the output saying so. Ticks that find every worker busy increment
// wf_write_lag_total rather than queueing, which keeps the tick clock honest
// and makes the shortfall visible.
func writeLoop(ctx context.Context, rate float64, concurrency int, resource string, fn func(i int)) {
	if rate <= 0 {
		return
	}
	if concurrency < 1 {
		concurrency = 1
	}
	work := make(chan int, concurrency)
	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				fn(i)
			}
		}()
	}
	defer func() {
		close(work)
		wg.Wait()
	}()

	interval := time.Duration(float64(time.Second) / rate)
	t := time.NewTicker(interval)
	defer t.Stop()
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			select {
			case work <- i:
			default:
				writeLagTotal.WithLabelValues(resource).Inc()
			}
		}
	}
}

type objRef struct{ ns, name string }

func listSlices(ctx context.Context, c kubernetes.Interface) ([]objRef, error) {
	var out []objRef
	cont := ""
	for {
		l, err := c.DiscoveryV1().EndpointSlices("").List(ctx, metav1.ListOptions{Limit: 2000, Continue: cont})
		if err != nil {
			return nil, err
		}
		for i := range l.Items {
			out = append(out, objRef{l.Items[i].Namespace, l.Items[i].Name})
		}
		if cont = l.Continue; cont == "" {
			return out, nil
		}
	}
}

func listServices(ctx context.Context, c kubernetes.Interface) ([]objRef, error) {
	var out []objRef
	cont := ""
	for {
		l, err := c.CoreV1().Services("").List(ctx, metav1.ListOptions{Limit: 2000, Continue: cont})
		if err != nil {
			return nil, err
		}
		for i := range l.Items {
			out = append(out, objRef{l.Items[i].Namespace, l.Items[i].Name})
		}
		if cont = l.Continue; cont == "" {
			return out, nil
		}
	}
}

// touchSlice flips the last endpoint's ready condition. That is what real
// EndpointSlice churn looks like -- a same-size update, not a resize -- so the
// serialized payload every watcher receives stays representative.
func touchSlice(ctx context.Context, c kubernetes.Interface, ns, name string) error {
	s, err := c.DiscoveryV1().EndpointSlices(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if len(s.Endpoints) == 0 {
		return nil
	}
	last := &s.Endpoints[len(s.Endpoints)-1]
	ready := last.Conditions.Ready == nil || !*last.Conditions.Ready
	last.Conditions.Ready = ptr.To(ready)
	last.Conditions.Serving = ptr.To(ready)
	stamp(&s.ObjectMeta)
	_, err = c.DiscoveryV1().EndpointSlices(ns).Update(ctx, s, metav1.UpdateOptions{})
	return err
}

// stampKey carries the write time so drain clients can compute end-to-end
// delivery latency. Stamped as late as possible before the Update call so it
// excludes the writer's own Get round-trip.
const stampKey = "watch-fanout/write-ts"

func stamp(m *metav1.ObjectMeta) {
	if m.Annotations == nil {
		m.Annotations = map[string]string{}
	}
	m.Annotations[stampKey] = strconv.FormatInt(time.Now().UnixNano(), 10)
}

func touchService(ctx context.Context, c kubernetes.Interface, ns, name string) error {
	s, err := c.CoreV1().Services(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	stamp(&s.ObjectMeta)
	_, err = c.CoreV1().Services(ns).Update(ctx, s, metav1.UpdateOptions{})
	return err
}

var _ = discoveryv1.AddToScheme
