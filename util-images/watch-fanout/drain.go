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
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// Drain modes, cheapest to most faithful. Comparing CPU across them is the
// point of the rig: it decomposes real kube-proxy's measured 0.244 cores at
// storm peak into per-event decode versus structural resync/store work.
const (
	// modeRaw reads the watch stream and discards bytes. No decode. This is the
	// floor: what it costs merely to hold the connection and accept the data.
	modeRaw = "raw"
	// modeDecode decodes each event into a typed object and drops it. Adds
	// protobuf decode and allocation, no store, no handlers.
	modeDecode = "decode"
	// modeInformer runs a real SharedInformer with a store and a no-op handler,
	// matching kube-proxy's informer half including periodic resync.
	modeInformer = "informer"
)

func runDrain(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("drain", flag.ExitOnError)
	klog.InitFlags(fs)
	var (
		kubeconfig  = fs.String("kubeconfig", "", "path to kubeconfig")
		server      = fs.String("server", "", "apiserver URL for anonymous auth (skips TLS verify)")
		clients     = fs.Int("clients", 100, "number of simulated nodes")
		mode        = fs.String("mode", modeDecode, "raw | decode | informer")
		watchSvc    = fs.Bool("watch-services", true, "also watch services (2 svc watchers per node in production)")
		resync      = fs.Duration("resync", 0, "informer resync period; kube-proxy uses 30s (ConfigSyncPeriod)")
		drainDelay  = fs.Duration("drain-delay", 0, "artificial per-event delay, to sweep client drain rate")
		rampDelay   = fs.Duration("ramp-delay", 2*time.Millisecond, "stagger between client starts")
		metricsAddr = fs.String("metrics-addr", ":9112", "address for /metrics, empty to disable")
		// Goroutine population is the axis this rig was missing; see idlewatch.go.
		idleWatches   = fs.Int("idle-watches", 0, idleWatchesHelp)
		idleBookmarks = fs.Bool("idle-bookmarks", true, "request bookmarks on idle watches, as a real reflector does")
		qps           = fs.Float64("qps", 100, "per-client QPS for the initial list")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	switch *mode {
	case modeRaw, modeDecode, modeInformer:
	default:
		return fmt.Errorf("unknown mode %q", *mode)
	}

	base, err := baseConfig(*kubeconfig, *server, float32(*qps))
	if err != nil {
		return err
	}
	serveMetrics(ctx, *metricsAddr)

	klog.Infof("starting %d clients, mode=%s watch-services=%v resync=%s drain-delay=%s",
		*clients, *mode, *watchSvc, *resync, *drainDelay)
	drainClients.Set(float64(*clients))

	var wg sync.WaitGroup
	for i := 0; i < *clients; i++ {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(*rampDelay):
		}
		cfg := perClientConfig(base)
		wg.Add(1)
		go func(id int, cfg *rest.Config) {
			defer wg.Done()
			runClient(ctx, id, cfg, *mode, *watchSvc, *resync, *drainDelay, *idleWatches, *idleBookmarks)
		}(i, cfg)
	}
	klog.Infof("all %d clients started", *clients)

	wg.Wait()
	return ctx.Err()
}

// runClient simulates one node. It reconnects forever, the way a reflector
// does, so a watch closed by the apiserver (including a terminated watcher)
// shows up as a re-list rather than a lost client.
func runClient(ctx context.Context, id int, cfg *rest.Config, mode string, watchSvc bool, resync, drainDelay time.Duration, idleWatches int, idleBookmarks bool) {
	client, err := newClient(cfg)
	if err != nil {
		klog.Errorf("client %d: %v", id, err)
		return
	}

	// Started before the churning watches so the goroutine population is in
	// place by the time delivery latency is sampled.
	if idleWatches > 0 {
		runIdleWatches(ctx, client, id, idleWatches, idleBookmarks)
	}

	if mode == modeInformer {
		runInformerClient(ctx, client, watchSvc, resync)
		return
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		watchForever(ctx, client, "endpointslices", epsSelector, mode, drainDelay)
	}()
	if watchSvc {
		wg.Add(1)
		go func() {
			defer wg.Done()
			watchForever(ctx, client, "services", svcSelector, mode, drainDelay)
		}()
	}
	wg.Wait()
}

func watchForever(ctx context.Context, client kubernetes.Interface, resource, selector, mode string, drainDelay time.Duration) {
	wait.UntilWithContext(ctx, func(ctx context.Context) {
		err := watchOnce(ctx, client, resource, selector, mode, drainDelay)
		if ctx.Err() != nil {
			return
		}
		// Count EVERY re-establishment, not just errored ones. An
		// apiserver-terminated watcher closes the stream cleanly, so watchOnce
		// returns nil and the client silently reconnects. Counting only errors
		// reported zero restarts during a window where the apiserver's own
		// apiserver_terminated_watchers_total rose by one per watcher.
		outcome := "eof"
		if err != nil {
			outcome = "error"
			klog.V(3).Infof("watch %s: %v", resource, err)
		}
		watchRestarts.WithLabelValues(resource, outcome).Inc()
	}, 100*time.Millisecond)
}

func watchOnce(ctx context.Context, client kubernetes.Interface, resource, selector, mode string, drainDelay time.Duration) error {
	rv, err := currentRV(ctx, client, resource, selector)
	if err != nil {
		return err
	}
	opts := metav1.ListOptions{
		Watch:               true,
		ResourceVersion:     rv,
		LabelSelector:       selector,
		AllowWatchBookmarks: true,
	}

	if mode == modeRaw {
		return drainRaw(ctx, client, resource, opts, drainDelay)
	}
	return drainDecoded(ctx, client, resource, opts, drainDelay)
}

// drainRaw reads the wire bytes without decoding. The server-side path --
// dispatch, serialization, compression, http2 framing, socket write -- is
// exercised in full; only the client's decode is skipped.
func drainRaw(ctx context.Context, client kubernetes.Interface, resource string, opts metav1.ListOptions, drainDelay time.Duration) error {
	stream, err := restClientFor(client, resource).Get().
		Resource(resource).
		VersionedParams(&opts, scheme.ParameterCodec).
		Stream(ctx)
	if err != nil {
		return err
	}
	defer stream.Close()

	buf := make([]byte, 32*1024)
	for {
		n, err := stream.Read(buf)
		if n > 0 {
			bytesTotal.WithLabelValues(resource).Add(float64(n))
			// Without decoding there are no event boundaries, so raw mode
			// reports bytes only; use the writer's known rate for event counts.
			if drainDelay > 0 {
				time.Sleep(drainDelay)
			}
		}
		if err != nil {
			if err == io.EOF || ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
}

func drainDecoded(ctx context.Context, client kubernetes.Interface, resource string, opts metav1.ListOptions, drainDelay time.Duration) error {
	w, err := restClientFor(client, resource).Get().
		Resource(resource).
		VersionedParams(&opts, scheme.ParameterCodec).
		Watch(ctx)
	if err != nil {
		return err
	}
	defer w.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-w.ResultChan():
			if !ok {
				return nil
			}
			eventsTotal.WithLabelValues(resource, string(ev.Type)).Inc()
			observeDelivery(resource, ev.Object)
			if drainDelay > 0 {
				time.Sleep(drainDelay)
			}
		}
	}
}

// observeDelivery records write-to-delivery latency from the writer's stamp.
// Bookmarks and any object the writer never touched carry no stamp; they are
// counted separately rather than silently skipped, because a histogram built
// from an unknown subset of events is worse than no histogram.
func observeDelivery(resource string, obj runtime.Object) {
	m, err := meta.Accessor(obj)
	if err != nil {
		deliveryUnstamped.WithLabelValues(resource).Inc()
		return
	}
	ns, err := strconv.ParseInt(m.GetAnnotations()[stampKey], 10, 64)
	if err != nil {
		deliveryUnstamped.WithLabelValues(resource).Inc()
		return
	}
	deliveryLatency.WithLabelValues(resource).Observe(time.Since(time.Unix(0, ns)).Seconds())
}

// runInformerClient mirrors how kube-proxy builds its informers
// (cmd/kube-proxy/app/server.go:587-613): separate factories per resource, each
// with its own label selector tweak, and a resync period.
func runInformerClient(ctx context.Context, client kubernetes.Interface, watchSvc bool, resync time.Duration) {
	epsFactory := informers.NewSharedInformerFactoryWithOptions(client, resync,
		informers.WithTweakListOptions(func(o *metav1.ListOptions) { o.LabelSelector = epsSelector }))
	epsInf := epsFactory.Discovery().V1().EndpointSlices().Informer()
	epsInf.AddEventHandler(countingHandler("endpointslices")) //nolint:errcheck
	epsFactory.Start(ctx.Done())

	if watchSvc {
		svcFactory := informers.NewSharedInformerFactoryWithOptions(client, resync,
			informers.WithTweakListOptions(func(o *metav1.ListOptions) { o.LabelSelector = svcSelector }))
		svcInf := svcFactory.Core().V1().Services().Informer()
		svcInf.AddEventHandler(countingHandler("services")) //nolint:errcheck
		svcFactory.Start(ctx.Done())
	}
	<-ctx.Done()
}

func countingHandler(resource string) cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { eventsTotal.WithLabelValues(resource, "ADDED").Inc() },
		UpdateFunc: func(any, any) { eventsTotal.WithLabelValues(resource, "MODIFIED").Inc() },
		DeleteFunc: func(any) { eventsTotal.WithLabelValues(resource, "DELETED").Inc() },
	}
}

// currentRV gets the resource version to watch from, the way a reflector's
// initial list would, without paying for the full object dump on every
// reconnect.
func currentRV(ctx context.Context, client kubernetes.Interface, resource, selector string) (string, error) {
	res := &metav1.PartialObjectMetadataList{}
	err := restClientFor(client, resource).Get().
		Resource(resource).
		VersionedParams(&metav1.ListOptions{Limit: 1, LabelSelector: selector}, scheme.ParameterCodec).
		SetHeader("Accept", "application/json;as=PartialObjectMetadataList;v=v1;g=meta.k8s.io").
		Do(ctx).Into(res)
	if err != nil {
		return "", err
	}
	return res.ResourceVersion, nil
}

func restClientFor(client kubernetes.Interface, resource string) rest.Interface {
	if resource == "endpointslices" {
		return client.DiscoveryV1().RESTClient()
	}
	return client.CoreV1().RESTClient()
}
