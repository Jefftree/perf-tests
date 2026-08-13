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

// watch-fanout reproduces the apiserver watch fan-out of a large cluster
// without the cluster: N independent watch clients against a real apiserver,
// plus a writer producing the handful of writes per second that drive it.
package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

// Selectors kube-proxy and kubelet actually use, so the cacher evaluates the
// same predicates it does in production.
// cmd/kube-proxy/app/server.go:592 (endpointslices), :603 (services).
const (
	epsSelector = "!service.kubernetes.io/headless"
	svcSelector = "!service.kubernetes.io/service-proxy-name"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cmd := os.Args[1]
	args := os.Args[2:]

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch cmd {
	case "preload":
		err = runPreload(ctx, args)
	case "write":
		err = runWrite(ctx, args)
	case "drain":
		err = runDrain(ctx, args)
	case "pods":
		err = runPods(ctx, args)
	case "list":
		err = runList(ctx, args)
	case "kubelet":
		err = runKubelet(ctx, args)
	default:
		usage()
	}
	if err != nil && ctx.Err() == nil {
		klog.Exitf("%s: %v", cmd, err)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `usage: watch-fanout <preload|write|drain> [flags]

  preload  create the Service/EndpointSlice population a load test would leave behind
  write    drive EndpointSlice/Service writes at a fixed rate
  drain    run N independent watch clients (the fan-out load)

`)
	os.Exit(2)
}

// baseConfig builds a rest.Config from --kubeconfig, or from --server for an
// anonymous-auth apiserver.
func baseConfig(kubeconfig, server string, qps float32) (*rest.Config, error) {
	var (
		cfg *rest.Config
		err error
	)
	switch {
	case server != "":
		cfg = &rest.Config{Host: server}
		cfg.TLSClientConfig.Insecure = true
	case kubeconfig != "":
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, err
		}
	default:
		// In-cluster, which is how every role runs under kind. The
		// ServiceAccount credential is only used for the CA and the server
		// URL by the kubelet role, which then replaces the bearer token per
		// client with its own system:node identity.
		cfg, err = rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("no --kubeconfig or --server given and not running in-cluster: %w", err)
		}
	}
	cfg.AcceptContentTypes = "application/vnd.kubernetes.protobuf,application/json"
	cfg.ContentType = "application/vnd.kubernetes.protobuf"
	cfg.QPS = qps
	cfg.Burst = int(qps) * 2
	return cfg, nil
}

// perClientConfig returns a config guaranteed to get its own http.Transport,
// and therefore its own TCP connection.
//
// client-go caches transports on a key that includes the *DialHolder pointer
// (transport/cache.go:264-271), and rest.Config only allocates a DialHolder
// when Dial is non-nil (rest/transport.go:116-118). Configs that leave Dial nil
// all collapse onto one shared transport, and HTTP/2 then multiplexes every
// watch onto ceil(N/250) connections instead of N. Handing each client its own
// dial closure is what keeps the connection count honest.
func perClientConfig(base *rest.Config) *rest.Config {
	c := rest.CopyConfig(base)
	d := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	c.Dial = func(ctx context.Context, network, addr string) (net.Conn, error) {
		return d.DialContext(ctx, network, addr)
	}
	return c
}

func newClient(cfg *rest.Config) (kubernetes.Interface, error) {
	return kubernetes.NewForConfig(cfg)
}

// serveMetrics exposes the default registry, which already carries
// process_cpu_seconds_total -- the number this rig exists to measure.
func serveMetrics(ctx context.Context, addr string) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx) //nolint:errcheck
	}()
	go func() {
		klog.Infof("serving metrics on %s/metrics", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			klog.Errorf("metrics server: %v", err)
		}
	}()
}
