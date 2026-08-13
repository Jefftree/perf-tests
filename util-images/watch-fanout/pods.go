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
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
)

// Co-tenant LIST load: the ingredient the rig still lacks.
//
// Watcher count and event rate are already matched to production, and both
// goroutine population and apiserver GOMAXPROCS have been ruled out, yet this
// rig delivers in ~24ms where production stalls for ~0.9s. The largest
// remaining difference is that production's apiserver is simultaneously
// serving ~150k pods through repeated cluster-scoped rv=0 LISTs at
// ~130-137MB per response, allocating 1-3 GB/s against an uncapped GOGC=100
// heap that rides to 50-60GB.
//
// That is a GC and http2-write-path story, and neither shows up as dispatcher
// utilisation. This rig's apiserver sits at ~2GB with almost no allocation
// churn, so if heap pressure is what turns a 24ms delivery into a 900ms one,
// this is what has been missing.
//
// Pods here are never scheduled and never run. They exist to be LISTed.
const podNamespacePrefix = "wf-pods"

func runPods(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("pods", flag.ExitOnError)
	klog.InitFlags(fs)
	var (
		kubeconfig = fs.String("kubeconfig", "", "path to kubeconfig")
		server     = fs.String("server", "", "apiserver URL for anonymous auth (skips TLS verify)")
		count      = fs.Int("count", 150000, "pods to create")
		namespaces = fs.Int("namespaces", 50, "spread pods across this many namespaces")
		workers    = fs.Int("workers", 64, "concurrent creators")
		qps        = fs.Float64("qps", 4000, "client QPS")
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

	for n := 0; n < *namespaces; n++ {
		if err := ensureNamespace(ctx, client, fmt.Sprintf("%s-%03d", podNamespacePrefix, n)); err != nil {
			return err
		}
	}

	var made, exists, errs atomic.Int64
	idx := make(chan int, 4096)
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range idx {
				if ctx.Err() != nil {
					return
				}
				ns := fmt.Sprintf("%s-%03d", podNamespacePrefix, i%*namespaces)
				_, err := client.CoreV1().Pods(ns).Create(ctx, realisticPod(ns, i), metav1.CreateOptions{})
				switch {
				case err == nil:
					made.Add(1)
				case apierrors.IsAlreadyExists(err):
					exists.Add(1)
				default:
					errs.Add(1)
					klog.V(3).Infof("create pod %d: %v", i, err)
				}
			}
		}()
	}
	start := time.Now()
	go func() {
		defer close(idx)
		for i := 0; i < *count; i++ {
			idx <- i
		}
	}()
	wg.Wait()
	klog.Infof("pods: %d created, %d existed, %d errors in %s",
		made.Load(), exists.Load(), errs.Load(), time.Since(start).Truncate(time.Second))
	return ctx.Err()
}

// realisticPod mirrors the CL2_REALISTIC_POD template field for field:
// clusterloader2/testing/load/modules/reconcile-objects/deployment.yaml,
// the $RealisticPod branches.
//
// Object size matters here because the watch cache deep-copies every object it
// serves, and that copy's cost scales with the object GRAPH, not with byte
// count. An earlier hand-rolled approximation of this pod serialized to 3,850
// bytes against production's ~913 (137MB per 150k-pod LIST), so it was 4x too
// fat and inflated every deep-copy measurement taken against it. The excess was
// mostly a synthetic last-applied-configuration annotation and four fully
// populated containers where the real template has two plus two init
// containers.
//
// Pods here are never scheduled and never run; they exist to be watched and
// LISTed.
func realisticPod(ns string, i int) *corev1.Pod {
	name := fmt.Sprintf("wf-pod-%07d", i)
	res := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("5m"),
			corev1.ResourceMemory: resource.MustParse("20M"),
		},
	}
	fieldEnv := func(n, path string) corev1.EnvVar {
		return corev1.EnvVar{Name: n, ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: path}}}
	}
	resEnv := func(n, r string) corev1.EnvVar {
		return corev1.EnvVar{Name: n, ValueFrom: &corev1.EnvVarSource{
			ResourceFieldRef: &corev1.ResourceFieldSelector{ContainerName: "main", Resource: r}}}
	}
	initContainer := func(n string) corev1.Container {
		return corev1.Container{
			Name:    n,
			Image:   "registry.k8s.io/e2e-test-images/agnhost:2.53",
			Command: []string{"/bin/sh", "-c", "sleep 1"},
		}
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels: map[string]string{
				"name":                        fmt.Sprintf("wf-dep-%d", i%1000),
				"svc":                         fmt.Sprintf("wf-svc-%d", i%1000),
				"group":                       "load",
				"app.kubernetes.io/name":      "payment-service",
				"app.kubernetes.io/instance":  "payment-service-primary",
				"app.kubernetes.io/version":   "v2.1.4",
				"app.kubernetes.io/component": "backend",
				"app.kubernetes.io/part-of":   "ecommerce-platform",
				"env":                         "production",
				"track":                       "canary",
			},
			Annotations: map[string]string{
				"prometheus.io/scrape":                           "false",
				"prometheus.io/port":                             "8080",
				"prometheus.io/path":                             "/metrics",
				"sidecar.istio.io/inject":                        "false",
				"cluster-autoscaler.kubernetes.io/safe-to-evict": "false",
				"vault.hashicorp.com/agent-inject":               "false",
				"vault.hashicorp.com/role":                       "my-app-db-role",
				"fluentbit.io/parser":                            "json",
				"argocd.argoproj.io/hook":                        "PreSync",
				"argocd.argoproj.io/hook-delete-policy":          "HookSucceeded",
			},
		},
		Spec: corev1.PodSpec{
			// Unschedulable on purpose: this rig has no nodes, and nothing
			// should ever try to run these.
			NodeSelector:   map[string]string{"watch-fanout/never": "true"},
			InitContainers: []corev1.Container{initContainer("init-0"), initContainer("init-1")},
			Containers: []corev1.Container{
				{
					Name:  "main",
					Image: "registry.k8s.io/pause:3.9",
					Env: []corev1.EnvVar{
						fieldEnv("POD_NAME", "metadata.name"),
						fieldEnv("POD_NAMESPACE", "metadata.namespace"),
						fieldEnv("NODE_NAME", "spec.nodeName"),
						fieldEnv("POD_IP", "status.podIP"),
						resEnv("GOMAXPROCS", "limits.cpu"),
						resEnv("GOMEMLIMIT", "limits.memory"),
						{Name: "JAVA_TOOL_OPTIONS", Value: "-XX:MaxRAMPercentage=75.0"},
						{Name: "REDIS_URL", Value: "redis://main:6379/0"},
						{Name: "PYTHONUNBUFFERED", Value: "1"},
						{Name: "NODE_ENV", Value: "prod"},
					},
					Resources: res,
					VolumeMounts: []corev1.VolumeMount{
						{Name: "configmap", MountPath: "/var/configmap"},
						{Name: "secret", MountPath: "/var/secret"},
					},
				},
				{Name: "sidecar", Image: "registry.k8s.io/pause:3.9", Resources: res},
			},
			Volumes: []corev1.Volume{
				{Name: "configmap", VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: idleConfigMapName(i % idleConfigMaps)},
					}}},
				{Name: "secret", VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{SecretName: "wf-secret"}}},
			},
		},
	}
}

// runList issues cluster-scoped rv=0 pod LISTs, the shape that drives the
// apiserver's allocation rate in the real load test.
func runList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	klog.InitFlags(fs)
	var (
		kubeconfig  = fs.String("kubeconfig", "", "path to kubeconfig")
		server      = fs.String("server", "", "apiserver URL for anonymous auth (skips TLS verify)")
		concurrency = fs.Int("concurrency", 4, "simultaneous LISTs in flight")
		interval    = fs.Duration("interval", time.Second, "delay between LISTs per worker")
		metricsAddr = fs.String("metrics-addr", ":9114", "address for /metrics, empty to disable")
		qps         = fs.Float64("qps", 500, "client QPS")
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
	klog.Infof("cluster-scoped rv=0 pod LISTs, concurrency %d, interval %s", *concurrency, *interval)

	var wg sync.WaitGroup
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 256*1024)
			for ctx.Err() == nil {
				start := time.Now()
				// Stream and discard rather than List(): decoding a
				// cluster-scoped pod LIST materialises every pod as a Go object
				// on the CLIENT, which at 150k pods is gigabytes per worker and
				// would OOM the box long before the apiserver felt anything.
				// The apiserver-side cost -- watch-cache walk, deep copy,
				// protobuf encode, http2 write -- is identical either way, and
				// that is the allocation pressure being reproduced.
				//
				// rv=0 is served from the watch cache, the production path.
				// Never paginate: rv=0 pod LIST pagination is off the table.
				stream, err := client.CoreV1().RESTClient().Get().
					Resource("pods").
					VersionedParams(&metav1.ListOptions{ResourceVersion: "0"}, scheme.ParameterCodec).
					Stream(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					listErrors.Inc()
					klog.V(3).Infof("list pods: %v", err)
					continue
				}
				var n int64
				for {
					r, rerr := stream.Read(buf)
					n += int64(r)
					if rerr != nil {
						break
					}
				}
				stream.Close()
				listDuration.Observe(time.Since(start).Seconds())
				listBytes.Observe(float64(n))
				select {
				case <-ctx.Done():
					return
				case <-time.After(*interval):
				}
			}
		}()
	}
	wg.Wait()
	return ctx.Err()
}
