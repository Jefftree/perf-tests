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

// realisticPod approximates the CL2_REALISTIC_POD shape: init containers, a
// sidecar, configmap and secret volume mounts, and a filled-in status. Target
// is production's ~900 bytes of serialized pod, derived from 137MB per 150k-pod
// LIST, since total LIST bytes is what drives the allocation.
func realisticPod(ns string, i int) *corev1.Pod {
	name := fmt.Sprintf("wf-pod-%07d", i)
	labels := map[string]string{
		"name":                       "wf-load",
		"app":                        fmt.Sprintf("wf-app-%03d", i%200),
		"pod-template-hash":          fmt.Sprintf("%09d", i),
		"group":                      "load",
		"kubernetes.io/managed-by":   managedBy,
		"app.kubernetes.io/instance": fmt.Sprintf("inst-%05d", i%1000),
	}
	annotations := map[string]string{
		"watch-fanout/purpose": "list-load target, never scheduled",
		"kubectl.kubernetes.io/last-applied-configuration": fmt.Sprintf(
			`{"apiVersion":"v1","kind":"Pod","metadata":{"name":%q,"namespace":%q}}`, name, ns),
	}
	res := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("10m"),
			corev1.ResourceMemory: resource.MustParse("32Mi"),
		},
	}
	container := func(n string) corev1.Container {
		return corev1.Container{
			Name:      n,
			Image:     "registry.k8s.io/pause:3.9",
			Resources: res,
			VolumeMounts: []corev1.VolumeMount{
				{Name: "cm", MountPath: "/etc/cm"},
				{Name: "sec", MountPath: "/etc/sec"},
			},
			TerminationMessagePath:   corev1.TerminationMessagePathDefault,
			TerminationMessagePolicy: corev1.TerminationMessageReadFile,
			ImagePullPolicy:          corev1.PullIfNotPresent,
		}
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: ns, Labels: labels, Annotations: annotations,
		},
		Spec: corev1.PodSpec{
			// Unschedulable on purpose: no node in this rig, and nothing should
			// ever try to run these.
			NodeSelector:   map[string]string{"watch-fanout/never": "true"},
			InitContainers: []corev1.Container{container("init-1"), container("init-2")},
			Containers:     []corev1.Container{container("main"), container("sidecar")},
			Volumes: []corev1.Volume{
				{Name: "cm", VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: idleConfigMapName(i % idleConfigMaps)},
					}}},
				{Name: "sec", VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{SecretName: "wf-secret"}}},
			},
			RestartPolicy:                 corev1.RestartPolicyAlways,
			TerminationGracePeriodSeconds: ptrInt64(30),
			DNSPolicy:                     corev1.DNSClusterFirst,
			ServiceAccountName:            "default",
			SchedulerName:                 corev1.DefaultSchedulerName,
		},
	}
}

func ptrInt64(v int64) *int64 { return &v }

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
