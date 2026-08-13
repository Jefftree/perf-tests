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

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

// Shape of the population the 5k load test leaves behind, derived from
// clusterloader2/testing/load/config.yaml at 5000 nodes:
//
//	50 namespaces (5000 nodes / NODES_PER_NAMESPACE 100)
//	3000 pods per namespace, so per namespace
//	  149 small services  (5 endpoints)   -> 1 slice each
//	   12 medium services (30 endpoints)  -> 1 slice each
//	    1 big service     (250 endpoints) -> 3 slices
//	= 162 services, 164 EndpointSlices per namespace
//	= 8100 services, 8200 EndpointSlices total
const (
	defaultNamespaces    = 50
	smallSvcPerNS        = 149
	mediumSvcPerNS       = 12
	bigSvcPerNS          = 1
	smallEndpoints       = 5
	mediumEndpoints      = 30
	bigEndpointsPerSlice = 100
	bigSlices            = 3
	// managedBy keeps a real cluster's EndpointSlice controller from garbage
	// collecting these; it only reconciles slices it owns.
	managedBy = "watch-fanout"
)

func runPreload(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("preload", flag.ExitOnError)
	klog.InitFlags(fs)
	var (
		kubeconfig = fs.String("kubeconfig", "", "path to kubeconfig")
		server     = fs.String("server", "", "apiserver URL for anonymous auth (skips TLS verify)")
		namespaces = fs.Int("namespaces", defaultNamespaces, "number of namespaces to create")
		prefix     = fs.String("prefix", "wf", "namespace name prefix")
		workers    = fs.Int("workers", 32, "concurrent creators")
		qps        = fs.Float64("qps", 2000, "client QPS")
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

	type job struct {
		ns    string
		index int
		kind  string // small | medium | big
	}
	jobs := make(chan job, 1024)

	var svcCount, sliceCount, errCount atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if ctx.Err() != nil {
					return
				}
				n, err := createService(ctx, client, j.ns, j.kind, j.index)
				if err != nil {
					errCount.Add(1)
					klog.V(2).Infof("create %s/%s-%d: %v", j.ns, j.kind, j.index, err)
					continue
				}
				svcCount.Add(1)
				sliceCount.Add(int64(n))
			}
		}()
	}

	go func() {
		defer close(jobs)
		for n := 0; n < *namespaces; n++ {
			ns := fmt.Sprintf("%s-%03d", *prefix, n)
			if err := ensureNamespace(ctx, client, ns); err != nil {
				klog.Errorf("namespace %s: %v", ns, err)
				continue
			}
			for i := 0; i < smallSvcPerNS; i++ {
				jobs <- job{ns: ns, index: i, kind: "small"}
			}
			for i := 0; i < mediumSvcPerNS; i++ {
				jobs <- job{ns: ns, index: i, kind: "medium"}
			}
			for i := 0; i < bigSvcPerNS; i++ {
				jobs <- job{ns: ns, index: i, kind: "big"}
			}
		}
	}()

	wg.Wait()
	klog.Infof("preload done: %d services, %d endpointslices, %d errors",
		svcCount.Load(), sliceCount.Load(), errCount.Load())

	if err := preloadIdleConfigMaps(ctx, client); err != nil {
		return err
	}
	return ctx.Err()
}

// preloadIdleConfigMaps creates the target pool for drain --idle-watches.
// They are never written to: their job is to hold open watch goroutines, not
// to generate events.
func preloadIdleConfigMaps(ctx context.Context, client kubernetes.Interface) error {
	if err := ensureNamespace(ctx, client, idleNamespace); err != nil {
		return err
	}
	var made, errs int
	for i := 0; i < idleConfigMaps; i++ {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: idleConfigMapName(i), Namespace: idleNamespace},
			Data:       map[string]string{"payload": "watch-fanout idle target"},
		}
		_, err := client.CoreV1().ConfigMaps(idleNamespace).Create(ctx, cm, metav1.CreateOptions{})
		switch {
		case err == nil:
			made++
		case apierrors.IsAlreadyExists(err):
		default:
			errs++
			klog.V(2).Infof("create configmap %s: %v", cm.Name, err)
		}
	}
	klog.Infof("idle configmaps: %d created, %d errors, %d total in %s", made, errs, idleConfigMaps, idleNamespace)
	return nil
}

func ensureNamespace(ctx context.Context, c kubernetes.Interface, name string) error {
	_, err := c.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

// createService creates one Service plus its EndpointSlices, returning the
// number of slices created.
func createService(ctx context.Context, c kubernetes.Interface, ns, kind string, index int) (int, error) {
	name := fmt.Sprintf("%s-%04d", kind, index)

	_, err := c.CoreV1().Services(ns).Create(ctx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{"group": "load", "kind": kind},
		},
		Spec: corev1.ServiceSpec{
			// ClusterIP is assigned by the apiserver; a non-headless service is
			// what both the kubelet and kube-proxy service selectors admit.
			//
			// Deliberately selectorless. The endpointslice controller only
			// reconciles Services that HAVE a selector, and on a cluster with a
			// kube-controller-manager (kind, unlike the bash rig) a selector
			// here means the controller takes ownership of these slices: it
			// created its own empty slice for all 8,100 preloaded Services,
			// throttling itself at ~1 request/s, and then competes with the
			// writer for the very objects whose churn rate this rig is trying
			// to hold fixed. Fan-out has to be controlled by the rig, not by a
			// controller reacting to it, so the slices are managed by hand.
			Ports: []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromInt32(8080)}},
		},
	}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return 0, err
	}

	var sliceSizes []int
	switch kind {
	case "small":
		sliceSizes = []int{smallEndpoints}
	case "medium":
		sliceSizes = []int{mediumEndpoints}
	case "big":
		for i := 0; i < bigSlices; i++ {
			sliceSizes = append(sliceSizes, bigEndpointsPerSlice)
		}
	}

	created := 0
	for i, size := range sliceSizes {
		slice := &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("%s-%d", name, i),
				Labels: map[string]string{
					discoveryv1.LabelServiceName: name,
					discoveryv1.LabelManagedBy:   managedBy,
				},
			},
			AddressType: discoveryv1.AddressTypeIPv4,
			Ports: []discoveryv1.EndpointPort{{
				Name: ptr.To("http"), Port: ptr.To(int32(8080)),
			}},
			Endpoints: makeEndpoints(ns, name, i, size),
		}
		_, err := c.DiscoveryV1().EndpointSlices(ns).Create(ctx, slice, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return created, err
		}
		created++
	}
	return created, nil
}

// makeEndpoints builds endpoints shaped like the real thing: each carries a
// targetRef and nodeName, which is most of an EndpointSlice's serialized size.
func makeEndpoints(ns, svc string, sliceIdx, n int) []discoveryv1.Endpoint {
	eps := make([]discoveryv1.Endpoint, 0, n)
	for i := 0; i < n; i++ {
		eps = append(eps, discoveryv1.Endpoint{
			Addresses:  []string{fmt.Sprintf("10.%d.%d.%d", (sliceIdx*251+i)%256, (i*7)%256, (i*13)%256)},
			Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true), Serving: ptr.To(true), Terminating: ptr.To(false)},
			TargetRef: &corev1.ObjectReference{
				Kind:      "Pod",
				Namespace: ns,
				Name:      fmt.Sprintf("%s-%d-%08d", svc, sliceIdx, i),
				UID:       k8stypes.UID(fmt.Sprintf("%s-%s-%d-%08d-uid", ns, svc, sliceIdx, i)),
			},
			NodeName: ptr.To(fmt.Sprintf("node-%04d", (sliceIdx*997+i*31)%5000)),
		})
	}
	return eps
}
