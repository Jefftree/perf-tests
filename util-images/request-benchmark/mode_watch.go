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
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

func runWatch(args []string) error {
	fs := flag.NewFlagSet("watch", flag.ExitOnError)
	klog.InitFlags(fs)

	kubeconfig := fs.String("kubeconfig", "", "Path to kubeconfig. Uses in-cluster config if empty.")
	namespace := fs.String("namespace", "", "Target namespace to watch (all namespaces if empty).")
	fieldSelector := fs.String("field-selector", "", "Optional field selector to filter watches (e.g. metadata.name=bench-pod-%d).")
	labelSelector := fs.String("label-selector", "", "Optional label selector to filter watches.")
	apiVersion := fs.String("api-version", "", "apiVersion of the target resource.")
	resource := fs.String("resource", "", "resource name of the target resource.")
	contentyType := fs.String("content-type", "", "Content type for requests (required). Valid values: [json, proto]")
	watches := fs.Int("watches", 1, "Number of concurrent watch streams to open from this instance.")
	podCount := fs.Int("pod-count", 0, "Optional pod count modulo when --field-selector contains %d.")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *apiVersion != "v1" || *resource != "pods" {
		return fmt.Errorf("only v1/pods are supported for --api-version and --resource flags")
	}
	if *namespace == "" {
		return fmt.Errorf("--namespace must be non empty")
	}
	if *watches < 1 {
		return fmt.Errorf("--watches must be >= 1")
	}

	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		config, err = getConfig()
		if err != nil {
			return fmt.Errorf("failed to build kubeconfig: %w", err)
		}
	}

	switch *contentyType {
	case "json":
		config.AcceptContentTypes = "application/json"
		config.ContentType = "application/json"
	case "proto":
		config.AcceptContentTypes = "application/vnd.kubernetes.protobuf"
		config.ContentType = "application/vnd.kubernetes.protobuf"
	default:
		return fmt.Errorf("only json,proto values are supported for --content-type")
	}
	config.QPS = -1

	// Distribute watches across independent HTTP/2 connections (~100 streams per connection)
	// so large --watches counts do not hit HTTP/2 MaxConcurrentStreams limits.
	numClients := max(1, (*watches+99)/100)
	clients := make([]kubernetes.Interface, numClients)
	for i := range numClients {
		cfgCopy := rest.CopyConfig(config)
		dialer := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
		cfgCopy.Dial = dialer.DialContext
		c, err := kubernetes.NewForConfig(cfgCopy)
		if err != nil {
			return fmt.Errorf("failed to create kubernetes client %d: %w", i, err)
		}
		clients[i] = c
	}

	ctx := context.Background()
	initialRV := ""
	if list, err := clients[0].CoreV1().Pods(*namespace).List(ctx, metav1.ListOptions{Limit: 1}); err == nil {
		initialRV = list.ResourceVersion
	}

	klog.Infof("Starting pure watch workload: apiVersion=%q, resource=%q, namespace=%q, fieldSelector=%q, watches=%d, clients=%d, initialRV=%q",
		*apiVersion, *resource, *namespace, *fieldSelector, *watches, numClients, initialRV)

	handshakeSem := make(chan struct{}, 64)
	var readyWatches atomic.Int64

	var wg sync.WaitGroup
	for i := range *watches {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sel := *fieldSelector
			if strings.Contains(sel, "%d") {
				mod := *podCount
				if mod <= 0 {
					mod = *watches
				}
				sel = fmt.Sprintf(sel, idx%mod)
			}
			opts := metav1.ListOptions{
				FieldSelector:   sel,
				LabelSelector:   *labelSelector,
				ResourceVersion: initialRV,
			}
			notifiedReady := false
			runSingleWatch(ctx, clients[idx%numClients], *namespace, opts, handshakeSem, func() {
				if !notifiedReady {
					notifiedReady = true
					if readyWatches.Add(1) == int64(*watches) {
						klog.Infof("All %d watches established and ready", *watches)
						if err := os.WriteFile("/tmp/watches-ready", []byte("ok\n"), 0644); err != nil {
							klog.Errorf("Failed to write readiness file: %v", err)
						}
					}
				}
			})
		}(i)
	}
	wg.Wait()
	return nil
}

func runSingleWatch(ctx context.Context, client kubernetes.Interface, namespace string, opts metav1.ListOptions, handshakeSem chan struct{}, onReady func()) {
	for {
		handshakeSem <- struct{}{}
		w, err := client.CoreV1().Pods(namespace).Watch(ctx, opts)
		<-handshakeSem
		if err != nil {
			klog.Errorf("Watch failed: %v. Retrying in 1s...", err)
			time.Sleep(1 * time.Second)
			continue
		}
		if onReady != nil {
			onReady()
		}
		for event := range w.ResultChan() {
			switch event.Type {
			case watch.Added, watch.Modified, watch.Deleted, watch.Bookmark:
				if obj, ok := event.Object.(metav1.Object); ok && obj.GetResourceVersion() != "" {
					opts.ResourceVersion = obj.GetResourceVersion()
				}
			case watch.Error:
				err := apierrors.FromObject(event.Object)
				if apierrors.IsResourceExpired(err) {
					opts.ResourceVersion = ""
				}
				klog.Errorf("Watch failed: %v. Retrying in 1s...", err)
				time.Sleep(1 * time.Second)
			default:
				panic(fmt.Sprintf("unexpected watch event type %q: %#v", event.Type, event))
			}
		}
		w.Stop()
	}
}
