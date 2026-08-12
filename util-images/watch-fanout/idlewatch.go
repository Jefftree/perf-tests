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
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

// Idle watches reproduce the goroutine population a real node contributes.
//
// A 5k cluster's apiserver carries ~190k watch goroutines, but only ~10k of
// them are the EndpointSlice and Service watches this rig was originally built
// around. The rest come from kubelet's watch-based ConfigMap/Secret manager,
// which opens ONE reflector per referenced object with a metadata.name field
// selector (pkg/kubelet/util/manager/watch_based_manager.go:222-241). With
// CL2_REALISTIC_POD mounting a configmap and a secret per pod, every node holds
// a fistful of these.
//
// They matter because goroutine parking is a function of goroutine POPULATION,
// not of event rate. These objects almost never change, so each watch costs
// near-zero CPU while still occupying a cacheWatcher, an http2 stream and a
// serve goroutine. That is precisely the load this rig was missing: at 5,200
// clients it had 38,542 apiserver goroutines against production's ~190k.
//
// Sizing: measured ~3.7 apiserver goroutines per watch, so reaching ~190k from
// a 10,400-watch baseline needs roughly 8 extra watches per simulated node.
const (
	idleNamespace   = "wf-idle"
	idleConfigMaps  = 500
	idleWatchesHelp = "single-object ConfigMap watches per node, kubelet-style (0 disables)"
)

func idleConfigMapName(i int) string {
	return fmt.Sprintf("wf-idle-cm-%04d", i)
}

// runIdleWatches starts n single-object ConfigMap watches for one simulated
// node. Objects are picked by node id so the pool spreads across clients the
// way real nodes reference overlapping but non-identical configmaps.
//
// These share the node's rest.Config, hence its TCP connection, exactly as a
// kubelet's do: http2 multiplexes them onto one socket. Connections stay at one
// per node while goroutines multiply, which is the whole point -- the axis that
// was missed is goroutines, not sockets.
func runIdleWatches(ctx context.Context, client kubernetes.Interface, id, n int, bookmarks bool) {
	for k := 0; k < n; k++ {
		name := idleConfigMapName((id*n + k) % idleConfigMaps)
		go watchOneObject(ctx, client, name, bookmarks)
	}
}

func watchOneObject(ctx context.Context, client kubernetes.Interface, name string, bookmarks bool) {
	// MinWatchTimeout in the real manager is 30m to avoid churning watches;
	// the 1s floor here only applies to reconnects after an error.
	wait.UntilWithContext(ctx, func(ctx context.Context) {
		opts := metav1.ListOptions{
			FieldSelector:       "metadata.name=" + name,
			Watch:               true,
			AllowWatchBookmarks: bookmarks,
		}
		w, err := client.CoreV1().ConfigMaps(idleNamespace).Watch(ctx, opts)
		if err != nil {
			klog.V(4).Infof("idle watch %s: %v", name, err)
			return
		}
		defer w.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-w.ResultChan():
				if !ok {
					return
				}
				eventsTotal.WithLabelValues("configmaps", string(ev.Type)).Inc()
			}
		}
	}, time.Second)
}
