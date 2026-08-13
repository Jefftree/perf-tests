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
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

// modeWatchList issues real WatchList requests: watch with sendInitialEvents,
// which makes the apiserver replay the whole collection from the watch cache
// before switching to incremental events.
//
// Both gates are on by default -- server-side WatchList has been Beta/true
// since 1.34 and client-go's WatchListClient since 1.35 -- but the raw and
// decode modes hand-roll a plain Watch with a resourceVersion, so they never
// send sendInitialEvents and never touch this path. Only informer mode would,
// and at 119MB per client that cannot be run at scale.
//
// This mode exists to exercise "Reduce WatchList RLock hold time from O(N) to
// O(1) via lazy snapshot". That change is about LOCK CONTENTION during the
// initial snapshot, so the load that reveals it is many clients establishing
// WatchList streams concurrently, not one client streaming for a long time.
// Each client therefore times the initial-events phase, tears the stream down,
// and immediately re-establishes.
const modeWatchList = "watchlist"

// runWatchListClient repeatedly establishes a WatchList stream, measures how
// long the initial-events replay takes, then drops it and starts again.
func runWatchListClient(ctx context.Context, client kubernetes.Interface, resource, selector string, settle time.Duration) {
	wait.UntilWithContext(ctx, func(ctx context.Context) {
		start := time.Now()
		n, err := watchListOnce(ctx, client, resource, selector)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			watchlistErrors.WithLabelValues(resource).Inc()
			klog.V(3).Infof("watchlist %s: %v", resource, err)
			return
		}
		watchlistInitial.WithLabelValues(resource).Observe(time.Since(start).Seconds())
		watchlistObjects.WithLabelValues(resource).Observe(float64(n))
		if settle > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(settle):
			}
		}
	}, 10*time.Millisecond)
}

// watchListOnce returns once the initial-events bookmark arrives, which is the
// end of the replay the optimization targets.
func watchListOnce(ctx context.Context, client kubernetes.Interface, resource, selector string) (int, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// sendInitialEvents requires resourceVersionMatch, and rv=0 means "any
	// reasonably recent snapshot" rather than forcing a consistent read, which
	// is what a reflector does on a normal start.
	opts := metav1.ListOptions{
		Watch:                true,
		SendInitialEvents:    ptr.To(true),
		ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
		ResourceVersion:      "0",
		AllowWatchBookmarks:  true,
		LabelSelector:        selector,
	}
	w, err := restClientFor(client, resource).Get().
		Resource(resource).
		VersionedParams(&opts, scheme.ParameterCodec).
		Watch(ctx)
	if err != nil {
		return 0, err
	}
	defer w.Stop()

	var n int
	for {
		select {
		case <-ctx.Done():
			return n, nil
		case ev, ok := <-w.ResultChan():
			if !ok {
				return n, nil
			}
			if ev.Type == watch.Bookmark {
				m, err := meta.Accessor(ev.Object)
				if err == nil && m.GetAnnotations()[metav1.InitialEventsAnnotationKey] == "true" {
					return n, nil
				}
				continue
			}
			n++
			eventsTotal.WithLabelValues(resource, string(ev.Type)).Inc()
		}
	}
}
