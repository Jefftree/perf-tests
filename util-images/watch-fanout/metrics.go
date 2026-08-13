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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// The default registry also exports process_cpu_seconds_total, which is the
// measurement this rig exists to produce: divide its rate by wf_clients to get
// cores per simulated node.
var (
	eventsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wf_events_total",
		Help: "Watch events received by drain clients.",
	}, []string{"resource", "type"})

	bytesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wf_bytes_total",
		Help: "Watch stream bytes read by drain clients (raw mode).",
	}, []string{"resource"})

	// outcome="eof" is the one that matters: an apiserver-terminated watcher
	// closes the stream cleanly, so it shows up here and NOT as an error.
	watchRestarts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wf_watch_restarts_total",
		Help: "Watch re-establishments, including apiserver-terminated watchers.",
	}, []string{"resource", "outcome"})

	drainClients = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "wf_clients",
		Help: "Number of simulated nodes in this process.",
	})

	writesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wf_writes_total",
		Help: "Successful writes issued by the writer.",
	}, []string{"resource"})

	writeErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wf_write_errors_total",
		Help: "Failed writes issued by the writer.",
	}, []string{"resource"})

	// End-to-end write-to-delivery latency, from a timestamp the writer stamps
	// into the object to the moment a drain client decodes the event.
	//
	// This is the rig's reason to exist. The apiserver exposes no per-resource
	// watch DELIVERY latency metric, so on a real cluster this number cannot be
	// obtained at all. Here both ends are on one host and one clock, so it is
	// exact. Goroutine parking shows up here and NOWHERE else: a parked watcher
	// still delivers, just late, so it costs zero CPU and zero terminated
	// watchers while adding latency.
	//
	// 0.2ms to ~230s. The factor is 1.7 rather than 2.5 because at 2.5 the
	// adjacent buckets were 24.4ms / 61ms / 152.6ms, so a p99 could only land
	// on one of those and a sweep read as noise when it was quantization.
	deliveryLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "wf_delivery_latency_seconds",
		Help:    "Write-to-delivery latency observed by drain clients.",
		Buckets: prometheus.ExponentialBuckets(0.0002, 1.7, 26),
	}, []string{"resource"})

	// Events whose timestamp annotation was missing or unparseable, so they
	// were not observed into deliveryLatency. Must stay near zero or the
	// latency histogram is measuring a biased subset.
	deliveryUnstamped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wf_delivery_unstamped_total",
		Help: "Events with no usable writer timestamp.",
	}, []string{"resource"})

	kubeletCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "wf_kubelets",
		Help: "Simulated kubelets in this process.",
	})

	// outcome="throttled" is an APF rejection (429). That is the reported
	// production symptom and the reason this load exists: until these requests
	// authenticated as system:nodes rather than system:masters, every request
	// was exempt from flow control and no priority level was ever exercised.
	leaseWrites = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wf_lease_writes_total",
		Help: "Node lease renewals by outcome.",
	}, []string{"outcome"})

	leaseLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "wf_lease_latency_seconds",
		Help:    "Node lease renewal round-trip.",
		Buckets: prometheus.ExponentialBuckets(0.001, 1.7, 22),
	})

	// WatchList initial-events replay: the phase the O(N)->O(1) RLock change
	// targets. Measured per establishment, not per event.
	watchlistInitial = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "wf_watchlist_initial_seconds",
		Help:    "Time from WatchList request to the initial-events-end bookmark.",
		Buckets: prometheus.ExponentialBuckets(0.001, 1.7, 22),
	}, []string{"resource"})

	watchlistObjects = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "wf_watchlist_objects",
		Help:    "Objects replayed per WatchList establishment.",
		Buckets: prometheus.ExponentialBuckets(100, 2, 10),
	}, []string{"resource"})

	watchlistErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wf_watchlist_errors_total",
		Help: "Failed WatchList establishments.",
	}, []string{"resource"})

	listDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "wf_list_duration_seconds",
		Help:    "Cluster-scoped rv=0 pod LIST duration.",
		Buckets: prometheus.ExponentialBuckets(0.01, 1.7, 20),
	})

	listBytes = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "wf_list_bytes",
		Help:    "Response bytes per LIST.",
		Buckets: prometheus.ExponentialBuckets(1<<20, 2, 12),
	})

	listErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "wf_list_errors_total",
		Help: "Failed LISTs.",
	})

	// Non-zero means the requested rate exceeds what the writer pool can push,
	// so the achieved rate is below the one on the command line. Always check
	// this before reading a rate sweep: a synchronous writer silently caps at
	// 1/round-trip (~35/s against a loopback apiserver).
	writeLagTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wf_write_lag_total",
		Help: "Ticks dropped because every writer worker was busy.",
	}, []string{"resource"})
)
