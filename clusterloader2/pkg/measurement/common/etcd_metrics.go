/*
Copyright 2018 The Kubernetes Authors.

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

package common

import (
	"cmp"
	"fmt"
	"maps"
	"math"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/common/model"
	"k8s.io/klog/v2"
	"k8s.io/perf-tests/clusterloader2/pkg/measurement"
	measurementutil "k8s.io/perf-tests/clusterloader2/pkg/measurement/util"
	"k8s.io/perf-tests/clusterloader2/pkg/provider"
	"k8s.io/perf-tests/clusterloader2/pkg/util"
)

const (
	etcdMetricsMetricName = "EtcdMetrics"
)

func init() {
	if err := measurement.Register(etcdMetricsMetricName, createEtcdMetricsMeasurement); err != nil {
		klog.Fatalf("Cannot register %s: %v", etcdMetricsMetricName, err)
	}
}

func createEtcdMetricsMeasurement() measurement.Measurement {
	return &etcdMetricsMeasurement{
		wg:               &sync.WaitGroup{},
		stopCh:           make(chan struct{}),
		metrics:          newEtcdMetrics(),
		initialSnapshots: make(map[string]etcdHostSnapshot),
	}
}

type etcdMetricsMeasurement struct {
	wg        *sync.WaitGroup
	isRunning bool
	stopCh    chan struct{}

	sync.Mutex
	metrics          *etcdMetrics
	initialSnapshots map[string]etcdHostSnapshot
}

// Execute supports two actions:
// - start - Starts collecting etcd metrics.
// - gather - Gathers and prints etcd metrics summary.
func (e *etcdMetricsMeasurement) Execute(config *measurement.Config) ([]measurement.Summary, error) {
	provider := config.ClusterFramework.GetClusterConfig().Provider
	// Etcd is only exposed on localhost level. We are using ssh method
	if !provider.Features().SupportSSHToMaster {
		klog.Warningf("not grabbing etcd metrics through master SSH: unsupported for provider, %s", config.ClusterFramework.GetClusterConfig().Provider.Name())
		return nil, nil
	}

	action, err := util.GetString(config.Params, "action")
	if err != nil {
		return nil, err
	}

	hosts := config.ClusterFramework.GetClusterConfig().MasterIPs
	if len(hosts) < 1 {
		klog.Warningf("ETCD measurements will be disabled due to no MasterIps: %v", hosts)
		return nil, nil
	}

	etcdInsecurePort := config.ClusterFramework.GetClusterConfig().EtcdInsecurePort
	switch action {
	case "start":
		klog.V(2).Infof("%s: starting etcd metrics collecting...", e)
		waitTime, err := util.GetDurationOrDefault(config.Params, "waitTime", time.Minute)
		if err != nil {
			return nil, err
		}
		e.resetState()
		snapshots := make(map[string]etcdHostSnapshot, len(hosts))
		for _, h := range hosts {
			snapshot, err := e.captureInitialMetrics(h, provider, etcdInsecurePort)
			if err != nil {
				return nil, err
			}
			snapshots[h] = snapshot
		}
		e.setInitialSnapshots(snapshots)
		for _, h := range hosts {
			e.startCollecting(h, provider, waitTime, etcdInsecurePort)
		}
		return nil, nil
	case "gather":
		e.Dispose()
		summaryMetrics := newEtcdMetrics()
		summaryMetrics.MaxDatabaseSize = e.getMaxDatabaseSize()
		for _, h := range hosts {
			hostMetrics, err := e.collectHostMetrics(h, provider, etcdInsecurePort)
			if err != nil {
				return nil, err
			}
			mergeEtcdMetrics(summaryMetrics, hostMetrics)
		}
		content, err := util.PrettyPrintJSON(summaryMetrics)
		if err != nil {
			return nil, err
		}
		summary := measurement.CreateSummary(etcdMetricsMetricName, "json", content)
		return []measurement.Summary{summary}, nil
	default:
		return nil, fmt.Errorf("unknown action %v", action)
	}
}

// Dispose cleans up after the measurement.
func (e *etcdMetricsMeasurement) Dispose() {
	if e.isRunning {
		e.isRunning = false
		close(e.stopCh)
		e.wg.Wait()
	}
}

func (e *etcdMetricsMeasurement) String() string {
	return etcdMetricsMetricName
}

func (e *etcdMetricsMeasurement) resetState() {
	e.Dispose()
	e.stopCh = make(chan struct{})
	e.Lock()
	defer e.Unlock()
	e.metrics = newEtcdMetrics()
	e.initialSnapshots = make(map[string]etcdHostSnapshot)
}

func (e *etcdMetricsMeasurement) captureInitialMetrics(host string, provider provider.Provider, port int) (etcdHostSnapshot, error) {
	samples, err := e.getEtcdMetrics(host, provider, port)
	if err != nil {
		return etcdHostSnapshot{}, err
	}
	return extractEtcdHostSnapshot(samples), nil
}

func (e *etcdMetricsMeasurement) setInitialSnapshots(snapshots map[string]etcdHostSnapshot) {
	e.Lock()
	defer e.Unlock()
	e.initialSnapshots = snapshots
}

func (e *etcdMetricsMeasurement) getInitialSnapshot(host string) (etcdHostSnapshot, bool) {
	e.Lock()
	defer e.Unlock()
	snapshot, ok := e.initialSnapshots[host]
	return snapshot, ok
}

func (e *etcdMetricsMeasurement) getMaxDatabaseSize() float64 {
	e.Lock()
	defer e.Unlock()
	return e.metrics.MaxDatabaseSize
}

func (e *etcdMetricsMeasurement) startCollecting(host string, provider provider.Provider, interval time.Duration, port int) {
	e.isRunning = true
	e.wg.Add(1)

	collectEtcdDatabaseSize := func() error {
		dbSize, err := e.getEtcdDatabaseSize(host, provider, port)
		if err != nil {
			return err
		}

		e.Lock()
		defer e.Unlock()
		e.metrics.MaxDatabaseSize = math.Max(e.metrics.MaxDatabaseSize, dbSize)

		return nil
	}
	go func() {
		defer e.wg.Done()
		for {
			select {
			case <-time.After(interval):
				err := collectEtcdDatabaseSize()
				if err != nil {
					klog.Errorf("%s: failed to collect etcd database size", e)
					continue
				}
			case <-e.stopCh:
				return
			}
		}
	}()
}

func (e *etcdMetricsMeasurement) collectHostMetrics(host string, provider provider.Provider, port int) (*etcdMetrics, error) {
	samples, err := e.getEtcdMetrics(host, provider, port)
	if err != nil {
		return nil, err
	}

	current := extractEtcdHostSnapshot(samples)
	if initial, ok := e.getInitialSnapshot(host); ok {
		return subtractEtcdHostSnapshot(current, initial), nil
	}
	return current.metrics, nil
}

func (e *etcdMetricsMeasurement) getEtcdMetrics(host string, provider provider.Provider, port int) ([]*model.Sample, error) {

	// In https://github.com/kubernetes/kubernetes/pull/74690, mTLS is enabled for etcd server
	// in order to bypass TLS credential requirement when checking etc /metrics and /health, you
	// need to provide the insecure http port number to access etcd, http://localhost:2382 for
	// example.
	cmd := fmt.Sprintf("curl http://localhost:%d/metrics", port)
	samples, err := e.sshEtcdMetrics(cmd, host, provider)
	if err == nil {
		return samples, nil
	}
	klog.Warningf("%s: call on %d port (%s) failed due to %v. Falling back to default 2379 port.", e, port, cmd, err)

	// Use old endpoint if new one fails, "2379" is hard-coded here as well, it is kept as is since
	// we don't want to bloat the cluster config only for a fall-back attempt.
	etcdCert, etcdKey, etcdHost := os.Getenv("ETCD_CERTIFICATE"), os.Getenv("ETCD_KEY"), os.Getenv("ETCD_HOST")
	if etcdHost == "" {
		etcdHost = "localhost"
	}
	if etcdCert == "" || etcdKey == "" {
		klog.Warning("empty etcd cert or key, using http")
		cmd = fmt.Sprintf("curl http://%s:2379/metrics", etcdHost)
	} else {
		cmd = fmt.Sprintf("curl -k --cert %s --key %s https://%s:2379/metrics", etcdCert, etcdKey, etcdHost)
	}

	return e.sshEtcdMetrics(cmd, host, provider)
}

func (e *etcdMetricsMeasurement) sshEtcdMetrics(cmd, host string, provider provider.Provider) ([]*model.Sample, error) {
	sshResult, err := measurementutil.SSH(cmd, host+":22", provider)
	if err != nil {
		return nil, fmt.Errorf("unexpected error (code: %d) in ssh connection to master: %#v", sshResult.Code, err)
	} else if sshResult.Code != 0 {
		return nil, fmt.Errorf("failed running command: %s on the host: %s, result: %+v", cmd, host, sshResult)
	}
	data := sshResult.Stdout

	return measurementutil.ExtractMetricSamples(data)
}

func (e *etcdMetricsMeasurement) getEtcdDatabaseSize(host string, provider provider.Provider, port int) (float64, error) {
	samples, err := e.getEtcdMetrics(host, provider, port)
	if err != nil {
		return 0, err
	}
	for _, sample := range samples {
		if sample.Metric[model.MetricNameLabel] == "etcd_debugging_mvcc_db_total_size_in_bytes" ||
			sample.Metric[model.MetricNameLabel] == "etcd_mvcc_db_total_size_in_bytes" {
			return float64(sample.Value), nil
		}
	}
	return 0, fmt.Errorf("couldn't find etcd database size metric")
}

type etcdMetrics struct {
	BackendCommitDuration     measurementutil.HistogramVec `json:"backendCommitDuration"`
	SnapshotSaveTotalDuration measurementutil.HistogramVec `json:"snapshotSaveTotalDuration"`
	PeerRoundTripTime         measurementutil.HistogramVec `json:"peerRoundTripTime"`
	WalFsyncDuration          measurementutil.HistogramVec `json:"walFsyncDuration"`
	MaxDatabaseSize           float64                      `json:"maxDatabaseSize"`
}

func newEtcdMetrics() *etcdMetrics {
	return &etcdMetrics{
		BackendCommitDuration:     make(measurementutil.HistogramVec, 0),
		SnapshotSaveTotalDuration: make(measurementutil.HistogramVec, 0),
		PeerRoundTripTime:         make(measurementutil.HistogramVec, 0),
		WalFsyncDuration:          make(measurementutil.HistogramVec, 0),
	}
}

type etcdHostSnapshot struct {
	metrics             *etcdMetrics
	processStartTime    float64
	hasProcessStartTime bool
}

func extractEtcdHostSnapshot(samples []*model.Sample) etcdHostSnapshot {
	snapshot := etcdHostSnapshot{
		metrics: newEtcdMetrics(),
	}
	for _, sample := range samples {
		switch sample.Metric[model.MetricNameLabel] {
		case "etcd_disk_backend_commit_duration_seconds_bucket":
			measurementutil.ConvertSampleToBucket(sample, &snapshot.metrics.BackendCommitDuration)
		case "etcd_debugging_snap_save_total_duration_seconds_bucket":
			measurementutil.ConvertSampleToBucket(sample, &snapshot.metrics.SnapshotSaveTotalDuration)
		case "etcd_disk_wal_fsync_duration_seconds_bucket":
			measurementutil.ConvertSampleToBucket(sample, &snapshot.metrics.WalFsyncDuration)
		case "etcd_network_peer_round_trip_time_seconds_bucket":
			measurementutil.ConvertSampleToBucket(sample, &snapshot.metrics.PeerRoundTripTime)
		case "process_start_time_seconds":
			val := float64(sample.Value)
			if !math.IsNaN(val) && !math.IsInf(val, 0) {
				snapshot.processStartTime = val
				snapshot.hasProcessStartTime = true
			}
		}
	}
	return snapshot
}

func subtractEtcdHostSnapshot(current, initial etcdHostSnapshot) *etcdMetrics {
	if current.hasProcessStartTime && initial.hasProcessStartTime && current.processStartTime != initial.processStartTime {
		return cloneEtcdMetrics(current.metrics)
	}
	return &etcdMetrics{
		BackendCommitDuration:     subtractHistogramVec(current.metrics.BackendCommitDuration, initial.metrics.BackendCommitDuration),
		SnapshotSaveTotalDuration: subtractHistogramVec(current.metrics.SnapshotSaveTotalDuration, initial.metrics.SnapshotSaveTotalDuration),
		PeerRoundTripTime:         subtractHistogramVec(current.metrics.PeerRoundTripTime, initial.metrics.PeerRoundTripTime),
		WalFsyncDuration:          subtractHistogramVec(current.metrics.WalFsyncDuration, initial.metrics.WalFsyncDuration),
	}
}

func subtractHistogramVec(currentVec, initialVec measurementutil.HistogramVec) measurementutil.HistogramVec {
	subtracted := make(measurementutil.HistogramVec, 0, len(currentVec))
	for _, currentHist := range currentVec {
		initialHist, found := findMatchingHistogram(initialVec, currentHist.Labels)
		if !found || hasHistogramReset(currentHist, initialHist) {
			subtracted = append(subtracted, cloneHistogram(currentHist))
			continue
		}
		buckets := make(map[string]int, len(currentHist.Buckets))
		for bucket, currentCount := range currentHist.Buckets {
			buckets[bucket] = currentCount - initialHist.Buckets[bucket]
		}
		subtracted = append(subtracted, measurementutil.Histogram{
			Labels:  maps.Clone(currentHist.Labels),
			Buckets: buckets,
		})
	}
	return subtracted
}

func findMatchingHistogram(histograms measurementutil.HistogramVec, labels map[string]string) (measurementutil.Histogram, bool) {
	for _, hist := range histograms {
		if maps.Equal(hist.Labels, labels) {
			return hist, true
		}
	}
	return measurementutil.Histogram{}, false
}

type bucketBound struct {
	key        string
	upperBound float64
}

func hasHistogramReset(current, initial measurementutil.Histogram) bool {
	for bucket, initialCount := range initial.Buckets {
		currentCount, exists := current.Buckets[bucket]
		if !exists || currentCount < initialCount {
			return true
		}
	}

	bounds := make([]bucketBound, 0, len(current.Buckets))
	for bucket := range current.Buckets {
		upperBound, err := strconv.ParseFloat(bucket, 64)
		if err != nil || math.IsNaN(upperBound) {
			klog.ErrorS(err, "Failed to parse etcd histogram bucket bound", "bucket", bucket)
			return true
		}
		bounds = append(bounds, bucketBound{
			key:        bucket,
			upperBound: upperBound,
		})
	}
	slices.SortFunc(bounds, func(a, b bucketBound) int {
		return cmp.Compare(a.upperBound, b.upperBound)
	})

	// Without a counter reset, non-cumulative interval counts cannot decrease, so
	// cumulative bucket deltas (current[b_i] - initial[b_i]) must be non-decreasing.
	prevDelta := 0
	for _, bound := range bounds {
		delta := current.Buckets[bound.key] - initial.Buckets[bound.key]
		if delta < prevDelta {
			return true
		}
		prevDelta = delta
	}
	return false
}

func mergeEtcdMetrics(dst, src *etcdMetrics) {
	mergeHistogramVec(&dst.BackendCommitDuration, src.BackendCommitDuration)
	mergeHistogramVec(&dst.SnapshotSaveTotalDuration, src.SnapshotSaveTotalDuration)
	mergeHistogramVec(&dst.PeerRoundTripTime, src.PeerRoundTripTime)
	mergeHistogramVec(&dst.WalFsyncDuration, src.WalFsyncDuration)
}

func mergeHistogramVec(dst *measurementutil.HistogramVec, src measurementutil.HistogramVec) {
	for _, srcHist := range src {
		matched := false
		for i := range *dst {
			if maps.Equal((*dst)[i].Labels, srcHist.Labels) {
				for bucket, count := range srcHist.Buckets {
					(*dst)[i].Buckets[bucket] += count
				}
				matched = true
				break
			}
		}
		if !matched {
			*dst = append(*dst, cloneHistogram(srcHist))
		}
	}
}

func cloneEtcdMetrics(src *etcdMetrics) *etcdMetrics {
	return &etcdMetrics{
		BackendCommitDuration:     cloneHistogramVec(src.BackendCommitDuration),
		SnapshotSaveTotalDuration: cloneHistogramVec(src.SnapshotSaveTotalDuration),
		PeerRoundTripTime:         cloneHistogramVec(src.PeerRoundTripTime),
		WalFsyncDuration:          cloneHistogramVec(src.WalFsyncDuration),
		MaxDatabaseSize:           src.MaxDatabaseSize,
	}
}

func cloneHistogramVec(src measurementutil.HistogramVec) measurementutil.HistogramVec {
	cloned := make(measurementutil.HistogramVec, 0, len(src))
	for _, hist := range src {
		cloned = append(cloned, cloneHistogram(hist))
	}
	return cloned
}

func cloneHistogram(src measurementutil.Histogram) measurementutil.Histogram {
	return measurementutil.Histogram{
		Labels:  maps.Clone(src.Labels),
		Buckets: maps.Clone(src.Buckets),
	}
}
