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

package common

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/perf-tests/clusterloader2/pkg/config"
	"k8s.io/perf-tests/clusterloader2/pkg/framework"
	"k8s.io/perf-tests/clusterloader2/pkg/measurement"
	measurementutil "k8s.io/perf-tests/clusterloader2/pkg/measurement/util"
	"k8s.io/perf-tests/clusterloader2/pkg/provider"
)

type fakeEtcdSSHProvider struct {
	provider.Provider

	mu      sync.Mutex
	outputs map[string]string
}

func newFakeEtcdSSHProvider() *fakeEtcdSSHProvider {
	return &fakeEtcdSSHProvider{
		Provider: provider.NewSkeletonProvider(nil),
		outputs:  make(map[string]string),
	}
}

func (f *fakeEtcdSSHProvider) setHostMetrics(host, exposition string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.outputs[host+":22"] = exposition
}

func (f *fakeEtcdSSHProvider) RunSSHCommand(cmd, host string) (string, string, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out, ok := f.outputs[host]
	if !ok {
		return "", fmt.Sprintf("missing fake metrics for %s (%s)", host, cmd), 1, nil
	}
	return out, "", 0, nil
}

func newEtcdTestConfig(t *testing.T, sshProvider provider.Provider, hosts []string, action string) *measurement.Config {
	t.Helper()
	kubeconfigPath := filepath.Join(t.TempDir(), "kubeconfig")
	const minimalKubeconfig = `apiVersion: v1
kind: Config
clusters:
- cluster:
    server: https://127.0.0.1:6443
  name: test
contexts:
- context:
    cluster: test
    user: test
  name: test
current-context: test
users:
- name: test
  user:
    token: fake-token
`
	require.NoError(t, os.WriteFile(kubeconfigPath, []byte(minimalKubeconfig), 0o600))

	clusterConfig := &config.ClusterConfig{
		KubeConfigPath:   kubeconfigPath,
		Provider:         sshProvider,
		MasterIPs:        hosts,
		EtcdInsecurePort: 2382,
	}
	clusterFramework, err := framework.NewFramework(clusterConfig, 1)
	require.NoError(t, err)

	return &measurement.Config{
		ClusterFramework: clusterFramework,
		Params: map[string]interface{}{
			"action":   action,
			"waitTime": "10m",
		},
	}
}

func findPeerHistogram(t *testing.T, vec measurementutil.HistogramVec, peerID string) measurementutil.Histogram {
	t.Helper()
	for _, hist := range vec {
		if hist.Labels["To"] == peerID {
			return hist
		}
	}
	t.Fatalf("peer histogram To=%q not found in %+v", peerID, vec)
	return measurementutil.Histogram{}
}

func TestEtcdMetricsMeasurementSubtractsInitialHistogramsAcrossMasters(t *testing.T) {
	sshProvider := newFakeEtcdSSHProvider()
	hosts := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}

	// Initial pre-test boot state on 3 masters.
	sshProvider.setHostMetrics("10.0.0.1", strings.Join([]string{
		`process_start_time_seconds 1700000001`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="0.002"} 100`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="+Inf"} 120`,
		`etcd_debugging_snap_save_total_duration_seconds_bucket{le="0.5"} 5`,
		`etcd_debugging_snap_save_total_duration_seconds_bucket{le="+Inf"} 5`,
		`etcd_disk_wal_fsync_duration_seconds_bucket{le="0.001"} 200`,
		`etcd_disk_wal_fsync_duration_seconds_bucket{le="+Inf"} 250`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="2222222222222222",le="0.0004"} 20`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="2222222222222222",le="+Inf"} 30`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="3333333333333333",le="0.0004"} 15`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="3333333333333333",le="+Inf"} 30`,
		"",
	}, "\n"))
	sshProvider.setHostMetrics("10.0.0.2", strings.Join([]string{
		`process_start_time_seconds 1700000002`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="0.002"} 80`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="+Inf"} 100`,
		`etcd_debugging_snap_save_total_duration_seconds_bucket{le="0.5"} 4`,
		`etcd_debugging_snap_save_total_duration_seconds_bucket{le="+Inf"} 4`,
		`etcd_disk_wal_fsync_duration_seconds_bucket{le="0.001"} 150`,
		`etcd_disk_wal_fsync_duration_seconds_bucket{le="+Inf"} 200`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="1111111111111111",le="0.0004"} 10`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="1111111111111111",le="+Inf"} 25`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="3333333333333333",le="0.0004"} 12`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="3333333333333333",le="+Inf"} 25`,
		"",
	}, "\n"))
	sshProvider.setHostMetrics("10.0.0.3", strings.Join([]string{
		`process_start_time_seconds 1700000003`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="0.002"} 90`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="+Inf"} 110`,
		`etcd_debugging_snap_save_total_duration_seconds_bucket{le="0.5"} 6`,
		`etcd_debugging_snap_save_total_duration_seconds_bucket{le="+Inf"} 6`,
		`etcd_disk_wal_fsync_duration_seconds_bucket{le="0.001"} 180`,
		`etcd_disk_wal_fsync_duration_seconds_bucket{le="+Inf"} 220`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="1111111111111111",le="0.0004"} 18`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="1111111111111111",le="+Inf"} 25`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="2222222222222222",le="0.0004"} 14`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="2222222222222222",le="+Inf"} 25`,
		"",
	}, "\n"))

	m := createEtcdMetricsMeasurement()
	t.Cleanup(m.Dispose)

	summaries, err := m.Execute(newEtcdTestConfig(t, sshProvider, hosts, "start"))
	require.NoError(t, err)
	assert.Empty(t, summaries)

	// End-of-test state: each master accumulated test traffic; snapshot save had zero new events.
	sshProvider.setHostMetrics("10.0.0.1", strings.Join([]string{
		`process_start_time_seconds 1700000001`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="0.002"} 400`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="+Inf"} 520`,
		`etcd_debugging_snap_save_total_duration_seconds_bucket{le="0.5"} 5`,
		`etcd_debugging_snap_save_total_duration_seconds_bucket{le="+Inf"} 5`,
		`etcd_disk_wal_fsync_duration_seconds_bucket{le="0.001"} 700`,
		`etcd_disk_wal_fsync_duration_seconds_bucket{le="+Inf"} 850`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="2222222222222222",le="0.0004"} 70`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="2222222222222222",le="+Inf"} 130`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="3333333333333333",le="0.0004"} 55`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="3333333333333333",le="+Inf"} 130`,
		"",
	}, "\n"))
	sshProvider.setHostMetrics("10.0.0.2", strings.Join([]string{
		`process_start_time_seconds 1700000002`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="0.002"} 280`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="+Inf"} 400`,
		`etcd_debugging_snap_save_total_duration_seconds_bucket{le="0.5"} 4`,
		`etcd_debugging_snap_save_total_duration_seconds_bucket{le="+Inf"} 4`,
		`etcd_disk_wal_fsync_duration_seconds_bucket{le="0.001"} 450`,
		`etcd_disk_wal_fsync_duration_seconds_bucket{le="+Inf"} 600`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="1111111111111111",le="0.0004"} 50`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="1111111111111111",le="+Inf"} 125`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="3333333333333333",le="0.0004"} 42`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="3333333333333333",le="+Inf"} 125`,
		"",
	}, "\n"))
	sshProvider.setHostMetrics("10.0.0.3", strings.Join([]string{
		`process_start_time_seconds 1700000003`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="0.002"} 290`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="+Inf"} 410`,
		`etcd_debugging_snap_save_total_duration_seconds_bucket{le="0.5"} 6`,
		`etcd_debugging_snap_save_total_duration_seconds_bucket{le="+Inf"} 6`,
		`etcd_disk_wal_fsync_duration_seconds_bucket{le="0.001"} 380`,
		`etcd_disk_wal_fsync_duration_seconds_bucket{le="+Inf"} 520`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="1111111111111111",le="0.0004"} 48`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="1111111111111111",le="+Inf"} 125`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="2222222222222222",le="0.0004"} 64`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="2222222222222222",le="+Inf"} 125`,
		"",
	}, "\n"))

	em := m.(*etcdMetricsMeasurement)
	em.Lock()
	em.metrics.MaxDatabaseSize = 8388608
	em.Unlock()

	summaries, err = m.Execute(newEtcdTestConfig(t, sshProvider, hosts, "gather"))
	require.NoError(t, err)
	require.Len(t, summaries, 1)

	var got etcdMetrics
	require.NoError(t, json.Unmarshal([]byte(summaries[0].SummaryContent()), &got))
	assert.Equal(t, float64(8388608), got.MaxDatabaseSize)

	// BackendCommit: (400-100)+(280-80)+(290-90) = 700; +Inf: (520-120)+(400-100)+(410-110) = 1000.
	require.Len(t, got.BackendCommitDuration, 1)
	assert.Equal(t, map[string]int{"0.002": 700, "+Inf": 1000}, got.BackendCommitDuration[0].Buckets)

	// SnapshotSave: 0 new snapshots across all 3 masters.
	require.Len(t, got.SnapshotSaveTotalDuration, 1)
	assert.Equal(t, map[string]int{"0.5": 0, "+Inf": 0}, got.SnapshotSaveTotalDuration[0].Buckets)

	// WalFsync: (700-200)+(450-150)+(380-180) = 1000; +Inf: (850-250)+(600-200)+(520-220) = 1300.
	require.Len(t, got.WalFsyncDuration, 1)
	assert.Equal(t, map[string]int{"0.001": 1000, "+Inf": 1300}, got.WalFsyncDuration[0].Buckets)

	// PeerRoundTripTime preserves To labels per peer while subtracting pre-test counts per host.
	require.Len(t, got.PeerRoundTripTime, 3)
	assert.Equal(t, map[string]int{"0.0004": 70, "+Inf": 200}, findPeerHistogram(t, got.PeerRoundTripTime, "1111111111111111").Buckets)
	assert.Equal(t, map[string]int{"0.0004": 100, "+Inf": 200}, findPeerHistogram(t, got.PeerRoundTripTime, "2222222222222222").Buckets)
	assert.Equal(t, map[string]int{"0.0004": 70, "+Inf": 200}, findPeerHistogram(t, got.PeerRoundTripTime, "3333333333333333").Buckets)
}

func TestEtcdMetricsMeasurementHandlesSingleMemberRestartWithoutMaskingOtherMasters(t *testing.T) {
	sshProvider := newFakeEtcdSSHProvider()
	hosts := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}

	sshProvider.setHostMetrics("10.0.0.1", strings.Join([]string{
		`process_start_time_seconds 1700000001`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="0.002"} 100`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="+Inf"} 120`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="3333333333333333",le="0.0004"} 50`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="3333333333333333",le="+Inf"} 60`,
		"",
	}, "\n"))
	sshProvider.setHostMetrics("10.0.0.2", strings.Join([]string{
		`process_start_time_seconds 1700000002`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="0.002"} 100`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="+Inf"} 120`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="3333333333333333",le="0.0004"} 50`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="3333333333333333",le="+Inf"} 60`,
		"",
	}, "\n"))
	sshProvider.setHostMetrics("10.0.0.3", strings.Join([]string{
		`process_start_time_seconds 1700000003`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="0.002"} 100`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="+Inf"} 120`,
		"",
	}, "\n"))

	m := createEtcdMetricsMeasurement()
	t.Cleanup(m.Dispose)
	_, err := m.Execute(newEtcdTestConfig(t, sshProvider, hosts, "start"))
	require.NoError(t, err)

	// Master 1 & 3 continue normally; Master 2 restarts mid-test with lower post-restart counts.
	sshProvider.setHostMetrics("10.0.0.1", strings.Join([]string{
		`process_start_time_seconds 1700000001`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="0.002"} 250`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="+Inf"} 320`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="3333333333333333",le="0.0004"} 150`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="3333333333333333",le="+Inf"} 180`,
		"",
	}, "\n"))
	sshProvider.setHostMetrics("10.0.0.2", strings.Join([]string{
		`process_start_time_seconds 1700000999`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="0.002"} 30`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="+Inf"} 40`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="3333333333333333",le="0.0004"} 20`,
		`etcd_network_peer_round_trip_time_seconds_bucket{To="3333333333333333",le="+Inf"} 25`,
		"",
	}, "\n"))
	sshProvider.setHostMetrics("10.0.0.3", strings.Join([]string{
		`process_start_time_seconds 1700000003`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="0.002"} 200`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="+Inf"} 260`,
		"",
	}, "\n"))

	summaries, err := m.Execute(newEtcdTestConfig(t, sshProvider, hosts, "gather"))
	require.NoError(t, err)
	require.Len(t, summaries, 1)

	var got etcdMetrics
	require.NoError(t, json.Unmarshal([]byte(summaries[0].SummaryContent()), &got))

	// Master 1: (250-100, 320-120) = (150, 200)
	// Master 2 (restarted): (30, 40)
	// Master 3: (200-100, 260-120) = (100, 140)
	require.Len(t, got.BackendCommitDuration, 1)
	assert.Equal(t, map[string]int{"0.002": 280, "+Inf": 380}, got.BackendCommitDuration[0].Buckets)

	// Shared To="3333333333333333": Master 1 subtracted (150-50=100, 180-60=120) + Master 2 unsubtracted (20, 25).
	require.Len(t, got.PeerRoundTripTime, 1)
	assert.Equal(t, map[string]int{"0.0004": 120, "+Inf": 145}, got.PeerRoundTripTime[0].Buckets)
}

func TestSubtractEtcdHostSnapshotCounterResetEdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		initial     string
		current     string
		wantBuckets map[string]int
	}{
		{
			name: "process restart where post-restart counts exceed initial counts",
			initial: strings.Join([]string{
				`process_start_time_seconds 1700000000`,
				`etcd_disk_backend_commit_duration_seconds_bucket{le="0.002"} 40`,
				`etcd_disk_backend_commit_duration_seconds_bucket{le="+Inf"} 50`,
				"",
			}, "\n"),
			current: strings.Join([]string{
				`process_start_time_seconds 1700000500`,
				`etcd_disk_backend_commit_duration_seconds_bucket{le="0.002"} 300`,
				`etcd_disk_backend_commit_duration_seconds_bucket{le="+Inf"} 400`,
				"",
			}, "\n"),
			wantBuckets: map[string]int{"0.002": 300, "+Inf": 400},
		},
		{
			name: "counter reset without process_start_time_seconds where interval delta decreased",
			initial: strings.Join([]string{
				`etcd_disk_backend_commit_duration_seconds_bucket{le="0.002"} 80`,
				`etcd_disk_backend_commit_duration_seconds_bucket{le="+Inf"} 100`,
				"",
			}, "\n"),
			current: strings.Join([]string{
				`etcd_disk_backend_commit_duration_seconds_bucket{le="0.002"} 90`,
				`etcd_disk_backend_commit_duration_seconds_bucket{le="+Inf"} 105`,
				"",
			}, "\n"),
			// Without interval-reset detection, subtracting would produce {"0.002": 10, "+Inf": 5} (200% CDF).
			wantBuckets: map[string]int{"0.002": 90, "+Inf": 105},
		},
		{
			name: "counter reset without process_start_time_seconds where total count dropped",
			initial: strings.Join([]string{
				`etcd_disk_backend_commit_duration_seconds_bucket{le="0.002"} 80`,
				`etcd_disk_backend_commit_duration_seconds_bucket{le="+Inf"} 100`,
				"",
			}, "\n"),
			current: strings.Join([]string{
				`etcd_disk_backend_commit_duration_seconds_bucket{le="0.002"} 20`,
				`etcd_disk_backend_commit_duration_seconds_bucket{le="+Inf"} 25`,
				"",
			}, "\n"),
			wantBuckets: map[string]int{"0.002": 20, "+Inf": 25},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			initSamples, err := measurementutil.ExtractMetricSamples(tc.initial)
			require.NoError(t, err)
			curSamples, err := measurementutil.ExtractMetricSamples(tc.current)
			require.NoError(t, err)

			got := subtractEtcdHostSnapshot(extractEtcdHostSnapshot(curSamples), extractEtcdHostSnapshot(initSamples))
			require.Len(t, got.BackendCommitDuration, 1)
			assert.Equal(t, tc.wantBuckets, got.BackendCommitDuration[0].Buckets)
		})
	}
}

func TestEtcdMetricsMeasurementRepeatedCyclesAndGatherWithoutStart(t *testing.T) {
	sshProvider := newFakeEtcdSSHProvider()
	hosts := []string{"10.0.0.1"}

	m := createEtcdMetricsMeasurement()
	t.Cleanup(m.Dispose)

	// 1. gather without start falls back to raw cumulative counts.
	sshProvider.setHostMetrics("10.0.0.1", strings.Join([]string{
		`process_start_time_seconds 1700000001`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="0.002"} 50`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="+Inf"} 60`,
		"",
	}, "\n"))
	summaries, err := m.Execute(newEtcdTestConfig(t, sshProvider, hosts, "gather"))
	require.NoError(t, err)
	require.Len(t, summaries, 1)
	var first etcdMetrics
	require.NoError(t, json.Unmarshal([]byte(summaries[0].SummaryContent()), &first))
	require.Len(t, first.BackendCommitDuration, 1)
	assert.Equal(t, map[string]int{"0.002": 50, "+Inf": 60}, first.BackendCommitDuration[0].Buckets)

	// 2. Subsequent start -> gather resets prior state and subtracts the new start baseline.
	_, err = m.Execute(newEtcdTestConfig(t, sshProvider, hosts, "start"))
	require.NoError(t, err)

	sshProvider.setHostMetrics("10.0.0.1", strings.Join([]string{
		`process_start_time_seconds 1700000001`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="0.002"} 85`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="+Inf"} 110`,
		"",
	}, "\n"))
	summaries, err = m.Execute(newEtcdTestConfig(t, sshProvider, hosts, "gather"))
	require.NoError(t, err)
	require.Len(t, summaries, 1)
	var second etcdMetrics
	require.NoError(t, json.Unmarshal([]byte(summaries[0].SummaryContent()), &second))
	require.Len(t, second.BackendCommitDuration, 1)
	assert.Equal(t, map[string]int{"0.002": 35, "+Inf": 50}, second.BackendCommitDuration[0].Buckets)

	// 3. Repeated gather without an intervening start does not double-count already gathered buckets.
	summaries, err = m.Execute(newEtcdTestConfig(t, sshProvider, hosts, "gather"))
	require.NoError(t, err)
	require.Len(t, summaries, 1)
	var repeated etcdMetrics
	require.NoError(t, json.Unmarshal([]byte(summaries[0].SummaryContent()), &repeated))
	require.Len(t, repeated.BackendCommitDuration, 1)
	assert.Equal(t, map[string]int{"0.002": 35, "+Inf": 50}, repeated.BackendCommitDuration[0].Buckets)
}

func TestEtcdMetricsMeasurementFailedStartDoesNotLeavePartialSnapshots(t *testing.T) {
	sshProvider := newFakeEtcdSSHProvider()
	hosts := []string{"10.0.0.1", "10.0.0.2"}

	// Only host 1 succeeds on start; host 2 has no metrics yet so start returns an error.
	sshProvider.setHostMetrics("10.0.0.1", strings.Join([]string{
		`process_start_time_seconds 1700000001`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="0.002"} 40`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="+Inf"} 50`,
		"",
	}, "\n"))

	m := createEtcdMetricsMeasurement()
	t.Cleanup(m.Dispose)

	_, err := m.Execute(newEtcdTestConfig(t, sshProvider, hosts, "start"))
	require.Error(t, err)

	// Later gather after host 2 recovers reports consistent unsubtracted counts across both masters.
	sshProvider.setHostMetrics("10.0.0.2", strings.Join([]string{
		`process_start_time_seconds 1700000002`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="0.002"} 60`,
		`etcd_disk_backend_commit_duration_seconds_bucket{le="+Inf"} 70`,
		"",
	}, "\n"))

	summaries, err := m.Execute(newEtcdTestConfig(t, sshProvider, hosts, "gather"))
	require.NoError(t, err)
	require.Len(t, summaries, 1)

	var got etcdMetrics
	require.NoError(t, json.Unmarshal([]byte(summaries[0].SummaryContent()), &got))
	require.Len(t, got.BackendCommitDuration, 1)
	assert.Equal(t, map[string]int{"0.002": 100, "+Inf": 120}, got.BackendCommitDuration[0].Buckets)
}
