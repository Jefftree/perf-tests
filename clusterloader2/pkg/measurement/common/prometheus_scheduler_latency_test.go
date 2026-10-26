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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/perf-tests/clusterloader2/pkg/measurement"
	"k8s.io/perf-tests/clusterloader2/pkg/measurement/common/executors"
)

func TestPrometheusSchedulerLatencyGatherer(t *testing.T) {
	executor, err := executors.NewPromqlExecutor("testdata/prometheus_scheduler_latency/leader_failover.yaml")
	require.NoError(t, err)
	defer executor.Close()

	start := time.Unix(0, 0).UTC()
	end := start.Add(10 * time.Minute)
	gatherer := &prometheusSchedulerLatencyGatherer{}

	t.Run("preserves_histograms_across_leader_failover_and_overrides_summary_name", func(t *testing.T) {
		summaries, err := gatherer.Gather(executor, start, end, &measurement.Config{
			Identifier: "TestMetrics",
			Params: map[string]interface{}{
				"summaryName": schedulerLatencyMetricName,
			},
		})
		require.NoError(t, err)
		require.Len(t, summaries, 1)
		assert.Equal(t, "SchedulingMetrics", summaries[0].SummaryName())
		assert.Equal(t, "json", summaries[0].SummaryExt())

		var got schedulingMetrics
		require.NoError(t, json.Unmarshal([]byte(summaries[0].SummaryContent()), &got))

		assert.Greater(t, got.E2eSchedulingLatency.Perc50, time.Duration(0))
		assert.Greater(t, got.E2eSchedulingLatency.Perc90, got.E2eSchedulingLatency.Perc50)
		assert.Greater(t, got.E2eSchedulingLatency.Perc99, time.Second)

		assert.Greater(t, got.SchedulingLatency.Perc50, time.Duration(0))
		assert.Greater(t, got.SchedulingLatency.Perc99, got.SchedulingLatency.Perc50)

		for _, ep := range extentionsPoints {
			require.Contains(t, got.FrameworkExtensionPointDuration, ep)
			require.NotNil(t, got.FrameworkExtensionPointDuration[ep])
		}
		assert.Greater(t, got.FrameworkExtensionPointDuration["Bind"].Perc99, 10*time.Millisecond)
	})

	t.Run("defaults_summary_name_for_standalone_identifiers", func(t *testing.T) {
		draSummaries, err := gatherer.Gather(executor, start, end, &measurement.Config{
			Identifier: "FastFillSchedulingMetrics",
			Params:     map[string]interface{}{},
		})
		require.NoError(t, err)
		require.Len(t, draSummaries, 1)
		assert.Equal(t, "FastFillSchedulingMetrics_PrometheusSchedulingMetrics", draSummaries[0].SummaryName())

		bareSummaries, err := gatherer.Gather(executor, start, end, &measurement.Config{
			Params: map[string]interface{}{},
		})
		require.NoError(t, err)
		require.Len(t, bareSummaries, 1)
		assert.Equal(t, "PrometheusSchedulingMetrics", bareSummaries[0].SummaryName())
	})

	t.Run("rejects_zero_or_inverted_window", func(t *testing.T) {
		_, err := gatherer.Gather(executor, time.Time{}, end, &measurement.Config{})
		assert.Error(t, err)

		_, err = gatherer.Gather(executor, end, start, &measurement.Config{})
		assert.Error(t, err)
	})
}
