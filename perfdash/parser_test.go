/*
Copyright 2016 The Kubernetes Authors.

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
	"encoding/json"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/kubernetes/test/e2e/perftype"
)

func Test_parseSystemPodMetrics(t *testing.T) {
	tests := []struct {
		name        string
		data        []byte
		buildNumber int
		testResult  *BuildData
		want        *BuildData
	}{
		{
			name:        "same-container-in-two-pods",
			buildNumber: 123,
			data:        sameContainerInTwoPodsSummary(),
			testResult:  &BuildData{Job: "", Version: "", Builds: NewBuilds(map[string][]perftype.DataItem{})},
			want: &BuildData{Job: "", Version: "", Builds: NewBuilds(map[string][]perftype.DataItem{
				"123": {
					perftype.DataItem{
						Data: map[string]float64{
							"c1": 2,
							"c2": 1,
						},
						Labels: map[string]string{
							"RestartCount": "RestartCount",
						},
						Unit: "",
					},
				},
			}),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parseSystemPodMetrics(tt.data, tt.buildNumber, tt.testResult)
			if !reflect.DeepEqual(*tt.testResult, *tt.want) {
				t.Errorf("want %v, got %v", *tt.want, *tt.testResult)
			}
		})
	}
}

func sameContainerInTwoPodsSummary() []byte {
	json := `{
		"pods": [
			{
				"name": "p1",
				"containers": [
					{
						"name": "c1",
						"restartCount": 2
					},
					{
						"name": "c2",
						"restartCount": 1
					}
				]
			},
			{
				"name": "p2",
				"containers": [
					{
						"name": "c1",
						"restartCount": 0
					}
				]
			}
		]
	}`
	return []byte(json)
}

func Test_parseContainerRestarts(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want *BuildData
	}{
		{
			name: "simple",
			data: []byte(`[
				{
				  "Container": "container1",
				  "Pod": "container1-hostname1",
				  "Namespace": "default",
				  "RestartCount": 1
				},
				{
					"Container": "container2",
					"Pod": "container2-hostname1",
					"Namespace": "default",
					"RestartCount": 3
				},
				{
					"Container": "container1",
					"Pod": "container1-hostname2",
					"Namespace": "default",
					"RestartCount": 4
				}]`),
			want: &BuildData{
				Builds: NewBuilds(map[string][]perftype.DataItem{
					"123": {
						{
							Data: map[string]float64{
								"container1": 5,
								"container2": 3,
							},
							Labels: map[string]string{"RestartCount": "RestartCount"},
						},
					},
				}),
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := &BuildData{Builds: NewBuilds(nil)}
			parseContainerRestarts(tc.data, 123, got)
			require.NotNil(t, got.Builds)
			assert.ElementsMatch(t, tc.want.Builds.Builds("123"), got.Builds.Builds("123"))
		})
	}
}

func Test_parseResourceUsageDataAddsAverageResourcesBackwardCompatible(t *testing.T) {
	data := []byte(`{
		"Perc50": [
			{"Name": "ip-10-0-0-1/kubelet", "CPU": 1, "Mem": 104857600},
			{"Name": "ip-10-0-0-2/kubelet", "CPU": 3, "Mem": 314572800}
		],
		"Perc90": [
			{"Name": "ip-10-0-0-1/kubelet", "CPU": 2, "Mem": 209715200},
			{"Name": "ip-10-0-0-2/kubelet", "CPU": 4, "Mem": 419430400}
		]
	}`)

	got := &BuildData{Builds: NewBuilds(nil)}
	parseResourceUsageData(data, 7, got)

	assert.Equal(t, "v1", got.Version)
	require.NotNil(t, got.Builds)
	assert.ElementsMatch(t, []perftype.DataItem{
		{
			Unit: "cores",
			Labels: map[string]string{
				"PodName":  "ip-10-0-0-1/kubelet",
				"Resource": "CPU",
			},
			Data: map[string]float64{
				"Perc50": 1,
				"Perc90": 2,
			},
		},
		{
			Unit: "MiB",
			Labels: map[string]string{
				"PodName":  "ip-10-0-0-1/kubelet",
				"Resource": "memory",
			},
			Data: map[string]float64{
				"Perc50": 100,
				"Perc90": 200,
			},
		},
		{
			Unit: "cores",
			Labels: map[string]string{
				"PodName":  "ip-10-0-0-2/kubelet",
				"Resource": "CPU",
			},
			Data: map[string]float64{
				"Perc50": 3,
				"Perc90": 4,
			},
		},
		{
			Unit: "MiB",
			Labels: map[string]string{
				"PodName":  "ip-10-0-0-2/kubelet",
				"Resource": "memory",
			},
			Data: map[string]float64{
				"Perc50": 300,
				"Perc90": 400,
			},
		},
		{
			Unit: "cores",
			Labels: map[string]string{
				"PodName":  "all-nodes",
				"Resource": "CPU_Average",
			},
			Data: map[string]float64{
				"Perc50": 2,
				"Perc90": 3,
			},
		},
		{
			Unit: "MiB",
			Labels: map[string]string{
				"PodName":  "all-nodes",
				"Resource": "memory_Average",
			},
			Data: map[string]float64{
				"Perc50": 200,
				"Perc90": 300,
			},
		},
	}, got.Builds.Builds("7"))
}

func Test_BuildDataToJson(t *testing.T) {
	tests := []struct {
		name      string
		buildData *BuildData
		want      string
	}{
		{
			name: "ToJsonTest",
			buildData: &BuildData{Job: "job", Version: "version", Builds: NewBuilds(map[string][]perftype.DataItem{
				"123": {
					perftype.DataItem{
						Data: map[string]float64{
							"c1": 2,
							"c2": 1,
						},
						Labels: map[string]string{
							"RestartCount": "RestartCount",
						},
						Unit: "unit",
					},
				},
			}),
			},
			want: `{"builds":{"123":[{"data":{"c1":2,"c2":1},"unit":"unit","labels":{"RestartCount":"RestartCount"}}]},"job":"job","version":"version"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			jsonData, err := json.Marshal(tc.buildData)
			if err != nil {
				t.Fatalf("failed to marshal data into json %v", err)
			}
			require.NotNil(t, jsonData)
			assert.Equal(t, tc.want, string(jsonData))
			got := &BuildData{}
			err = json.Unmarshal(jsonData, got)
			if err != nil {
				t.Fatalf("failed to unmarshal data into json %v", err)
			}
			if !reflect.DeepEqual(*got, *tc.buildData) {
				t.Errorf("want %v, got %v", *tc.buildData, *got)
			}
		})
	}
}

type fakeMetricsBucket struct {
	builds []int
	files  map[string][]byte
}

func (f *fakeMetricsBucket) GetBuildNumbers(job string) ([]int, error) {
	return f.builds, nil
}

func (f *fakeMetricsBucket) ListFilesInBuild(job string, buildNumber int, prefix string) ([]string, error) {
	var out []string
	for k := range f.files {
		out = append(out, k)
	}
	return out, nil
}

func (f *fakeMetricsBucket) GetFilePrefix(job string, buildNumber int, prefix string) string {
	return joinStringsAndInts(job, buildNumber, prefix)
}

func (f *fakeMetricsBucket) ReadFile(job string, buildNumber int, path string) ([]byte, error) {
	return f.files[joinStringsAndInts(job, buildNumber, path)], nil
}

func Test_getJobDataGenericMeasurementsWithMatchAllTests(t *testing.T) {
	job := "ci-kubernetes-benchmark-etcd-write-throughput"
	artifactPath := "artifacts/GenericPrometheusQuery EtcdWriteThroughput_etcd-write-throughput_2026-09-17T06:53:53Z.json"
	bucket := &fakeMetricsBucket{
		builds: []int{1},
		files: map[string][]byte{
			joinStringsAndInts(job, 1, artifactPath): []byte(`{
				"version": "v1",
				"dataItems": [{
					"data": {"PutThroughput": 988.03, "BackendCommitRate": 6.44},
					"unit": "ops/s"
				}]
			}`),
		},
	}

	downloader := NewDownloader(&DownloaderOptions{DefaultBuildsCount: 1}, bucket, false)
	result := make(JobToCategoryData)
	var wg sync.WaitGroup
	var mu sync.Mutex
	wg.Add(1)
	downloader.getJobData(&wg, result, &mu, job, Tests{
		Prefix:       "benchmark etcd write throughput",
		Descriptions: performanceDescriptions,
		BuildsCount:  1,
		ArtifactsDir: "artifacts",
	})
	wg.Wait()

	require.Contains(t, result, "benchmark etcd write throughput")
	require.Contains(t, result["benchmark etcd write throughput"], "GenericMeasurements")
	require.Contains(t, result["benchmark etcd write throughput"]["GenericMeasurements"], "EtcdWriteThroughput")
	items := result["benchmark etcd write throughput"]["GenericMeasurements"]["EtcdWriteThroughput"].Builds.Builds("1")
	require.Len(t, items, 1)
	assert.InDelta(t, 988.03, items[0].Data["PutThroughput"], 0.01)
}

func Test_parseHistogramMetric(t *testing.T) {
	type buildInput struct {
		buildNumber int
		data        []byte
	}
	tests := []struct {
		name       string
		metricName string
		inputs     []buildInput
		wantBuilds map[string][]perftype.DataItem
	}{
		{
			name:       "merges peerRoundTripTime across peers and drops To label across builds",
			metricName: "peerRoundTripTime",
			inputs: []buildInput{
				{
					buildNumber: 101,
					data: []byte(`{
						"peerRoundTripTime": [
							{
								"labels": {"To": "4fed7f0ddeb072b3", "__name__": "etcd_network_peer_round_trip_time_seconds_bucket"},
								"buckets": {"0.0002": 10, "0.0004": 50, "+Inf": 100}
							},
							{
								"labels": {"To": "9476244e2699b652", "__name__": "etcd_network_peer_round_trip_time_seconds_bucket"},
								"buckets": {"0.0002": 90, "0.0004": 210, "+Inf": 300}
							},
							{
								"labels": {"To": "ed9578e8e68e1f14", "__name__": "etcd_network_peer_round_trip_time_seconds_bucket"},
								"buckets": {"0.0002": 25, "0.0004": 40, "+Inf": 100}
							}
						]
					}`),
				},
				{
					buildNumber: 102,
					data: []byte(`{
						"peerRoundTripTime": [
							{
								"labels": {"To": "1111222233334444", "__name__": "etcd_network_peer_round_trip_time_seconds_bucket"},
								"buckets": {"0.0002": 30, "0.0004": 80, "+Inf": 100}
							},
							{
								"labels": {"To": "5555666677778888", "__name__": "etcd_network_peer_round_trip_time_seconds_bucket"},
								"buckets": {"0.0002": 50, "0.0004": 100, "+Inf": 100}
							}
						]
					}`),
				},
			},
			wantBuilds: map[string][]perftype.DataItem{
				"101": {
					{
						Unit:   "%",
						Labels: map[string]string{},
						Data: map[string]float64{
							"<= 0.0002s": 25,
							"<= 0.0004s": 60,
						},
					},
				},
				"102": {
					{
						Unit:   "%",
						Labels: map[string]string{},
						Data: map[string]float64{
							"<= 0.0002s": 40,
							"<= 0.0004s": 90,
						},
					},
				},
			},
		},
		{
			name:       "preserves single-series backendCommitDuration and strips __name__",
			metricName: "backendCommitDuration",
			inputs: []buildInput{
				{
					buildNumber: 201,
					data: []byte(`{
						"backendCommitDuration": [
							{
								"labels": {"__name__": "etcd_disk_backend_commit_duration_seconds_bucket"},
								"buckets": {"0.002": 60, "0.004": 90, "+Inf": 100}
							}
						]
					}`),
				},
			},
			wantBuilds: map[string][]perftype.DataItem{
				"201": {
					{
						Unit:   "%",
						Labels: map[string]string{},
						Data: map[string]float64{
							"<= 0.002s": 60,
							"<= 0.004s": 90,
						},
					},
				},
			},
		},
		{
			name:       "empty peerRoundTripTime emits no data items",
			metricName: "peerRoundTripTime",
			inputs: []buildInput{
				{
					buildNumber: 301,
					data:        []byte(`{"peerRoundTripTime": []}`),
				},
			},
			wantBuilds: map[string][]perftype.DataItem{
				"301": nil,
			},
		},
		{
			name:       "zero +Inf count emits zero percentages",
			metricName: "peerRoundTripTime",
			inputs: []buildInput{
				{
					buildNumber: 401,
					data: []byte(`{
						"peerRoundTripTime": [
							{
								"labels": {"To": "aaaa1111bbbb2222", "__name__": "etcd_network_peer_round_trip_time_seconds_bucket"},
								"buckets": {"0.0002": 0, "+Inf": 0}
							},
							{
								"labels": {"To": "cccc3333dddd4444", "__name__": "etcd_network_peer_round_trip_time_seconds_bucket"},
								"buckets": {"0.0002": 0, "+Inf": 0}
							}
						]
					}`),
				},
			},
			wantBuilds: map[string][]perftype.DataItem{
				"401": {
					{
						Unit:   "%",
						Labels: map[string]string{},
						Data: map[string]float64{
							"<= 0.0002s": 0,
						},
					},
				},
			},
		},
		{
			name:       "skips malformed entry missing +Inf without corrupting valid peers",
			metricName: "peerRoundTripTime",
			inputs: []buildInput{
				{
					buildNumber: 501,
					data: []byte(`{
						"peerRoundTripTime": [
							{
								"labels": {"To": "aaaa1111bbbb2222", "__name__": "etcd_network_peer_round_trip_time_seconds_bucket"},
								"buckets": {"0.0002": 999}
							},
							{
								"labels": {"To": "cccc3333dddd4444", "__name__": "etcd_network_peer_round_trip_time_seconds_bucket"},
								"buckets": {"0.0002": 20, "+Inf": 80}
							}
						]
					}`),
				},
			},
			wantBuilds: map[string][]perftype.DataItem{
				"501": {
					{
						Unit:   "%",
						Labels: map[string]string{},
						Data: map[string]float64{
							"<= 0.0002s": 25,
						},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := &BuildData{Builds: NewBuilds(nil)}
			parseFn := parseHistogramMetric(tc.metricName)
			for _, input := range tc.inputs {
				parseFn(input.data, input.buildNumber, got)
			}
			assert.Equal(t, "v1", got.Version)
			for build, wantItems := range tc.wantBuilds {
				assert.Equal(t, wantItems, got.Builds.Builds(build))
			}
		})
	}
}
