/*
Copyright 2019 The Kubernetes Authors.

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
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"gopkg.in/yaml.v2"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakeclientset "k8s.io/client-go/kubernetes/fake"
	"k8s.io/perf-tests/clusterloader2/pkg/framework"
	"k8s.io/perf-tests/clusterloader2/pkg/measurement"
)

func Test_subtractInitialRestartCounts(t *testing.T) {
	tests := []struct {
		name        string
		metrics     *systemPodsMetrics
		initMetrics *systemPodsMetrics
		want        *systemPodsMetrics
	}{
		{
			name:        "same-pods-and-containers",
			metrics:     generatePodMetrics("p1", "c1", 5),
			initMetrics: generatePodMetrics("p1", "c1", 4),
			want:        generatePodMetrics("p1", "c1", 1),
		},
		{
			name:        "different-container-names",
			metrics:     generatePodMetrics("p1", "c1", 5),
			initMetrics: generatePodMetrics("p1", "c2", 4),
			want:        generatePodMetrics("p1", "c1", 5),
		},
		{
			name:        "different-pod-names",
			metrics:     generatePodMetrics("p1", "c1", 5),
			initMetrics: generatePodMetrics("p2", "c1", 4),
			want:        generatePodMetrics("p1", "c1", 5),
		},
		{
			name:        "recreated-pod-with-lower-restart-count",
			metrics:     generatePodMetrics("p1", "c1", 1),
			initMetrics: generatePodMetrics("p1", "c1", 4),
			want:        generatePodMetrics("p1", "c1", 1),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subtractInitialRestartCounts(tt.metrics, tt.initMetrics)
			if !reflect.DeepEqual(*tt.metrics, *tt.want) {
				t.Errorf("want %v, got %v", *tt.want, *tt.metrics)
			}
		})
	}
}

func Test_validateRestartCounts(t *testing.T) {
	tests := []struct {
		name    string
		metrics *systemPodsMetrics
		config  *measurement.Config
		wantErr bool
	}{
		{
			name:    "check-disabled",
			metrics: generatePodMetrics("p", "c", 1),
			config:  buildConfig(t, false, true, nil),
			wantErr: false,
		},
		{
			name:    "check-enabled-violation",
			metrics: generatePodMetrics("p", "c", 1),
			config:  buildConfig(t, true, true, nil),
			wantErr: true,
		},
		{
			name:    "check-enabled-failure-disabled",
			metrics: generatePodMetrics("p", "c", 1),
			config:  buildConfig(t, true, false, nil),
			wantErr: false,
		},
		{
			name:    "check-enabled-ok",
			metrics: generatePodMetrics("p", "c", 0),
			config:  buildConfig(t, true, true, nil),
			wantErr: false,
		},
		{
			name:    "override-equal-to-actual-count",
			metrics: generatePodMetrics("p", "c", 3),
			config:  buildConfig(t, true, true, map[string]int{"c": 3}),
			wantErr: false,
		},
		{
			name:    "override-default-used",
			metrics: generatePodMetrics("p", "c", 3),
			config:  buildConfig(t, true, true, map[string]int{"default": 3}),
			wantErr: false,
		},
		{
			name:    "override-default-not-used",
			metrics: generatePodMetrics("p", "c", 3),
			config: buildConfig(t, true, true, map[string]int{
				"default": 5,
				"c":       0,
			}),
			wantErr: true,
		},
		{
			name:    "override-below-actual-count",
			metrics: generatePodMetrics("p", "c", 3),
			config:  buildConfig(t, true, true, map[string]int{"c": 2}),
			wantErr: true,
		},
		{
			name:    "override-for-different-container",
			metrics: generatePodMetrics("p", "c1", 3),
			config:  buildConfig(t, true, true, map[string]int{"c2": 4}),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			overrides, err := getThresholdOverrides(tt.config)
			if err != nil {
				t.Fatalf("getThresholdOverrides() error = %v", err)
			}
			if err := validateRestartCounts(tt.metrics, tt.config, overrides); (err != nil) != tt.wantErr {
				t.Errorf("verifyViolations() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func generatePodMetrics(podName string, contName string, restartCount int32) *systemPodsMetrics {
	return &systemPodsMetrics{
		Pods: []podMetrics{
			{
				Name: podName,
				Containers: []containerMetrics{
					{
						Name:         contName,
						RestartCount: restartCount,
					},
				}},
		},
	}
}

func buildConfig(t *testing.T, checkEnabled bool, failureEnabled bool, thresholdOverrides map[string]int) *measurement.Config {
	serializedOverrides, err := yaml.Marshal(thresholdOverrides)
	if err != nil {
		t.Fatal(err)
	}
	return &measurement.Config{
		Params: map[string]interface{}{
			"enableRestartCountCheck":        checkEnabled,
			"systemPodsFailureEnabled":       failureEnabled,
			"restartCountThresholdOverrides": string(serializedOverrides),
		},
	}
}

func Test_extractImageName(t *testing.T) {
	tests := []struct {
		name  string
		image string
		want  string
	}{
		{
			name:  "GCR image",
			image: "gcr.io/gke-release-staging/k8s-dns-node-cache:1.22.0",
			want:  "gke-release-staging/k8s-dns-node-cache",
		},
		{
			name:  "GCR image with custom domain",
			image: "us.gcr.io/gke-release-staging/calico/node:v3.26.3-gke.16",
			want:  "gke-release-staging/calico/node",
		},
		{
			name:  "Artifact Registry image",
			image: "us-central1-docker.pkg.dev/gke-release-staging/gke-release-staging/calico/node:v3.26.3-gke.16",
			want:  "gke-release-staging/gke-release-staging/calico/node",
		},
		{
			name:  "k8s.gcr.io image",
			image: "k8s.gcr.io/pause:3.6",
			want:  "pause",
		},
		{
			name:  "Custom registry image",
			image: "my-registry.com:5000/my-image:tag",
			want:  "my-image",
		},
		{
			name:  "Digest image",
			image: "gcr.io/gke-release-staging/anet/cni-writer@sha256:12345",
			want:  "gke-release-staging/anet/cni-writer",
		},
		{
			name:  "Simple image name",
			image: "busybox",
			want:  "busybox",
		},
		{
			name:  "Empty image",
			image: "",
			want:  "",
		},
		{
			name:  "GKE project without registry",
			image: "gke-release-staging/event-exporter:tag",
			want:  "gke-release-staging/event-exporter",
		},
		{
			name:  "GKE project without registry and tag",
			image: "gke-release/kube-proxy",
			want:  "gke-release/kube-proxy",
		},
		{
			name:  "AWS ECR image",
			image: "602401143452.dkr.ecr.us-west-2.amazonaws.com/eks/kube-proxy:v1.22.15-eksbuild.1",
			want:  "eks/kube-proxy",
		},
		{
			name:  "Docker Hub library image",
			image: "docker.io/library/busybox:latest",
			want:  "library/busybox",
		},
		{
			name:  "registry.k8s.io image",
			image: "registry.k8s.io/sig-storage/csi-provisioner:v3.0.0",
			want:  "sig-storage/csi-provisioner",
		},
		{
			name:  "Quay.io image",
			image: "quay.io/coreos/etcd:v3.5.0",
			want:  "coreos/etcd",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractImageName(tt.image); got != tt.want {
				t.Errorf("extractImageName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_extractMetrics(t *testing.T) {
	podList := &v1.PodList{
		Items: []v1.Pod{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "pod1"},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "container1",
							Image: "gcr.io/gke-release-staging/k8s-dns-node-cache:1.22.0",
						},
						{
							Name:  "container2",
							Image: "gke-release-staging/event-exporter:v1.0.0",
						},
					},
				},
				Status: v1.PodStatus{
					ContainerStatuses: []v1.ContainerStatus{
						{
							Name:         "container1",
							Image:        "sha256:digest1",
							RestartCount: 1,
						},
						{
							Name:         "container2",
							Image:        "sha256:digest2",
							RestartCount: 2,
						},
					},
				},
			},
		},
	}

	want := &systemPodsMetrics{
		Pods: []podMetrics{
			{
				Name: "pod1",
				Containers: []containerMetrics{
					{
						Name:         "container1",
						Image:        "gke-release-staging/k8s-dns-node-cache",
						RestartCount: 1,
					},
					{
						Name:         "container2",
						Image:        "gke-release-staging/event-exporter",
						RestartCount: 2,
					},
				},
			},
		},
	}

	got := extractMetrics(podList)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("extractMetrics() = %v, want %v", got, want)
	}
}

type systemPodStep struct {
	action    string
	pods      []v1.Pod
	overrides map[string]int
}

func Test_Execute_PauseUnpause(t *testing.T) {
	tests := []struct {
		name    string
		steps   []systemPodStep
		want    map[string]map[string]int32
		wantErr bool
	}{
		{
			name: "preserves-restarts-before-pause-and-excludes-paused-window",
			steps: []systemPodStep{
				{
					action: "start",
					pods: []v1.Pod{
						makeSystemPod("worker-pod", "worker", 1),
						makeSystemPod("apiserver-1", "kube-apiserver", 2),
					},
				},
				{
					action: "pause",
					pods: []v1.Pod{
						makeSystemPod("worker-pod", "worker", 3),
						makeSystemPod("apiserver-1", "kube-apiserver", 2),
					},
				},
				{
					action: "unpause",
					pods: []v1.Pod{
						makeSystemPod("worker-pod", "worker", 4),
						makeSystemPod("apiserver-1", "kube-apiserver", 3),
					},
				},
				{
					action: "gather",
					pods: []v1.Pod{
						makeSystemPod("worker-pod", "worker", 5),
						makeSystemPod("apiserver-1", "kube-apiserver", 3),
					},
					overrides: map[string]int{"worker": 2},
				},
			},
			want: map[string]map[string]int32{
				"worker-pod":  {"worker": 3},
				"apiserver-1": {"kube-apiserver": 0},
			},
			wantErr: true,
		},
		{
			name: "pod-created-during-pause-window",
			steps: []systemPodStep{
				{
					action: "start",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 0)},
				},
				{
					action: "pause",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 0)},
				},
				{
					action: "unpause",
					pods: []v1.Pod{
						makeSystemPod("pod-1", "c1", 0),
						makeSystemPod("pod-new", "c1", 2),
					},
				},
				{
					action: "gather",
					pods: []v1.Pod{
						makeSystemPod("pod-1", "c1", 0),
						makeSystemPod("pod-new", "c1", 3),
					},
					overrides: map[string]int{"c1": 1},
				},
			},
			want: map[string]map[string]int32{
				"pod-1":   {"c1": 0},
				"pod-new": {"c1": 1},
			},
			wantErr: false,
		},
		{
			name: "pod-recreated-with-same-name-during-pause-window",
			steps: []systemPodStep{
				{
					action: "start",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 3)},
				},
				{
					action: "pause",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 5)},
				},
				{
					action: "unpause",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 1)},
				},
				{
					action:    "gather",
					pods:      []v1.Pod{makeSystemPod("pod-1", "c1", 2)},
					overrides: map[string]int{"c1": 3},
				},
			},
			want: map[string]map[string]int32{
				"pod-1": {"c1": 3},
			},
			wantErr: false,
		},
		{
			name: "pod-recreated-with-same-name-during-pause-with-zero-active-restarts",
			steps: []systemPodStep{
				{
					action: "start",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 4)},
				},
				{
					action: "pause",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 4)},
				},
				{
					action: "unpause",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 0)},
				},
				{
					action: "gather",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 0)},
				},
			},
			want: map[string]map[string]int32{
				"pod-1": {"c1": 0},
			},
			wantErr: false,
		},
		{
			name: "pod-recreated-with-same-name-after-unpause-preserves-pre-pause-restarts",
			steps: []systemPodStep{
				{
					action: "start",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 1)},
				},
				{
					action: "pause",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 3)},
				},
				{
					action: "unpause",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 7)},
				},
				{
					action:    "gather",
					pods:      []v1.Pod{makeSystemPod("pod-1", "c1", 1)},
					overrides: map[string]int{"c1": 3},
				},
			},
			want: map[string]map[string]int32{
				"pod-1": {"c1": 3},
			},
			wantErr: false,
		},
		{
			name: "pod-recreated-during-pause-and-recreated-again-after-unpause",
			steps: []systemPodStep{
				{
					action: "start",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 0)},
				},
				{
					action: "pause",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 5)},
				},
				{
					action: "unpause",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 2)},
				},
				{
					action:    "gather",
					pods:      []v1.Pod{makeSystemPod("pod-1", "c1", 0)},
					overrides: map[string]int{"c1": 5},
				},
			},
			want: map[string]map[string]int32{
				"pod-1": {"c1": 5},
			},
			wantErr: false,
		},
		{
			name: "pod-created-before-pause-and-restarted-during-pause",
			steps: []systemPodStep{
				{
					action: "start",
					pods:   []v1.Pod{},
				},
				{
					action: "pause",
					pods:   []v1.Pod{makeSystemPod("pod-late", "c1", 2)},
				},
				{
					action: "unpause",
					pods:   []v1.Pod{makeSystemPod("pod-late", "c1", 5)},
				},
				{
					action:    "gather",
					pods:      []v1.Pod{makeSystemPod("pod-late", "c1", 6)},
					overrides: map[string]int{"c1": 3},
				},
			},
			want: map[string]map[string]int32{
				"pod-late": {"c1": 3},
			},
			wantErr: false,
		},
		{
			name: "pod-deleted-before-pause-and-recreated-during-pause",
			steps: []systemPodStep{
				{
					action: "start",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 4)},
				},
				{
					action: "pause",
					pods:   []v1.Pod{},
				},
				{
					action: "unpause",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 1)},
				},
				{
					action: "gather",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 1)},
				},
			},
			want: map[string]map[string]int32{
				"pod-1": {"c1": 0},
			},
			wantErr: false,
		},
		{
			name: "pod-deleted-during-pause-and-recreated-after-unpause",
			steps: []systemPodStep{
				{
					action: "start",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 2)},
				},
				{
					action: "pause",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 4)},
				},
				{
					action: "unpause",
					pods:   []v1.Pod{},
				},
				{
					action:    "gather",
					pods:      []v1.Pod{makeSystemPod("pod-1", "c1", 1)},
					overrides: map[string]int{"c1": 3},
				},
			},
			want: map[string]map[string]int32{
				"pod-1": {"c1": 3},
			},
			wantErr: false,
		},
		{
			name: "repeated-pause-unpause-cycles",
			steps: []systemPodStep{
				{
					action: "start",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 1)},
				},
				{
					action: "pause",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 3)},
				},
				{
					action: "unpause",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 7)},
				},
				{
					action: "pause",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 8)},
				},
				{
					action: "unpause",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 2)},
				},
				{
					action:    "gather",
					pods:      []v1.Pod{makeSystemPod("pod-1", "c1", 5)},
					overrides: map[string]int{"c1": 6},
				},
			},
			want: map[string]map[string]int32{
				"pod-1": {"c1": 6},
			},
			wantErr: false,
		},
		{
			name: "second-pause-cycle-when-pod-deleted-after-first-unpause-and-recreated",
			steps: []systemPodStep{
				{
					action: "start",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 0)},
				},
				{
					action: "pause",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 5)},
				},
				{
					action: "unpause",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 6)},
				},
				{
					action: "pause",
					pods:   []v1.Pod{},
				},
				{
					action: "unpause",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 2)},
				},
				{
					action:    "gather",
					pods:      []v1.Pod{makeSystemPod("pod-1", "c1", 3)},
					overrides: map[string]int{"c1": 6},
				},
			},
			want: map[string]map[string]int32{
				"pod-1": {"c1": 6},
			},
			wantErr: false,
		},
		{
			name: "unpause-without-pause-is-noop",
			steps: []systemPodStep{
				{
					action: "start",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 1)},
				},
				{
					action: "unpause",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 3)},
				},
				{
					action:    "gather",
					pods:      []v1.Pod{makeSystemPod("pod-1", "c1", 4)},
					overrides: map[string]int{"c1": 3},
				},
			},
			want: map[string]map[string]int32{
				"pod-1": {"c1": 3},
			},
			wantErr: false,
		},
		{
			name: "duplicate-pause-preserves-first-pause-snapshot",
			steps: []systemPodStep{
				{
					action: "start",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 1)},
				},
				{
					action: "pause",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 3)},
				},
				{
					action: "pause",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 6)},
				},
				{
					action: "unpause",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 7)},
				},
				{
					action:    "gather",
					pods:      []v1.Pod{makeSystemPod("pod-1", "c1", 7)},
					overrides: map[string]int{"c1": 2},
				},
			},
			want: map[string]map[string]int32{
				"pod-1": {"c1": 2},
			},
			wantErr: false,
		},
		{
			name: "gather-while-paused-excludes-open-pause-window",
			steps: []systemPodStep{
				{
					action: "start",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 1)},
				},
				{
					action: "pause",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 3)},
				},
				{
					action:    "gather",
					pods:      []v1.Pod{makeSystemPod("pod-1", "c1", 7)},
					overrides: map[string]int{"c1": 2},
				},
			},
			want: map[string]map[string]int32{
				"pod-1": {"c1": 2},
			},
			wantErr: false,
		},
		{
			name: "gather-while-paused-then-unpause-and-gather",
			steps: []systemPodStep{
				{
					action: "start",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 1)},
				},
				{
					action: "pause",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 3)},
				},
				{
					action:    "gather",
					pods:      []v1.Pod{makeSystemPod("pod-1", "c1", 7)},
					overrides: map[string]int{"c1": 2},
				},
				{
					action: "unpause",
					pods:   []v1.Pod{makeSystemPod("pod-1", "c1", 9)},
				},
				{
					action:    "gather",
					pods:      []v1.Pod{makeSystemPod("pod-1", "c1", 10)},
					overrides: map[string]int{"c1": 3},
				},
			},
			want: map[string]map[string]int32{
				"pod-1": {"c1": 3},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fakeclientset.NewSimpleClientset()
			clusterFramework := framework.NewFrameworkFromClients(
				framework.NewMultiClientSetFromClients(fakeClient),
				nil,
			)
			m := createSystemPodMetricsMeasurement()

			var summaries []measurement.Summary
			var gatherErr error
			for i, step := range tt.steps {
				replaceSystemPods(t, fakeClient, step.pods)
				config := buildExecuteConfig(t, clusterFramework, step.action, step.overrides)
				summaries, gatherErr = m.Execute(config)
				if (i < len(tt.steps)-1 || step.action != "gather") && gatherErr != nil {
					t.Fatalf("Execute(%q) step %d unexpected error: %v", step.action, i, gatherErr)
				}
			}

			if (gatherErr != nil) != tt.wantErr {
				t.Fatalf("Execute(\"gather\") error = %v, wantErr %v", gatherErr, tt.wantErr)
			}
			if len(summaries) != 1 {
				t.Fatalf("got %d summaries, want 1", len(summaries))
			}

			var parsed systemPodsMetrics
			if err := json.Unmarshal([]byte(summaries[0].SummaryContent()), &parsed); err != nil {
				t.Fatalf("failed to unmarshal summary: %v", err)
			}
			got := make(map[string]map[string]int32, len(parsed.Pods))
			for _, p := range parsed.Pods {
				got[p.Name] = make(map[string]int32, len(p.Containers))
				for _, c := range p.Containers {
					got[p.Name][c.Name] = c.RestartCount
				}
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("gather restart counts = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_Execute_PauseUnpauseBeforeStart(t *testing.T) {
	m := createSystemPodMetricsMeasurement()
	for _, action := range []string{"pause", "unpause"} {
		summaries, err := m.Execute(&measurement.Config{
			Params: map[string]interface{}{"action": action},
		})
		if err != nil {
			t.Fatalf("Execute(%q) before start returned error: %v", action, err)
		}
		if summaries != nil {
			t.Fatalf("Execute(%q) before start returned summaries: %v", action, summaries)
		}
	}
}

func Test_Execute_DisabledAndUnknownAction(t *testing.T) {
	m := createSystemPodMetricsMeasurement()
	for _, action := range []string{"start", "pause", "unpause", "gather"} {
		params := map[string]interface{}{"action": action}
		if action == "start" || action == "gather" {
			params[systemPodMetricsEnabledFlagName] = false
		}
		summaries, err := m.Execute(&measurement.Config{Params: params})
		if err != nil {
			t.Fatalf("Execute(%q) when disabled returned error: %v", action, err)
		}
		if len(summaries) != 0 {
			t.Fatalf("Execute(%q) when disabled returned summaries: %v", action, summaries)
		}
	}

	if _, err := m.Execute(&measurement.Config{
		Params: map[string]interface{}{"action": "unknown"},
	}); err == nil {
		t.Fatal("Execute(\"unknown\") = nil, want error")
	}
}

func makeSystemPod(podName, containerName string, restartCount int32) v1.Pod {
	return v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: systemNamespace,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  containerName,
					Image: "registry.k8s.io/pause:3.9",
				},
			},
		},
		Status: v1.PodStatus{
			ContainerStatuses: []v1.ContainerStatus{
				{
					Name:         containerName,
					Image:        "registry.k8s.io/pause:3.9",
					RestartCount: restartCount,
				},
			},
		},
	}
}

func replaceSystemPods(t *testing.T, client *fakeclientset.Clientset, pods []v1.Pod) {
	t.Helper()
	existing, err := client.CoreV1().Pods(systemNamespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("failed listing fake pods: %v", err)
	}
	for _, p := range existing.Items {
		if err := client.CoreV1().Pods(systemNamespace).Delete(context.Background(), p.Name, metav1.DeleteOptions{}); err != nil {
			t.Fatalf("failed deleting fake pod %s: %v", p.Name, err)
		}
	}
	for i := range pods {
		if _, err := client.CoreV1().Pods(systemNamespace).Create(context.Background(), &pods[i], metav1.CreateOptions{}); err != nil {
			t.Fatalf("failed creating fake pod %s: %v", pods[i].Name, err)
		}
	}
}

func buildExecuteConfig(t *testing.T, clusterFramework *framework.Framework, action string, overrides map[string]int) *measurement.Config {
	t.Helper()
	params := map[string]interface{}{
		"action": action,
	}
	// Match restart-apiserver.yaml: pause and unpause only pass "action".
	if action == "start" || action == "gather" {
		serializedOverrides, err := yaml.Marshal(overrides)
		if err != nil {
			t.Fatalf("failed marshaling overrides: %v", err)
		}
		params[systemPodMetricsEnabledFlagName] = true
		params[enableRestartCountCheckFlagName] = true
		params[systemPodsFailureEnabledFlagName] = true
		params[restartThresholdOverridesFlagName] = string(serializedOverrides)
	}
	return &measurement.Config{
		ClusterFramework: clusterFramework,
		Params:           params,
	}
}
