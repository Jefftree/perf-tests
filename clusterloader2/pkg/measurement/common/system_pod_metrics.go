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
	"fmt"
	"maps"
	"strings"
	"sync"

	"gopkg.in/yaml.v2"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/perf-tests/clusterloader2/pkg/measurement"
	"k8s.io/perf-tests/clusterloader2/pkg/util"
)

const (
	systemPodMetricsName              = "SystemPodMetrics"
	systemNamespace                   = "kube-system"
	systemPodMetricsEnabledFlagName   = "systemPodMetricsEnabled"
	restartThresholdOverridesFlagName = "restartCountThresholdOverrides"
	enableRestartCountCheckFlagName   = "enableRestartCountCheck"
	systemPodsFailureEnabledFlagName  = "systemPodsFailureEnabled"
	defaultRestartCountThresholdKey   = "default"
)

func init() {
	if err := measurement.Register(systemPodMetricsName, createSystemPodMetricsMeasurement); err != nil {
		klog.Fatalf("Cannot register %s: %v", systemPodMetricsName, err)
	}
}

func createSystemPodMetricsMeasurement() measurement.Measurement {
	return &systemPodMetricsMeasurement{}
}

// Gathers metrics for system pods, right now it only gathers container restart counts.
// System pods are listed twice: first time for "start" action, second time for "gather" action.
// When executing "gather", initial restart counts are subtracted from the current
// restart counts. In effect, only restarts that happened during test execution
// (between "start" and "gather") are visible in the final summary.
// Restarts occurring between optional "pause" and "unpause" actions are also excluded.
type systemPodMetricsMeasurement struct {
	lock                sync.Mutex
	initSnapshot        *systemPodsMetrics
	pauseSnapshot       *systemPodsMetrics
	accumulatedRestarts map[string]map[string]int32
}

type containerMetrics struct {
	Name              string `json:"name"`
	Image             string `json:"image"`
	RestartCount      int32  `json:"restartCount"`
	LastRestartReason string `json:"lastRestartReason"`
}

type podMetrics struct {
	Name       string             `json:"name"`
	Containers []containerMetrics `json:"containers"`
}

type systemPodsMetrics struct {
	Pods []podMetrics `json:"pods"`
}

// Execute gathers and prints system pod metrics.
func (m *systemPodMetricsMeasurement) Execute(config *measurement.Config) ([]measurement.Summary, error) {
	action, err := util.GetString(config.Params, "action")
	if err != nil {
		return nil, err
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	switch action {
	case "start":
		return m.start(config)
	case "pause":
		return nil, m.pause(config)
	case "unpause":
		return nil, m.unpause(config)
	case "gather":
		return m.gather(config)
	default:
		return nil, fmt.Errorf("unknown action %v", action)
	}
}

func (m *systemPodMetricsMeasurement) start(config *measurement.Config) ([]measurement.Summary, error) {
	systemPodMetricsEnabled, err := util.GetBoolOrDefault(config.Params, systemPodMetricsEnabledFlagName, false)
	if err != nil {
		return nil, err
	}
	if !systemPodMetricsEnabled {
		m.initSnapshot = nil
		m.pauseSnapshot = nil
		m.accumulatedRestarts = nil
		klog.V(2).Info("skipping collection of system pod metrics")
		return []measurement.Summary{}, nil
	}
	if _, err := getThresholdOverrides(config); err != nil {
		return nil, err
	}
	metrics, err := getPodMetrics(config)
	if err != nil {
		return nil, err
	}
	m.initSnapshot = metrics
	m.pauseSnapshot = nil
	m.accumulatedRestarts = nil
	return nil, nil
}

func (m *systemPodMetricsMeasurement) pause(config *measurement.Config) error {
	if m.initSnapshot == nil {
		return nil
	}
	if m.pauseSnapshot != nil {
		klog.Warningf("%s: measurement already paused", m)
		return nil
	}
	metrics, err := getPodMetrics(config)
	if err != nil {
		return err
	}
	m.pauseSnapshot = metrics
	return nil
}

func (m *systemPodMetricsMeasurement) unpause(config *measurement.Config) error {
	if m.initSnapshot == nil {
		return nil
	}
	if m.pauseSnapshot == nil {
		klog.Warningf("%s: measurement already unpaused", m)
		return nil
	}
	metrics, err := getPodMetrics(config)
	if err != nil {
		return err
	}
	m.accumulatedRestarts = accumulateActiveRestarts(m.accumulatedRestarts, m.initSnapshot, m.pauseSnapshot)
	m.initSnapshot = metrics
	m.pauseSnapshot = nil
	return nil
}

func (m *systemPodMetricsMeasurement) gather(config *measurement.Config) ([]measurement.Summary, error) {
	systemPodMetricsEnabled, err := util.GetBoolOrDefault(config.Params, systemPodMetricsEnabledFlagName, false)
	if err != nil {
		return nil, err
	}
	if !systemPodMetricsEnabled {
		klog.V(2).Info("skipping collection of system pod metrics")
		return []measurement.Summary{}, nil
	}
	if m.initSnapshot == nil {
		return nil, fmt.Errorf("start needs to be executed before gather")
	}
	overrides, err := getThresholdOverrides(config)
	if err != nil {
		return nil, err
	}
	metrics, err := getPodMetrics(config)
	if err != nil {
		return nil, err
	}

	accumulated := m.accumulatedRestarts
	baseline := m.initSnapshot
	if m.pauseSnapshot != nil {
		accumulated = accumulateActiveRestarts(m.accumulatedRestarts, m.initSnapshot, m.pauseSnapshot)
		baseline = metrics
	}
	subtractInitialRestartCounts(metrics, baseline)
	addAccumulatedRestartCounts(metrics, accumulated)

	summary, err := buildSummary(metrics)
	if err != nil {
		return nil, err
	}
	if err = validateRestartCounts(metrics, config, overrides); err != nil {
		return summary, err
	}
	return summary, nil
}

func getPodMetrics(config *measurement.Config) (*systemPodsMetrics, error) {
	klog.V(2).Info("collecting system pod metrics...")
	lst, err := getPodList(config.ClusterFramework.GetClientSets().GetClient())
	if err != nil {
		return &systemPodsMetrics{}, err
	}
	return extractMetrics(lst), nil
}

func getPodList(client kubernetes.Interface) (*v1.PodList, error) {
	lst, err := client.CoreV1().Pods(systemNamespace).List(context.TODO(), metav1.ListOptions{
		ResourceVersion: "0", // to read from cache
	})
	if err != nil {
		return nil, err
	}
	return lst, nil
}

func accumulateActiveRestarts(accumulated map[string]map[string]int32, baselineMetrics, currentMetrics *systemPodsMetrics) map[string]map[string]int32 {
	result := make(map[string]map[string]int32, len(accumulated))
	for podName, containers := range accumulated {
		result[podName] = maps.Clone(containers)
	}
	if currentMetrics == nil {
		return result
	}

	baselineRestarts := buildRestartCountMap(baselineMetrics)
	for _, pod := range currentMetrics.Pods {
		for _, container := range pod.Containers {
			delta := container.RestartCount
			if baselineCount, ok := baselineRestarts[pod.Name][container.Name]; ok {
				delta = activeRestartsSinceBaseline(container.RestartCount, baselineCount)
			}
			if delta == 0 {
				continue
			}
			if result[pod.Name] == nil {
				result[pod.Name] = make(map[string]int32)
			}
			result[pod.Name][container.Name] += delta
		}
	}
	return result
}

func subtractInitialRestartCounts(metrics *systemPodsMetrics, initMetrics *systemPodsMetrics) {
	initRestarts := buildRestartCountMap(initMetrics)

	for _, pod := range metrics.Pods {
		for i, container := range pod.Containers {
			initRestartCount, ok := initRestarts[pod.Name][container.Name]
			if !ok {
				continue
			}
			pod.Containers[i].RestartCount = activeRestartsSinceBaseline(container.RestartCount, initRestartCount)
		}
	}
}

func addAccumulatedRestartCounts(metrics *systemPodsMetrics, accumulated map[string]map[string]int32) {
	if len(accumulated) == 0 {
		return
	}
	for _, pod := range metrics.Pods {
		podAccumulated := accumulated[pod.Name]
		if len(podAccumulated) == 0 {
			continue
		}
		for i, container := range pod.Containers {
			pod.Containers[i].RestartCount += podAccumulated[container.Name]
		}
	}
}

func buildRestartCountMap(metrics *systemPodsMetrics) map[string]map[string]int32 {
	if metrics == nil {
		return nil
	}
	restarts := make(map[string]map[string]int32, len(metrics.Pods))
	for _, pod := range metrics.Pods {
		containers := make(map[string]int32, len(pod.Containers))
		for _, container := range pod.Containers {
			containers[container.Name] = container.RestartCount
		}
		restarts[pod.Name] = containers
	}
	return restarts
}

func activeRestartsSinceBaseline(currentCount, baselineCount int32) int32 {
	if currentCount >= baselineCount {
		return currentCount - baselineCount
	}
	// Container restart count dropped below baseline because the pod was recreated with the same name.
	return currentCount
}

func validateRestartCounts(metrics *systemPodsMetrics, config *measurement.Config, overrides map[string]int) error {
	enabled, err := util.GetBoolOrDefault(config.Params, enableRestartCountCheckFlagName, false)
	if err != nil {
		return err
	}
	if !enabled {
		return nil
	}

	failureEnabled, err := util.GetBoolOrDefault(config.Params, systemPodsFailureEnabledFlagName, true)
	if err != nil {
		return err
	}

	violations := make([]string, 0)
	for _, p := range metrics.Pods {
		for _, c := range p.Containers {
			maxAllowedRestarts := getMaxAllowedRestarts(c.Name, overrides)
			if c.RestartCount > int32(maxAllowedRestarts) {
				violation := fmt.Sprintf("RestartCount(%v, %v)=%v, want <= %v",
					p.Name, c.Name, c.RestartCount, maxAllowedRestarts)
				violations = append(violations, violation)
			}
		}
	}

	if len(violations) == 0 {
		return nil
	}
	violationsJoined := strings.Join(violations, "; ")

	if !failureEnabled {
		klog.Warningf("System pod restart counts validation failed but %s is false: %v", systemPodsFailureEnabledFlagName, violationsJoined)
		return nil
	}
	return fmt.Errorf("restart counts violation: %v", violationsJoined)
}

func getMaxAllowedRestarts(containerName string, thresholdOverrides map[string]int) int {
	if override, ok := thresholdOverrides[containerName]; ok {
		return override
	}
	// This allows setting default threshold, which will be used for containers
	// not present in the thresholdOverrides map.
	if override, ok := thresholdOverrides[defaultRestartCountThresholdKey]; ok {
		return override
	}
	return 0 // do not allow any restarts if no override and no default specified
}

/*
getThresholdOverrides deserializes restart count override flag value. The value of
this flag is a map[string]int serialized using yaml format. Note that YamlQuote is used to ensure
proper indentation after gotemplate execution.

Alternatively, we could use yaml map as flag value, but then go templates executor would serialize it
using golang map format (for example "map[c1:4 c2:8]"), but it would require implementation of a parser
for such format. It would also introduce a dependency on golang map serialization format, which might break
clusterloader if format ever changes.
*/
func getThresholdOverrides(config *measurement.Config) (map[string]int, error) {
	serialized, err := util.GetStringOrDefault(config.Params, restartThresholdOverridesFlagName, "")
	if err != nil {
		return make(map[string]int), nil
	}
	var parsed map[string]int
	err = yaml.Unmarshal([]byte(serialized), &parsed)
	if err != nil {
		return nil, err
	}
	klog.V(2).Infof("Loaded restart count threshold overrides: %v", parsed)
	return parsed, nil
}

func extractMetrics(lst *v1.PodList) *systemPodsMetrics {
	metrics := systemPodsMetrics{
		Pods: []podMetrics{},
	}
	for _, pod := range lst.Items {
		podMetrics := podMetrics{
			Containers: []containerMetrics{},
			Name:       pod.Name,
		}
		specImages := make(map[string]string)
		for _, c := range pod.Spec.Containers {
			specImages[c.Name] = c.Image
		}
		for _, container := range pod.Status.ContainerStatuses {
			img := container.Image
			if specImg, ok := specImages[container.Name]; ok {
				img = specImg
			}
			metrics := containerMetrics{
				Name:         container.Name,
				Image:        extractImageName(img),
				RestartCount: container.RestartCount,
			}
			if container.LastTerminationState.Terminated != nil {
				metrics.LastRestartReason = container.LastTerminationState.Terminated.String()
			}
			podMetrics.Containers = append(podMetrics.Containers, metrics)
		}
		metrics.Pods = append(metrics.Pods, podMetrics)
	}
	return &metrics
}

func buildSummary(podMetrics *systemPodsMetrics) ([]measurement.Summary, error) {
	content, err := util.PrettyPrintJSON(podMetrics)
	if err != nil {
		return nil, err
	}

	summary := measurement.CreateSummary(systemPodMetricsName, "json", content)
	return []measurement.Summary{summary}, nil
}

// Dispose cleans up after the measurement.
func (m *systemPodMetricsMeasurement) Dispose() {}

// String returns string representation of this measurement.
func (*systemPodMetricsMeasurement) String() string {
	return systemPodMetricsName
}

func extractImageName(image string) string {
	parts := strings.Split(image, "/")
	if len(parts) == 0 {
		return ""
	}

	// Remove registry if it looks like one (contains dot or colon for port)
	if len(parts) > 1 && (strings.Contains(parts[0], ".") || strings.Contains(parts[0], ":")) {
		parts = parts[1:]
	}

	// Remove tag or digest from the last part
	lastPart := parts[len(parts)-1]
	if i := strings.Index(lastPart, ":"); i != -1 {
		lastPart = lastPart[:i]
	}
	if i := strings.Index(lastPart, "@"); i != -1 {
		lastPart = lastPart[:i]
	}
	parts[len(parts)-1] = lastPart

	return strings.Join(parts, "/")
}
