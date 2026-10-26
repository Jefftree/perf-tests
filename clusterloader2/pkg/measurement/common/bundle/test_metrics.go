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

package bundle

import (
	"context"
	"fmt"
	"strconv"
	"text/template"

	goerrors "github.com/go-errors/errors"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	clconfig "k8s.io/perf-tests/clusterloader2/pkg/config"
	"k8s.io/perf-tests/clusterloader2/pkg/errors"
	"k8s.io/perf-tests/clusterloader2/pkg/measurement"
	"k8s.io/perf-tests/clusterloader2/pkg/provider"
	"k8s.io/perf-tests/clusterloader2/pkg/util"
)

const (
	testMetricsMeasurementName = "TestMetrics"
)

func init() {
	if err := measurement.Register(testMetricsMeasurementName, createTestMetricsMeasurement); err != nil {
		klog.Fatalf("Cannot register %s: %v", testMetricsMeasurementName, err)
	}
}

func createTestMetricsMeasurement() measurement.Measurement {
	var metrics testMetrics
	var err error
	if metrics.etcdMetrics, err = measurement.CreateMeasurement("EtcdMetrics"); err != nil {
		klog.Errorf("%v: etcdMetrics creation error: %v", metrics, err)
	}
	if metrics.schedulingMetrics, err = measurement.CreateMeasurement("SchedulingMetrics"); err != nil {
		klog.Errorf("%v: schedulingMetrics creation error: %v", metrics, err)
	}
	if metrics.prometheusSchedulingMetrics, err = measurement.CreateMeasurement("PrometheusSchedulingMetrics"); err != nil {
		klog.Errorf("%v: prometheusSchedulingMetrics creation error: %v", metrics, err)
	}
	if metrics.metricsForE2E, err = measurement.CreateMeasurement("MetricsForE2E"); err != nil {
		klog.Errorf("%v: metricsForE2E creation error: %v", metrics, err)
	}
	if metrics.resourceUsageSummary, err = measurement.CreateMeasurement("ResourceUsageSummary"); err != nil {
		klog.Errorf("%v: resourceUsageSummary creation error: %v", metrics, err)
	}
	if metrics.etcdCPUProfile, err = measurement.CreateMeasurement("CPUProfile"); err != nil {
		klog.Errorf("%v: etcdCPUProfile creation error: %v", metrics, err)
	}
	if metrics.etcdMemoryProfile, err = measurement.CreateMeasurement("MemoryProfile"); err != nil {
		klog.Errorf("%v: etcdMemoryProfile creation error: %v", metrics, err)
	}
	if metrics.etcdBlockProfile, err = measurement.CreateMeasurement("BlockProfile"); err != nil {
		klog.Errorf("%v: etcdBlockProfile creation error: %v", metrics, err)
	}
	if metrics.etcdEventsCPUProfile, err = measurement.CreateMeasurement("CPUProfile"); err != nil {
		klog.Errorf("%v: etcdEventsCPUProfile creation error: %v", metrics, err)
	}
	if metrics.etcdEventsMemoryProfile, err = measurement.CreateMeasurement("MemoryProfile"); err != nil {
		klog.Errorf("%v: etcdEventsMemoryProfile creation error: %v", metrics, err)
	}
	if metrics.etcdEventsBlockProfile, err = measurement.CreateMeasurement("BlockProfile"); err != nil {
		klog.Errorf("%v: etcdEventsBlockProfile creation error: %v", metrics, err)
	}
	if metrics.apiserverCPUProfile, err = measurement.CreateMeasurement("CPUProfile"); err != nil {
		klog.Errorf("%v: apiserverCPUProfile creation error: %v", metrics, err)
	}
	if metrics.apiserverMemoryProfile, err = measurement.CreateMeasurement("MemoryProfile"); err != nil {
		klog.Errorf("%v: apiserverMemoryProfile creation error: %v", metrics, err)
	}
	if metrics.apiserverBlockProfile, err = measurement.CreateMeasurement("BlockProfile"); err != nil {
		klog.Errorf("%v: apiserverBlockProfile creation error: %v", metrics, err)
	}
	if metrics.schedulerCPUProfile, err = measurement.CreateMeasurement("CPUProfile"); err != nil {
		klog.Errorf("%v: schedulerCPUProfile creation error: %v", metrics, err)
	}
	if metrics.schedulerMemoryProfile, err = measurement.CreateMeasurement("MemoryProfile"); err != nil {
		klog.Errorf("%v: schedulerMemoryProfile creation error: %v", metrics, err)
	}
	if metrics.schedulerBlockProfile, err = measurement.CreateMeasurement("BlockProfile"); err != nil {
		klog.Errorf("%v: schedulerBlockProfile creation error: %v", metrics, err)
	}
	if metrics.controllerManagerCPUProfile, err = measurement.CreateMeasurement("CPUProfile"); err != nil {
		klog.Errorf("%v: controllerManagerCPUProfile creation error: %v", metrics, err)
	}
	if metrics.controllerManagerMemoryProfile, err = measurement.CreateMeasurement("MemoryProfile"); err != nil {
		klog.Errorf("%v: controllerManagerMemoryProfile creation error: %v", metrics, err)
	}
	if metrics.controllerManagerBlockProfile, err = measurement.CreateMeasurement("BlockProfile"); err != nil {
		klog.Errorf("%v: controllerManagerBlockProfile creation error: %v", metrics, err)
	}
	if metrics.systemPodMetrics, err = measurement.CreateMeasurement("SystemPodMetrics"); err != nil {
		klog.Errorf("%v: systemPodMetrics creation error: %v", metrics, err)
	}
	if metrics.clusterOOMsTracker, err = measurement.CreateMeasurement("ClusterOOMsTracker"); err != nil {
		klog.Errorf("%v: clusterOOMsTracker creation error: %v", metrics, err)
	}
	return &metrics
}

type testMetrics struct {
	etcdMetrics                    measurement.Measurement
	schedulingMetrics              measurement.Measurement
	prometheusSchedulingMetrics    measurement.Measurement
	activeSchedulingMetrics        measurement.Measurement
	metricsForE2E                  measurement.Measurement
	resourceUsageSummary           measurement.Measurement
	etcdCPUProfile                 measurement.Measurement
	etcdMemoryProfile              measurement.Measurement
	etcdBlockProfile               measurement.Measurement
	etcdEventsCPUProfile           measurement.Measurement
	etcdEventsMemoryProfile        measurement.Measurement
	etcdEventsBlockProfile         measurement.Measurement
	apiserverCPUProfile            measurement.Measurement
	apiserverMemoryProfile         measurement.Measurement
	apiserverBlockProfile          measurement.Measurement
	schedulerCPUProfile            measurement.Measurement
	schedulerMemoryProfile         measurement.Measurement
	schedulerBlockProfile          measurement.Measurement
	controllerManagerCPUProfile    measurement.Measurement
	controllerManagerMemoryProfile measurement.Measurement
	controllerManagerBlockProfile  measurement.Measurement
	systemPodMetrics               measurement.Measurement
	clusterOOMsTracker             measurement.Measurement
}

// Execute supports two actions. start - which sets up all metrics.
// stop - which stops all metrics and collects all measurements.
func (t *testMetrics) Execute(config *measurement.Config) ([]measurement.Summary, error) {
	var summaries []measurement.Summary
	errList := errors.NewErrorList()
	action, err := util.GetString(config.Params, "action")
	if err != nil {
		return summaries, err
	}

	actionStartConfig := createConfig(config, map[string]interface{}{
		"action": "start",
	})
	actionGatherConfig := createConfig(config, map[string]interface{}{
		"action": "gather",
	})
	schedulingStartConfig := createConfig(config, map[string]interface{}{
		"action":      "start",
		"summaryName": "SchedulingMetrics",
	})
	schedulingGatherConfig := createConfig(config, map[string]interface{}{
		"action":      "gather",
		"summaryName": "SchedulingMetrics",
	})
	etcdStartConfig := createConfig(config, map[string]interface{}{
		"action":        "start",
		"componentName": "etcd",
	})
	etcdGatherConfig := createConfig(config, map[string]interface{}{
		"action":        "gather",
		"componentName": "etcd",
	})
	etcdEventsStartConfig := createConfig(config, map[string]interface{}{
		"action":        "start",
		"componentName": "etcd-events",
	})
	etcdEventsGatherConfig := createConfig(config, map[string]interface{}{
		"action":        "gather",
		"componentName": "etcd-events",
	})
	kubeApiserverStartConfig := createConfig(config, map[string]interface{}{
		"action":        "start",
		"componentName": "kube-apiserver",
	})
	kubeApiserverGatherConfig := createConfig(config, map[string]interface{}{
		"action":        "gather",
		"componentName": "kube-apiserver",
	})
	kubeSchedulerStartConfig := createConfig(config, map[string]interface{}{
		"action":        "start",
		"componentName": "kube-scheduler",
	})
	kubeSchedulerGatherConfig := createConfig(config, map[string]interface{}{
		"action":        "gather",
		"componentName": "kube-scheduler",
	})
	kubeControllerManagerStartConfig := createConfig(config, map[string]interface{}{
		"action":        "start",
		"componentName": "kube-controller-manager",
	})
	kubeControllerManagerGatherConfig := createConfig(config, map[string]interface{}{
		"action":        "gather",
		"componentName": "kube-controller-manager",
	})

	switch action {
	case "start":
		summary, err := execute(t.etcdMetrics, actionStartConfig)
		appendResults(&summaries, errList, summary, executeError(t.etcdMetrics.String(), action, err))
		t.activeSchedulingMetrics = t.selectSchedulingMetrics(config)
		summary, err = execute(t.activeSchedulingMetrics, schedulingStartConfig)
		appendResults(&summaries, errList, summary, executeError(measurementName(t.activeSchedulingMetrics), action, err))
		summary, err = execute(t.resourceUsageSummary, actionStartConfig)
		appendResults(&summaries, errList, summary, executeError(t.resourceUsageSummary.String(), action, err))
		summary, err = execute(t.etcdCPUProfile, etcdStartConfig)
		appendResults(&summaries, errList, summary, executeError(t.etcdCPUProfile.String(), action, err))
		summary, err = execute(t.etcdMemoryProfile, etcdStartConfig)
		appendResults(&summaries, errList, summary, executeError(t.etcdMemoryProfile.String(), action, err))
		summary, err = execute(t.etcdBlockProfile, etcdStartConfig)
		appendResults(&summaries, errList, summary, executeError(t.etcdBlockProfile.String(), action, err))
		summary, err = execute(t.etcdEventsCPUProfile, etcdEventsStartConfig)
		appendResults(&summaries, errList, summary, executeError(t.etcdEventsCPUProfile.String(), action, err))
		summary, err = execute(t.etcdEventsMemoryProfile, etcdEventsStartConfig)
		appendResults(&summaries, errList, summary, executeError(t.etcdEventsMemoryProfile.String(), action, err))
		summary, err = execute(t.etcdEventsBlockProfile, etcdEventsStartConfig)
		appendResults(&summaries, errList, summary, executeError(t.etcdEventsBlockProfile.String(), action, err))
		summary, err = execute(t.apiserverCPUProfile, kubeApiserverStartConfig)
		appendResults(&summaries, errList, summary, executeError(t.apiserverCPUProfile.String(), action, err))
		summary, err = execute(t.apiserverMemoryProfile, kubeApiserverStartConfig)
		appendResults(&summaries, errList, summary, executeError(t.apiserverMemoryProfile.String(), action, err))
		summary, err = execute(t.apiserverBlockProfile, kubeApiserverStartConfig)
		appendResults(&summaries, errList, summary, executeError(t.apiserverBlockProfile.String(), action, err))
		summary, err = execute(t.schedulerCPUProfile, kubeSchedulerStartConfig)
		appendResults(&summaries, errList, summary, executeError(t.schedulerCPUProfile.String(), action, err))
		summary, err = execute(t.schedulerMemoryProfile, kubeSchedulerStartConfig)
		appendResults(&summaries, errList, summary, executeError(t.schedulerMemoryProfile.String(), action, err))
		summary, err = execute(t.schedulerBlockProfile, kubeSchedulerStartConfig)
		appendResults(&summaries, errList, summary, executeError(t.schedulerBlockProfile.String(), action, err))
		summary, err = execute(t.controllerManagerCPUProfile, kubeControllerManagerStartConfig)
		appendResults(&summaries, errList, summary, executeError(t.controllerManagerCPUProfile.String(), action, err))
		summary, err = execute(t.controllerManagerMemoryProfile, kubeControllerManagerStartConfig)
		appendResults(&summaries, errList, summary, executeError(t.controllerManagerMemoryProfile.String(), action, err))
		summary, err = execute(t.controllerManagerBlockProfile, kubeControllerManagerStartConfig)
		appendResults(&summaries, errList, summary, executeError(t.controllerManagerBlockProfile.String(), action, err))
		summary, err = execute(t.systemPodMetrics, config)
		appendResults(&summaries, errList, summary, executeError(t.systemPodMetrics.String(), action, err))
		summary, err = execute(t.clusterOOMsTracker, config)
		appendResults(&summaries, errList, summary, executeError(t.clusterOOMsTracker.String(), action, err))
	case "pauseSystemPodMetrics":
		forwarded := createConfig(config, map[string]interface{}{"action": "pause"})
		summary, err := execute(t.systemPodMetrics, forwarded)
		appendResults(&summaries, errList, summary, executeError(t.systemPodMetrics.String(), action, err))
	case "unpauseSystemPodMetrics":
		forwarded := createConfig(config, map[string]interface{}{"action": "unpause"})
		summary, err := execute(t.systemPodMetrics, forwarded)
		appendResults(&summaries, errList, summary, executeError(t.systemPodMetrics.String(), action, err))
	case "gather":
		summary, err := execute(t.etcdMetrics, actionGatherConfig)
		appendResults(&summaries, errList, summary, executeError(t.etcdMetrics.String(), action, err))
		schedulingMetrics := t.activeSchedulingMetrics
		if schedulingMetrics == nil {
			schedulingMetrics = t.schedulingMetrics
		}
		summary, err = execute(schedulingMetrics, schedulingGatherConfig)
		appendResults(&summaries, errList, summary, executeError(measurementName(schedulingMetrics), action, err))
		summary, err = execute(t.metricsForE2E, config)
		appendResults(&summaries, errList, summary, executeError(t.metricsForE2E.String(), action, err))
		summary, err = execute(t.resourceUsageSummary, actionGatherConfig)
		appendResults(&summaries, errList, summary, executeError(t.resourceUsageSummary.String(), action, err))
		summary, err = execute(t.etcdCPUProfile, etcdGatherConfig)
		appendResults(&summaries, errList, summary, executeError(t.etcdCPUProfile.String(), action, err))
		summary, err = execute(t.etcdMemoryProfile, etcdGatherConfig)
		appendResults(&summaries, errList, summary, executeError(t.etcdMemoryProfile.String(), action, err))
		summary, err = execute(t.etcdBlockProfile, etcdGatherConfig)
		appendResults(&summaries, errList, summary, executeError(t.etcdBlockProfile.String(), action, err))
		summary, err = execute(t.etcdEventsCPUProfile, etcdEventsGatherConfig)
		appendResults(&summaries, errList, summary, executeError(t.etcdEventsCPUProfile.String(), action, err))
		summary, err = execute(t.etcdEventsMemoryProfile, etcdEventsGatherConfig)
		appendResults(&summaries, errList, summary, executeError(t.etcdEventsMemoryProfile.String(), action, err))
		summary, err = execute(t.etcdEventsBlockProfile, etcdEventsGatherConfig)
		appendResults(&summaries, errList, summary, executeError(t.etcdEventsBlockProfile.String(), action, err))
		summary, err = execute(t.apiserverCPUProfile, kubeApiserverGatherConfig)
		appendResults(&summaries, errList, summary, executeError(t.apiserverCPUProfile.String(), action, err))
		summary, err = execute(t.apiserverMemoryProfile, kubeApiserverGatherConfig)
		appendResults(&summaries, errList, summary, executeError(t.apiserverMemoryProfile.String(), action, err))
		summary, err = execute(t.apiserverBlockProfile, kubeApiserverGatherConfig)
		appendResults(&summaries, errList, summary, executeError(t.apiserverBlockProfile.String(), action, err))
		summary, err = execute(t.schedulerCPUProfile, kubeSchedulerGatherConfig)
		appendResults(&summaries, errList, summary, executeError(t.schedulerCPUProfile.String(), action, err))
		summary, err = execute(t.schedulerMemoryProfile, kubeSchedulerGatherConfig)
		appendResults(&summaries, errList, summary, executeError(t.schedulerMemoryProfile.String(), action, err))
		summary, err = execute(t.schedulerBlockProfile, kubeSchedulerGatherConfig)
		appendResults(&summaries, errList, summary, executeError(t.schedulerBlockProfile.String(), action, err))
		summary, err = execute(t.controllerManagerCPUProfile, kubeControllerManagerGatherConfig)
		appendResults(&summaries, errList, summary, executeError(t.controllerManagerCPUProfile.String(), action, err))
		summary, err = execute(t.controllerManagerMemoryProfile, kubeControllerManagerGatherConfig)
		appendResults(&summaries, errList, summary, executeError(t.controllerManagerMemoryProfile.String(), action, err))
		summary, err = execute(t.controllerManagerBlockProfile, kubeControllerManagerGatherConfig)
		appendResults(&summaries, errList, summary, executeError(t.controllerManagerBlockProfile.String(), action, err))
		summary, err = execute(t.systemPodMetrics, config)
		appendResults(&summaries, errList, summary, executeError(t.systemPodMetrics.String(), action, err))
		summary, err = execute(t.clusterOOMsTracker, config)
		appendResults(&summaries, errList, summary, executeError(t.clusterOOMsTracker.String(), action, err))
	default:
		return summaries, fmt.Errorf("unknown action %v", action)
	}

	if !errList.IsEmpty() {
		klog.Errorf("%s: %v", t, errList.String())
		return summaries, errList
	}
	return summaries, nil
}

// Dispose cleans up after the measurement.
func (t *testMetrics) Dispose() {
	for _, m := range []measurement.Measurement{
		t.etcdMetrics,
		t.schedulingMetrics,
		t.prometheusSchedulingMetrics,
		t.metricsForE2E,
		t.resourceUsageSummary,
		t.etcdCPUProfile,
		t.etcdMemoryProfile,
		t.etcdBlockProfile,
		t.etcdEventsCPUProfile,
		t.etcdEventsMemoryProfile,
		t.etcdEventsBlockProfile,
		t.apiserverCPUProfile,
		t.apiserverMemoryProfile,
		t.apiserverBlockProfile,
		t.schedulerCPUProfile,
		t.schedulerMemoryProfile,
		t.schedulerBlockProfile,
		t.controllerManagerCPUProfile,
		t.controllerManagerMemoryProfile,
		t.controllerManagerBlockProfile,
	} {
		if m != nil {
			m.Dispose()
		}
	}
}

// String returns a string representation of the measurement.
func (*testMetrics) String() string {
	return testMetricsMeasurementName
}

func (t *testMetrics) selectSchedulingMetrics(config *measurement.Config) measurement.Measurement {
	if shouldUsePrometheusSchedulingMetrics(config) && t.prometheusSchedulingMetrics != nil {
		return t.prometheusSchedulingMetrics
	}
	return t.schedulingMetrics
}

// shouldUsePrometheusSchedulingMetrics mirrors prometheus.NewController's master-ip
// ServiceMonitor gating so TestMetrics only queries Prometheus when kube-scheduler is scraped.
func shouldUsePrometheusSchedulingMetrics(config *measurement.Config) bool {
	if config == nil {
		return false
	}
	prometheusClient, err := util.GetStringOrDefault(config.Params, "prometheusClient", "inCluster")
	if err != nil {
		return false
	}
	if prometheusClient == "managed" {
		return resolveCloudProvider(config) != nil
	}
	if prometheusClient != "inCluster" || config.PrometheusFramework == nil {
		return false
	}

	var scrapeApiserverOnly, scrapeApiserverOnlySet bool
	if config.ClusterLoaderConfig != nil {
		clusterCfg := config.ClusterLoaderConfig.ClusterConfig
		promCfg := config.ClusterLoaderConfig.PrometheusConfig
		if !promCfg.UseExistingServer {
			if clusterCfg.MasterDNSEndpoint != "" {
				return false
			}
			if promCfg.ScrapeMastersWithPublicIPs && len(clusterCfg.MasterIPs) == 0 {
				return false
			}
			if config.PrometheusFramework.GetClientSets() != nil {
				ep, err := config.PrometheusFramework.GetClientSets().GetClient().CoreV1().Endpoints("monitoring").Get(context.TODO(), "master", metav1.GetOptions{})
				if err != nil || !hasKubeSchedulerEndpoint(ep) {
					return false
				}
			}
		}
		if mapping, errList := clconfig.GetMapping(config.ClusterLoaderConfig, nil); errList == nil || errList.IsEmpty() {
			if raw, exists := mapping["PROMETHEUS_SCRAPE_APISERVER_ONLY"]; exists && raw != nil {
				scrapeApiserverOnly, _ = template.IsTrue(raw)
				scrapeApiserverOnlySet = true
			}
		}
		if !scrapeApiserverOnlySet {
			if parsed, err := strconv.ParseBool(promCfg.ScrapeApiserverOnly); err == nil {
				scrapeApiserverOnly, scrapeApiserverOnlySet = parsed, true
			}
		}
	}
	if !scrapeApiserverOnlySet {
		if p := resolveCloudProvider(config); p != nil {
			scrapeApiserverOnly = p.Features().ShouldPrometheusScrapeApiserverOnly
		}
	}
	return !scrapeApiserverOnly
}

func resolveCloudProvider(config *measurement.Config) provider.Provider {
	if config.CloudProvider != nil {
		return config.CloudProvider
	}
	if config.ClusterLoaderConfig != nil && config.ClusterLoaderConfig.ClusterConfig.Provider != nil {
		return config.ClusterLoaderConfig.ClusterConfig.Provider
	}
	if config.ClusterFramework != nil && config.ClusterFramework.GetClusterConfig() != nil {
		return config.ClusterFramework.GetClusterConfig().Provider
	}
	return nil
}

func hasKubeSchedulerEndpoint(ep *corev1.Endpoints) bool {
	if ep == nil {
		return false
	}
	for _, subset := range ep.Subsets {
		if len(subset.Addresses) == 0 {
			continue
		}
		for _, port := range subset.Ports {
			if port.Name == "kube-scheduler" {
				return true
			}
		}
	}
	return false
}

func measurementName(m measurement.Measurement) string {
	if m == nil {
		return "SchedulingMetrics"
	}
	return m.String()
}

func createConfig(config *measurement.Config, overrides map[string]interface{}) *measurement.Config {
	params := make(map[string]interface{})
	for k, v := range config.Params {
		params[k] = v
	}
	for k, v := range overrides {
		params[k] = v
	}
	cfg := *config
	cfg.Params = params
	return &cfg
}

func execute(m measurement.Measurement, config *measurement.Config) ([]measurement.Summary, error) {
	if m == nil {
		return nil, fmt.Errorf("uninitialized metric")
	}
	return m.Execute(config)
}

func appendResults(summaries *[]measurement.Summary, errList *errors.ErrorList, summaryResults []measurement.Summary, errResult error) {
	if errResult != nil {
		errList.Append(errResult)
	}
	*summaries = append(*summaries, summaryResults...)
}

func executeError(measurement, action string, err error) error {
	if err != nil {
		return goerrors.Errorf("action %s failed for %s measurement: %v", action, measurement, err)
	}
	return nil
}
