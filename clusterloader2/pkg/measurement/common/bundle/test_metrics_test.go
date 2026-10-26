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

package bundle

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/version"
	fakeclientset "k8s.io/client-go/kubernetes/fake"

	clconfig "k8s.io/perf-tests/clusterloader2/pkg/config"
	"k8s.io/perf-tests/clusterloader2/pkg/framework"
	"k8s.io/perf-tests/clusterloader2/pkg/measurement"
	"k8s.io/perf-tests/clusterloader2/pkg/provider"
)

type fakeSubMeasurement struct {
	name      string
	calls     []*measurement.Config
	disposed  bool
	summaries []measurement.Summary
}

func (f *fakeSubMeasurement) Execute(config *measurement.Config) ([]measurement.Summary, error) {
	f.calls = append(f.calls, config)
	return f.summaries, nil
}

func (f *fakeSubMeasurement) Dispose() {
	f.disposed = true
}

func (f *fakeSubMeasurement) String() string {
	return f.name
}

type fakeProvider struct {
	provider.Provider
	features provider.Features
}

func (p *fakeProvider) Features() *provider.Features {
	return &p.features
}

func newStubbedTestMetrics(directSched, promSched measurement.Measurement) *testMetrics {
	nop := func() measurement.Measurement { return &fakeSubMeasurement{name: "nop"} }
	return &testMetrics{
		etcdMetrics:                    nop(),
		schedulingMetrics:              directSched,
		prometheusSchedulingMetrics:    promSched,
		metricsForE2E:                  nop(),
		resourceUsageSummary:           nop(),
		etcdCPUProfile:                 nop(),
		etcdMemoryProfile:              nop(),
		etcdBlockProfile:               nop(),
		etcdEventsCPUProfile:           nop(),
		etcdEventsMemoryProfile:        nop(),
		etcdEventsBlockProfile:         nop(),
		apiserverCPUProfile:            nop(),
		apiserverMemoryProfile:         nop(),
		apiserverBlockProfile:          nop(),
		schedulerCPUProfile:            nop(),
		schedulerMemoryProfile:         nop(),
		schedulerBlockProfile:          nop(),
		controllerManagerCPUProfile:    nop(),
		controllerManagerMemoryProfile: nop(),
		controllerManagerBlockProfile:  nop(),
		systemPodMetrics:               nop(),
		clusterOOMsTracker:             nop(),
	}
}

func TestShouldUsePrometheusSchedulingMetrics(t *testing.T) {
	promFramework := &framework.Framework{}
	gceLike := &fakeProvider{features: provider.Features{ShouldPrometheusScrapeApiserverOnly: false}}
	apiserverOnlyProvider := &fakeProvider{features: provider.Features{ShouldPrometheusScrapeApiserverOnly: true}}

	promFrameworkWithSchedulerEndpoint := framework.NewFrameworkFromClients(
		framework.NewMultiClientSetFromClients(fakeclientset.NewSimpleClientset(&corev1.Endpoints{
			ObjectMeta: metav1.ObjectMeta{Namespace: "monitoring", Name: "master"},
			Subsets: []corev1.EndpointSubset{{
				Addresses: []corev1.EndpointAddress{{IP: "10.0.0.1"}},
				Ports:     []corev1.EndpointPort{{Name: "apiserver", Port: 443}, {Name: "kube-scheduler", Port: 10259}},
			}},
		})),
		nil,
	)
	promFrameworkApiserverOnlyEndpoint := framework.NewFrameworkFromClients(
		framework.NewMultiClientSetFromClients(fakeclientset.NewSimpleClientset(&corev1.Endpoints{
			ObjectMeta: metav1.ObjectMeta{Namespace: "monitoring", Name: "master"},
			Subsets: []corev1.EndpointSubset{{
				Addresses: []corev1.EndpointAddress{{IP: "10.0.0.1"}},
				Ports:     []corev1.EndpointPort{{Name: "apiserver", Port: 443}},
			}},
		})),
		nil,
	)

	tmpDir := t.TempDir()
	boolFalseOverride := filepath.Join(tmpDir, "bool_false.yaml")
	require.NoError(t, os.WriteFile(boolFalseOverride, []byte("PROMETHEUS_SCRAPE_APISERVER_ONLY: false\n"), 0o600))
	quotedFalseOverride := filepath.Join(tmpDir, "quoted_false.yaml")
	require.NoError(t, os.WriteFile(quotedFalseOverride, []byte("PROMETHEUS_SCRAPE_APISERVER_ONLY: \"false\"\n"), 0o600))

	cases := []struct {
		name   string
		config *measurement.Config
		want   bool
	}{
		{
			name:   "nil_config",
			config: nil,
			want:   false,
		},
		{
			name: "prometheus_disabled",
			config: &measurement.Config{
				CloudProvider: gceLike,
			},
			want: false,
		},
		{
			name: "nil_cluster_config_on_cluster_framework",
			config: &measurement.Config{
				PrometheusFramework: promFramework,
				ClusterFramework:    &framework.Framework{},
			},
			want: true,
		},
		{
			name: "managed_prometheus_client_with_provider",
			config: &measurement.Config{
				CloudProvider: apiserverOnlyProvider,
				Params:        map[string]interface{}{"prometheusClient": "managed"},
			},
			want: true,
		},
		{
			name: "gce_with_prometheus_enabled",
			config: &measurement.Config{
				PrometheusFramework: promFramework,
				CloudProvider:       gceLike,
				ClusterLoaderConfig: &clconfig.ClusterLoaderConfig{
					ClusterConfig:    clconfig.ClusterConfig{Provider: gceLike},
					PrometheusConfig: clconfig.PrometheusConfig{EnableServer: true},
				},
			},
			want: true,
		},
		{
			name: "live_master_endpoints_with_scheduler_port",
			config: &measurement.Config{
				PrometheusFramework: promFrameworkWithSchedulerEndpoint,
				CloudProvider:       gceLike,
				ClusterLoaderConfig: &clconfig.ClusterLoaderConfig{
					ClusterConfig:    clconfig.ClusterConfig{Provider: gceLike},
					PrometheusConfig: clconfig.PrometheusConfig{EnableServer: true},
				},
			},
			want: true,
		},
		{
			name: "live_master_endpoints_missing_scheduler_port_falls_back",
			config: &measurement.Config{
				PrometheusFramework: promFrameworkApiserverOnlyEndpoint,
				CloudProvider:       gceLike,
				ClusterLoaderConfig: &clconfig.ClusterLoaderConfig{
					ClusterConfig:    clconfig.ClusterConfig{Provider: gceLike},
					PrometheusConfig: clconfig.PrometheusConfig{EnableServer: true},
				},
			},
			want: false,
		},
		{
			name: "public_master_ips_requested_without_public_ips_falls_back",
			config: &measurement.Config{
				PrometheusFramework: promFramework,
				CloudProvider:       gceLike,
				ClusterLoaderConfig: &clconfig.ClusterLoaderConfig{
					ClusterConfig: clconfig.ClusterConfig{Provider: gceLike},
					PrometheusConfig: clconfig.PrometheusConfig{
						EnableServer:               true,
						ScrapeMastersWithPublicIPs: true,
					},
				},
			},
			want: false,
		},
		{
			name: "apiserver_only_provider_falls_back_to_direct_scrape",
			config: &measurement.Config{
				PrometheusFramework: promFramework,
				CloudProvider:       apiserverOnlyProvider,
				ClusterLoaderConfig: &clconfig.ClusterLoaderConfig{
					ClusterConfig:    clconfig.ClusterConfig{Provider: apiserverOnlyProvider},
					PrometheusConfig: clconfig.PrometheusConfig{EnableServer: true},
				},
			},
			want: false,
		},
		{
			name: "apiserver_only_provider_with_explicit_false_override_uses_prometheus",
			config: &measurement.Config{
				PrometheusFramework: promFramework,
				CloudProvider:       apiserverOnlyProvider,
				ClusterLoaderConfig: &clconfig.ClusterLoaderConfig{
					ClusterConfig: clconfig.ClusterConfig{Provider: apiserverOnlyProvider},
					PrometheusConfig: clconfig.PrometheusConfig{
						EnableServer:        true,
						ScrapeApiserverOnly: "false",
					},
				},
			},
			want: true,
		},
		{
			name: "override_file_bool_false_uses_prometheus",
			config: &measurement.Config{
				PrometheusFramework: promFramework,
				CloudProvider:       apiserverOnlyProvider,
				ClusterLoaderConfig: &clconfig.ClusterLoaderConfig{
					ClusterConfig:    clconfig.ClusterConfig{Provider: apiserverOnlyProvider},
					PrometheusConfig: clconfig.PrometheusConfig{EnableServer: true},
					OverridePaths:    []string{boolFalseOverride},
				},
			},
			want: true,
		},
		{
			name: "override_file_quoted_false_matches_go_template_truthiness",
			config: &measurement.Config{
				PrometheusFramework: promFramework,
				CloudProvider:       gceLike,
				ClusterLoaderConfig: &clconfig.ClusterLoaderConfig{
					ClusterConfig:    clconfig.ClusterConfig{Provider: gceLike},
					PrometheusConfig: clconfig.PrometheusConfig{EnableServer: true},
					OverridePaths:    []string{quotedFalseOverride},
				},
			},
			want: false,
		},
		{
			name: "gce_with_explicit_true_override_falls_back",
			config: &measurement.Config{
				PrometheusFramework: promFramework,
				CloudProvider:       gceLike,
				ClusterLoaderConfig: &clconfig.ClusterLoaderConfig{
					ClusterConfig: clconfig.ClusterConfig{Provider: gceLike},
					PrometheusConfig: clconfig.PrometheusConfig{
						EnableServer:        true,
						ScrapeApiserverOnly: "true",
					},
				},
			},
			want: false,
		},
		{
			name: "dns_master_endpoint_with_managed_cl2_prometheus_falls_back",
			config: &measurement.Config{
				PrometheusFramework: promFramework,
				CloudProvider:       gceLike,
				ClusterLoaderConfig: &clconfig.ClusterLoaderConfig{
					ClusterConfig: clconfig.ClusterConfig{
						Provider:          gceLike,
						MasterDNSEndpoint: "api.example.com",
					},
					PrometheusConfig: clconfig.PrometheusConfig{EnableServer: true},
				},
			},
			want: false,
		},
		{
			name: "dns_master_endpoint_with_existing_prometheus_uses_prometheus",
			config: &measurement.Config{
				PrometheusFramework: promFramework,
				CloudProvider:       gceLike,
				ClusterLoaderConfig: &clconfig.ClusterLoaderConfig{
					ClusterConfig: clconfig.ClusterConfig{
						Provider:          gceLike,
						MasterDNSEndpoint: "api.example.com",
					},
					PrometheusConfig: clconfig.PrometheusConfig{UseExistingServer: true},
				},
			},
			want: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, shouldUsePrometheusSchedulingMetrics(tc.config))
		})
	}
}

func TestTestMetricsSchedulingRouting(t *testing.T) {
	promFramework := &framework.Framework{}
	gceLike := &fakeProvider{features: provider.Features{ShouldPrometheusScrapeApiserverOnly: false}}
	clCfg := &clconfig.ClusterLoaderConfig{
		ClusterConfig:    clconfig.ClusterConfig{Provider: gceLike},
		PrometheusConfig: clconfig.PrometheusConfig{EnableServer: true},
	}

	t.Run("routes_to_prometheus_when_enabled", func(t *testing.T) {
		directSched := &fakeSubMeasurement{name: "SchedulingMetrics"}
		promSched := &fakeSubMeasurement{
			name:      "PrometheusSchedulingMetrics",
			summaries: []measurement.Summary{measurement.CreateSummary("SchedulingMetrics", "json", "{}")},
		}
		tm := newStubbedTestMetrics(directSched, promSched)

		startCfg := &measurement.Config{
			Identifier:          "TestMetrics",
			PrometheusFramework: promFramework,
			CloudProvider:       gceLike,
			ClusterLoaderConfig: clCfg,
			ClusterVersion:      version.Info{Major: "1", Minor: "37"},
			Params:              map[string]interface{}{"action": "start"},
		}
		_, err := tm.Execute(startCfg)
		require.NoError(t, err)
		require.Len(t, promSched.calls, 1)
		assert.Empty(t, directSched.calls)
		assert.Equal(t, "start", promSched.calls[0].Params["action"])
		assert.Equal(t, "SchedulingMetrics", promSched.calls[0].Params["summaryName"])
		assert.Equal(t, "TestMetrics", promSched.calls[0].Identifier)
		assert.Same(t, clCfg, promSched.calls[0].ClusterLoaderConfig)
		assert.Equal(t, "37", promSched.calls[0].ClusterVersion.Minor)

		gatherCfg := &measurement.Config{
			Identifier:          "TestMetrics",
			PrometheusFramework: promFramework,
			CloudProvider:       gceLike,
			ClusterLoaderConfig: clCfg,
			Params:              map[string]interface{}{"action": "gather"},
		}
		summaries, err := tm.Execute(gatherCfg)
		require.NoError(t, err)
		require.Len(t, promSched.calls, 2)
		assert.Empty(t, directSched.calls)
		assert.Equal(t, "gather", promSched.calls[1].Params["action"])
		assert.Equal(t, "SchedulingMetrics", promSched.calls[1].Params["summaryName"])
		require.Len(t, summaries, 1)
		assert.Equal(t, "SchedulingMetrics", summaries[0].SummaryName())

		tm.Dispose()
		assert.True(t, directSched.disposed)
		assert.True(t, promSched.disposed)
	})

	t.Run("falls_back_to_direct_scheduling_metrics_when_prometheus_disabled", func(t *testing.T) {
		directSched := &fakeSubMeasurement{
			name:      "SchedulingMetrics",
			summaries: []measurement.Summary{measurement.CreateSummary("SchedulingMetrics", "json", "{}")},
		}
		promSched := &fakeSubMeasurement{name: "PrometheusSchedulingMetrics"}
		tm := newStubbedTestMetrics(directSched, promSched)

		_, err := tm.Execute(&measurement.Config{
			Identifier:    "TestMetrics",
			CloudProvider: gceLike,
			Params:        map[string]interface{}{"action": "start"},
		})
		require.NoError(t, err)
		require.Len(t, directSched.calls, 1)
		assert.Empty(t, promSched.calls)
		assert.Equal(t, "start", directSched.calls[0].Params["action"])

		summaries, err := tm.Execute(&measurement.Config{
			Identifier:    "TestMetrics",
			CloudProvider: gceLike,
			Params:        map[string]interface{}{"action": "gather"},
		})
		require.NoError(t, err)
		require.Len(t, directSched.calls, 2)
		assert.Empty(t, promSched.calls)
		assert.Equal(t, "gather", directSched.calls[1].Params["action"])
		require.Len(t, summaries, 1)
		assert.Equal(t, "SchedulingMetrics", summaries[0].SummaryName())
	})

	t.Run("gather_without_prior_start_falls_back_to_direct_scrape_and_zero_dispose_is_safe", func(t *testing.T) {
		directSched := &fakeSubMeasurement{
			name:      "SchedulingMetrics",
			summaries: []measurement.Summary{measurement.CreateSummary("SchedulingMetrics", "json", "{}")},
		}
		promSched := &fakeSubMeasurement{name: "PrometheusSchedulingMetrics"}
		tm := newStubbedTestMetrics(directSched, promSched)

		summaries, err := tm.Execute(&measurement.Config{
			Identifier:          "TestMetrics",
			PrometheusFramework: promFramework,
			CloudProvider:       gceLike,
			ClusterLoaderConfig: clCfg,
			Params:              map[string]interface{}{"action": "gather"},
		})
		require.NoError(t, err)
		require.Len(t, directSched.calls, 1)
		assert.Empty(t, promSched.calls)
		require.Len(t, summaries, 1)

		assert.NotPanics(t, func() {
			(&testMetrics{}).Dispose()
		})
	})
}
