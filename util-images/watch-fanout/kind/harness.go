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

// Package kindharness runs the watch-fanout load against a kind cluster.
//
// This replaces hack/local-rig.sh as the reproducible entry point. The bash rig
// hand-rolls etcd and apiserver bring-up, which is fine on a workstation and
// bad as something CI consumes: it reimplements cluster bring-up, and every
// A/B I drove through it needed its own throwaway orchestration script. Here
// the cluster comes from kind and everything else is Go, so `go test` is the
// whole interface.
//
// A caveat to keep in view when reading results: this workload is bistable.
// On a dedicated 16-core workstation, two runs of an identical configuration
// produced mean delivery latencies of 15.7s and 32.5s. Nesting it inside kind's
// containers on a shared CI node does not improve that. Gate on the metrics
// that were categorically stable across a crossover (terminated watchers,
// delivery completeness, events/s, CPU per delivered byte) and treat latency
// percentiles as descriptive only.
package kindharness

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	// ClusterName is deliberately specific: a developer box commonly has other
	// kind clusters running and this must never touch them.
	ClusterName = "watch-fanout"
	Namespace   = "watch-fanout"
	Image       = "localhost/watch-fanout:latest"
	serviceAcct = "watch-fanout"
)

// Role is one load-generating Deployment.
type Role struct {
	Name     string
	Args     []string
	Replicas int32
	// MetricsPort is the port the role serves /metrics on. Roles run with
	// hostNetwork, so this is also the port on the kind node, which
	// extraPortMappings forwards to the host.
	MetricsPort int32
}

// Options configures a run. Defaults are CI-sized, not 5k-sized: the only
// regime that reproduced itself within noise on a 16-core box was ~1,200
// watchers at ~3 apiserver cores, which is what fits a 6-CPU CI container.
type Options struct {
	Watchers      int
	DecodeProbes  int
	Kubelets      int
	Pods          int
	PodNamespaces int
	EPSRate       float64
	SVCRate       float64
}

func DefaultOptions() Options {
	return Options{
		Watchers:      1000,
		DecodeProbes:  200,
		Kubelets:      500,
		Pods:          20000,
		PodNamespaces: 50,
		EPSRate:       33,
		SVCRate:       2.6,
	}
}

// Env reads an int from the environment, so CI can scale a run without a code
// change and a workstation can push the same harness to 5k shapes.
func Env(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func run(ctx context.Context, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	if err != nil {
		return out.String(), fmt.Errorf("%s %s: %w\n%s", name, strings.Join(args, " "), err, out.String())
	}
	return out.String(), nil
}

// ClusterExists reports whether the watch-fanout kind cluster is already up, so
// repeated runs reuse it instead of paying bring-up each time.
func ClusterExists(ctx context.Context) (bool, error) {
	out, err := run(ctx, "kind", "get", "clusters")
	if err != nil {
		return false, err
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) == ClusterName {
			return true, nil
		}
	}
	return false, nil
}

// CreateCluster writes the node token file and brings up the kind cluster.
//
// The token file has to exist before the cluster is created: the apiserver is
// started by kubeadm with --token-auth-file pointing into a hostPath mount, and
// a missing file makes the apiserver fail to start rather than fall back.
func CreateCluster(ctx context.Context, srcDir string, kubelets int) error {
	authDir, err := os.MkdirTemp("", "wf-auth-")
	if err != nil {
		return err
	}
	if err := os.Chmod(authDir, 0755); err != nil {
		return err
	}
	tokens := filepath.Join(authDir, "tokens.csv")
	if _, err := run(ctx, "go", "run", srcDir, "kubelet",
		"--write-tokens="+tokens, "--admin-token="+AdminToken, "--nodes="+strconv.Itoa(kubelets)); err != nil {
		return fmt.Errorf("write tokens: %w", err)
	}
	if err := os.Chmod(tokens, 0644); err != nil {
		return err
	}

	tmpl, err := os.ReadFile(filepath.Join(srcDir, "kind", "cluster.yaml"))
	if err != nil {
		return err
	}
	cfg := filepath.Join(authDir, "cluster.yaml")
	if err := os.WriteFile(cfg, bytes.ReplaceAll(tmpl, []byte("HOSTAUTHDIR"), []byte(authDir)), 0644); err != nil {
		return err
	}
	_, err = run(ctx, "kind", "create", "cluster", "--config", cfg, "--wait", "180s")
	return err
}

// AdminToken is the static admin credential the non-kubelet roles use. It is a
// test cluster with a random name and no exposed apiserver port beyond
// localhost, so a fixed token is not a meaningful exposure.
const AdminToken = "watchfanout0000000000000000000000"

func DeleteCluster(ctx context.Context) error {
	_, err := run(ctx, "kind", "delete", "cluster", "--name", ClusterName)
	return err
}

// BuildAndLoadImage builds the load generator and side-loads it into the kind
// node. No registry involved, so this works on a disconnected machine.
func BuildAndLoadImage(ctx context.Context, srcDir string) error {
	if _, err := run(ctx, "docker", "build", "-t", Image, srcDir); err != nil {
		return err
	}
	_, err := run(ctx, "kind", "load", "docker-image", Image, "--name", ClusterName)
	return err
}

// Client returns a client for the kind cluster.
func Client(ctx context.Context) (*kubernetes.Clientset, error) {
	out, err := run(ctx, "kind", "get", "kubeconfig", "--name", ClusterName)
	if err != nil {
		return nil, err
	}
	cfg, err := clientcmd.RESTConfigFromKubeConfig([]byte(out))
	if err != nil {
		return nil, err
	}
	cfg.QPS, cfg.Burst = 200, 400
	return kubernetes.NewForConfig(cfg)
}

// Setup creates the namespace and the cluster-admin binding the load roles need.
func Setup(ctx context.Context, c kubernetes.Interface) error {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: Namespace}}
	if _, err := c.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: serviceAcct, Namespace: Namespace}}
	if _, err := c.CoreV1().ServiceAccounts(Namespace).Create(ctx, sa, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	// cluster-admin: the roles create namespaces, services, endpointslices,
	// pods and nodes across the cluster. Narrowing this would be busywork on a
	// throwaway benchmark cluster.
	crb := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "watch-fanout-admin"},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "cluster-admin"},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: serviceAcct, Namespace: Namespace}},
	}
	if _, err := c.RbacV1().ClusterRoleBindings().Create(ctx, crb, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

// APIServerEnv points the in-cluster client straight at the apiserver on the
// node rather than at the kubernetes ClusterIP.
//
// in-cluster config reads these two variables, and the default 10.96.0.1:443
// is a virtual address serviced by kube-proxy's iptables. kube-proxy is
// programming 8,100 preloaded Services and re-syncing on every endpointslice
// write the rig makes, so routing the rig's own control path through it makes
// the load generator depend on the dataplane it is stressing: a pods job took
// an i/o timeout dialling 10.96.0.1:443 while the previous run's load was
// live. With hostNetwork the node IP is directly reachable.
func APIServerEnv(ctx context.Context, c kubernetes.Interface) ([]corev1.EnvVar, error) {
	nodes, err := c.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: "node-role.kubernetes.io/control-plane",
	})
	if err != nil || len(nodes.Items) == 0 {
		return nil, fmt.Errorf("find control-plane node: %w", err)
	}
	var ip string
	for _, a := range nodes.Items[0].Status.Addresses {
		if a.Type == corev1.NodeInternalIP {
			ip = a.Address
			break
		}
	}
	if ip == "" {
		return nil, fmt.Errorf("control-plane node has no InternalIP")
	}
	return []corev1.EnvVar{
		{Name: "KUBERNETES_SERVICE_HOST", Value: ip},
		{Name: "KUBERNETES_SERVICE_PORT", Value: "6443"},
	}, nil
}

// TeardownRoles removes any load Deployments left by an earlier run. Reusing a
// cluster is the point of WF_KEEP, but leaving the previous run's load running
// means the next run's population jobs compete with full fan-out, which is both
// slow and a different experiment.
func TeardownRoles(ctx context.Context, c kubernetes.Interface, names ...string) error {
	for _, n := range names {
		if err := c.AppsV1().Deployments(Namespace).Delete(ctx, n, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	// Wait for the pods to actually go away; a Deployment delete returns before
	// its watchers have disconnected.
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		pods, err := c.CoreV1().Pods(Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return err
		}
		running := 0
		for _, p := range pods.Items {
			if p.DeletionTimestamp == nil && p.Status.Phase == corev1.PodRunning {
				running++
			}
		}
		if running == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return nil
}

// RunJob runs a one-shot role (preload, pods) to completion.
func RunJob(ctx context.Context, c kubernetes.Interface, name string, args []string, timeout time.Duration) error {
	env, err := APIServerEnv(ctx, c)
	if err != nil {
		return err
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: Namespace},
		Spec: corev1.PodSpec{
			ServiceAccountName: serviceAcct,
			RestartPolicy:      corev1.RestartPolicyNever,
			Tolerations:        controlPlaneTolerations(),
			HostNetwork:        true,
			DNSPolicy:          corev1.DNSClusterFirstWithHostNet,
			Containers: []corev1.Container{{
				Name: name, Image: Image, ImagePullPolicy: corev1.PullNever, Args: args, Env: env,
			}},
		},
	}
	_ = c.CoreV1().Pods(Namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if _, err := c.CoreV1().Pods(Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return err
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		p, err := c.CoreV1().Pods(Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		switch p.Status.Phase {
		case corev1.PodSucceeded:
			return nil
		case corev1.PodFailed:
			return fmt.Errorf("%s failed: %s", name, PodLogs(ctx, c, name))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return fmt.Errorf("%s did not finish within %s: %s", name, timeout, PodLogs(ctx, c, name))
}

// StartRole creates a long-running load Deployment and, if asked, a NodePort
// Service for its /metrics.
func StartRole(ctx context.Context, c kubernetes.Interface, r Role) error {
	env, err := APIServerEnv(ctx, c)
	if err != nil {
		return err
	}
	replicas := r.Replicas
	if replicas == 0 {
		replicas = 1
	}
	labels := map[string]string{"app": r.Name}
	var ports []corev1.ContainerPort
	if r.MetricsPort > 0 {
		ports = []corev1.ContainerPort{{Name: "metrics", ContainerPort: r.MetricsPort}}
	}
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: r.Name, Namespace: Namespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					ServiceAccountName: serviceAcct,
					Tolerations:        controlPlaneTolerations(),
					// hostNetwork for two reasons. It puts /metrics directly on
					// the node so scraping needs no Service, and scraping
					// through a NodePort would route the measurement through
					// kube-proxy and the endpointslice controller, which are
					// part of the workload being measured. It also removes the
					// CNI hop from the load path.
					HostNetwork: true,
					DNSPolicy:   corev1.DNSClusterFirstWithHostNet,
					Containers: []corev1.Container{{
						Name: r.Name, Image: Image, ImagePullPolicy: corev1.PullNever,
						Args: r.Args, Ports: ports, Env: env,
						// No CPU limit on purpose: a throttled load generator
						// measures the throttle, not the apiserver.
					}},
				},
			},
		},
	}
	_ = c.AppsV1().Deployments(Namespace).Delete(ctx, r.Name, metav1.DeleteOptions{})
	if _, err := c.AppsV1().Deployments(Namespace).Create(ctx, dep, metav1.CreateOptions{}); err != nil {
		return err
	}
	return nil
}

// controlPlaneTolerations lets load pods run on the single control-plane node.
// kind untaints single-node clusters, but tolerating it costs nothing and keeps
// the harness working if a worker-node topology is added later.
func controlPlaneTolerations() []corev1.Toleration {
	return []corev1.Toleration{
		{Key: "node-role.kubernetes.io/control-plane", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
		{Key: "node-role.kubernetes.io/master", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
	}
}

// WaitReady blocks until every named Deployment has all replicas available.
func WaitReady(ctx context.Context, c kubernetes.Interface, timeout time.Duration, names ...string) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allReady := true
		for _, n := range names {
			d, err := c.AppsV1().Deployments(Namespace).Get(ctx, n, metav1.GetOptions{})
			if err != nil || d.Status.ReadyReplicas < *d.Spec.Replicas {
				allReady = false
				break
			}
		}
		if allReady {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
	return fmt.Errorf("deployments %v not ready within %s", names, timeout)
}

// PodLogs is best-effort, for failure messages.
func PodLogs(ctx context.Context, c kubernetes.Interface, name string) string {
	req := c.CoreV1().Pods(Namespace).GetLogs(name, &corev1.PodLogOptions{TailLines: ptr64(40)})
	rc, err := req.Stream(ctx)
	if err != nil {
		return "(no logs: " + err.Error() + ")"
	}
	defer rc.Close()
	b, _ := io.ReadAll(rc)
	return string(b)
}

func ptr64(v int64) *int64 { return &v }

// Scrape fetches a role's /metrics through its NodePort on the host.
func Scrape(ctx context.Context, nodePort int32) (string, error) {
	url := fmt.Sprintf("http://127.0.0.1:%d/metrics", nodePort)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return string(b), err
}

// ScrapeAPIServer reads the apiserver's own /metrics with the admin token.
func ScrapeAPIServer(ctx context.Context, c kubernetes.Interface) (string, error) {
	b, err := c.CoreV1().RESTClient().Get().AbsPath("/metrics").DoRaw(ctx)
	return string(b), err
}
