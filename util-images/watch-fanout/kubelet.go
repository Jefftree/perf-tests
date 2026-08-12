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
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	coordv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

// Kubelet-shaped request load, so that APF is actually exercised.
//
// The reported production symptom is APF rejection, not a LIST SLO breach, and
// this rig could never have shown it: every request was authenticating as
// system:masters, which is EXEMPT from flow control. All traffic landed in the
// exempt priority level and no other level ever saw a seat.
//
// Each simulated kubelet authenticates as system:node:<name> in group
// system:nodes, which is what steers it to the node-high priority level, and
// renews its own Lease the way a real kubelet does (default every 10s, see
// NodeLeaseDurationSeconds / the renew interval of 10s in
// pkg/kubelet/kubelet.go). 5,000 nodes at 10s is ~500 lease writes/s.
const nodeLeaseNamespace = "kube-node-lease"

func nodeUser(i int) string  { return fmt.Sprintf("system:node:wf-node-%05d", i) }
func nodeToken(i int) string { return fmt.Sprintf("wfnode%05dxxxxxxxxxxxxxxxxxxxx", i) }

// writeNodeTokens emits a token-auth-file granting each simulated kubelet its
// own identity in system:nodes, plus the admin token the rest of the rig uses.
// Distinct users matter: APF's flow distinguisher shards queues by user, so a
// single shared identity would collapse 5,000 kubelets into one queue and
// misrepresent both fairness and rejection behaviour.
func writeNodeTokens(path, adminToken string, nodes int) error {
	var b strings.Builder
	fmt.Fprintf(&b, "%s,admin,admin,\"system:masters\"\n", adminToken)
	for i := 0; i < nodes; i++ {
		fmt.Fprintf(&b, "%s,%s,uid-node-%05d,\"system:nodes\"\n", nodeToken(i), nodeUser(i), i)
	}
	return os.WriteFile(path, []byte(b.String()), 0600)
}

func runKubelet(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("kubelet", flag.ExitOnError)
	klog.InitFlags(fs)
	var (
		kubeconfig  = fs.String("kubeconfig", "", "kubeconfig, used only for the server URL and CA")
		nodes       = fs.Int("nodes", 5000, "simulated kubelets")
		renew       = fs.Duration("renew", 10*time.Second, "lease renewal interval, kubelet default 10s")
		rampDelay   = fs.Duration("ramp-delay", 1*time.Millisecond, "stagger between kubelet starts")
		metricsAddr = fs.String("metrics-addr", ":9115", "address for /metrics, empty to disable")
		writeTokens = fs.String("write-tokens", "", "only write a token-auth-file to this path and exit")
		adminToken  = fs.String("admin-token", "", "admin token to preserve when writing tokens")
		qps         = fs.Float64("qps", 20, "per-kubelet QPS")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *writeTokens != "" {
		if err := writeNodeTokens(*writeTokens, *adminToken, *nodes); err != nil {
			return err
		}
		klog.Infof("wrote %d node identities to %s", *nodes, *writeTokens)
		return nil
	}

	base, err := baseConfig(*kubeconfig, "", float32(*qps))
	if err != nil {
		return err
	}
	serveMetrics(ctx, *metricsAddr)
	klog.Infof("starting %d simulated kubelets, lease renewal every %s", *nodes, *renew)
	kubeletCount.Set(float64(*nodes))

	var wg sync.WaitGroup
	for i := 0; i < *nodes; i++ {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(*rampDelay):
		}
		cfg := perClientConfig(base)
		// Drop the admin credential and authenticate as this node, so the
		// request matches the system:nodes flow schema instead of exempt.
		cfg.BearerToken = nodeToken(i)
		cfg.BearerTokenFile = ""
		wg.Add(1)
		go func(id int, cfg *rest.Config) {
			defer wg.Done()
			runOneKubelet(ctx, id, cfg, *renew)
		}(i, cfg)
	}
	klog.Infof("all %d kubelets started", *nodes)
	wg.Wait()
	return ctx.Err()
}

func runOneKubelet(ctx context.Context, id int, cfg *rest.Config, renew time.Duration) {
	client, err := newClient(cfg)
	if err != nil {
		klog.Errorf("kubelet %d: %v", id, err)
		return
	}
	name := fmt.Sprintf("wf-node-%05d", id)
	if err := ensureLease(ctx, client, name); err != nil && ctx.Err() == nil {
		klog.V(3).Infof("kubelet %d ensure lease: %v", id, err)
	}
	t := time.NewTicker(renew)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			start := time.Now()
			err := renewLease(ctx, client, name)
			leaseLatency.Observe(time.Since(start).Seconds())
			switch {
			case err == nil:
				leaseWrites.WithLabelValues("ok").Inc()
			case apierrors.IsTooManyRequests(err):
				// This is the APF rejection being hunted: a 429 with
				// "request is being rejected" from flow control.
				leaseWrites.WithLabelValues("throttled").Inc()
			default:
				leaseWrites.WithLabelValues("error").Inc()
				klog.V(4).Infof("kubelet %d renew: %v", id, err)
			}
		}
	}
}

func ensureLease(ctx context.Context, c kubernetes.Interface, name string) error {
	now := metav1.NewMicroTime(time.Now())
	l := &coordv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: nodeLeaseNamespace},
		Spec: coordv1.LeaseSpec{
			HolderIdentity:       &name,
			LeaseDurationSeconds: ptrInt32(40),
			RenewTime:            &now,
		},
	}
	_, err := c.CoordinationV1().Leases(nodeLeaseNamespace).Create(ctx, l, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func renewLease(ctx context.Context, c kubernetes.Interface, name string) error {
	l, err := c.CoordinationV1().Leases(nodeLeaseNamespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	now := metav1.NewMicroTime(time.Now())
	l.Spec.RenewTime = &now
	_, err = c.CoordinationV1().Leases(nodeLeaseNamespace).Update(ctx, l, metav1.UpdateOptions{})
	return err
}

func ptrInt32(v int32) *int32 { return &v }
