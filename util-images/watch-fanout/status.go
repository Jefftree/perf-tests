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
	"encoding/json"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// Node and pod status writes.
//
// The rig previously simulated only Lease renewals, which are the smallest and
// most uniform write a kubelet makes. The two sources left out are the ones
// that actually shape the write mix on a real cluster:
//
//   - Node status: ~5-10 KB per write (capacity, allocatable, conditions and
//     the image list) but only every NodeStatusReportFrequency, default 5m
//     (pkg/kubelet/apis/config/v1beta1/defaults.go:141). At 5,000 nodes that
//     is ~17 writes/s of large objects.
//   - Pod status: small, but one per pod transition, so its rate is set by
//     churn rather than by node count. Every one of them lands in the pods
//     watch cache, which is the widest fan-out in the cluster.
//
// Both are driven from the kubelet identities, so they are subject to APF's
// node-high priority level exactly as the leases are.

// marshalStatusPatch encodes a strategic merge patch body. Kubelet patches the
// status subresource rather than updating the whole object, so the write size
// and conflict behaviour only match if the rig does the same.
func marshalStatusPatch(body map[string]any) ([]byte, error) {
	return json.Marshal(body)
}

// nodeImages returns a plausible image list. This is not padding: the image
// list is the reason a real Node status is kilobytes rather than bytes, and
// the write size is what makes node status a different load from a lease.
func nodeImages() []corev1.ContainerImage {
	names := []string{
		"registry.k8s.io/kube-proxy", "registry.k8s.io/pause",
		"registry.k8s.io/coredns/coredns", "registry.k8s.io/etcd",
		"registry.k8s.io/kube-apiserver", "registry.k8s.io/kube-scheduler",
		"registry.k8s.io/kube-controller-manager", "registry.k8s.io/metrics-server/metrics-server",
		"registry.k8s.io/e2e-test-images/agnhost", "registry.k8s.io/sig-storage/csi-provisioner",
		"registry.k8s.io/sig-storage/csi-attacher", "registry.k8s.io/sig-storage/csi-resizer",
		"registry.k8s.io/sig-storage/csi-snapshotter", "registry.k8s.io/sig-storage/livenessprobe",
		"registry.k8s.io/sig-storage/csi-node-driver-registrar", "registry.k8s.io/ingress-nginx/controller",
		"docker.io/library/nginx", "docker.io/library/redis",
		"docker.io/library/postgres", "docker.io/library/busybox",
	}
	out := make([]corev1.ContainerImage, 0, len(names))
	for i, n := range names {
		out = append(out, corev1.ContainerImage{
			Names:     []string{fmt.Sprintf("%s:v1.%d.0", n, i), fmt.Sprintf("%s@sha256:%064x", n, i)},
			SizeBytes: int64(20_000_000 + i*3_000_000),
		})
	}
	return out
}

func nodeConditions(t metav1.Time) []corev1.NodeCondition {
	cond := func(typ corev1.NodeConditionType, status corev1.ConditionStatus, reason, msg string) corev1.NodeCondition {
		return corev1.NodeCondition{
			Type: typ, Status: status, Reason: reason, Message: msg,
			LastHeartbeatTime: t, LastTransitionTime: t,
		}
	}
	return []corev1.NodeCondition{
		cond(corev1.NodeMemoryPressure, corev1.ConditionFalse, "KubeletHasSufficientMemory", "kubelet has sufficient memory available"),
		cond(corev1.NodeDiskPressure, corev1.ConditionFalse, "KubeletHasNoDiskPressure", "kubelet has no disk pressure"),
		cond(corev1.NodePIDPressure, corev1.ConditionFalse, "KubeletHasSufficientPID", "kubelet has sufficient PID available"),
		cond(corev1.NodeReady, corev1.ConditionTrue, "KubeletReady", "kubelet is posting ready status"),
	}
}

// ensureNode registers the simulated kubelet's Node object, the way a real
// kubelet does at startup. Pods can only carry a spec.nodeName that resolves,
// and node status writes need a target.
func ensureNode(ctx context.Context, c kubernetes.Interface, name string) error {
	now := metav1.NewTime(time.Now())
	capacity := corev1.ResourceList{
		corev1.ResourceCPU:              resource.MustParse("8"),
		corev1.ResourceMemory:           resource.MustParse("32Gi"),
		corev1.ResourcePods:             resource.MustParse("110"),
		corev1.ResourceEphemeralStorage: resource.MustParse("100Gi"),
	}
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"kubernetes.io/hostname":                   name,
				"kubernetes.io/os":                         "linux",
				"kubernetes.io/arch":                       "amd64",
				"node.kubernetes.io/instance-type":         "n1-standard-8",
				"topology.kubernetes.io/region":            "us-east1",
				"topology.kubernetes.io/zone":              "us-east1-b",
				"failure-domain.beta.kubernetes.io/region": "us-east1",
				"failure-domain.beta.kubernetes.io/zone":   "us-east1-b",
			},
		},
		Status: corev1.NodeStatus{
			Capacity:    capacity,
			Allocatable: capacity,
			Conditions:  nodeConditions(now),
			Images:      nodeImages(),
			NodeInfo: corev1.NodeSystemInfo{
				MachineID: fmt.Sprintf("%032x", 1), SystemUUID: fmt.Sprintf("%032x", 2),
				BootID: fmt.Sprintf("%032x", 3), KernelVersion: "6.1.0",
				OSImage: "Container-Optimized OS", ContainerRuntimeVersion: "containerd://1.7.0",
				KubeletVersion: "v1.37.0", KubeProxyVersion: "v1.37.0",
				OperatingSystem: "linux", Architecture: "amd64",
			},
			Addresses: []corev1.NodeAddress{
				{Type: corev1.NodeInternalIP, Address: "10.0.0.1"},
				{Type: corev1.NodeHostName, Address: name},
			},
		},
	}
	_, err := c.CoreV1().Nodes().Create(ctx, n, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

// patchNodeStatus refreshes the condition heartbeats, which is what a real
// kubelet's periodic status post amounts to when nothing has changed.
func patchNodeStatus(ctx context.Context, c kubernetes.Interface, name string) error {
	now := metav1.NewTime(time.Now())
	patch, err := marshalStatusPatch(map[string]any{
		"status": map[string]any{"conditions": nodeConditions(now)},
	})
	if err != nil {
		return err
	}
	_, err = c.CoreV1().Nodes().Patch(ctx, name, types.StrategicMergePatchType, patch, metav1.PatchOptions{}, "status")
	return err
}

// patchPodStatus refreshes one pod's Ready condition, the shape of the status
// write a kubelet issues on a probe transition.
func patchPodStatus(ctx context.Context, c kubernetes.Interface, ns, name string) error {
	now := metav1.NewTime(time.Now())
	patch, err := marshalStatusPatch(map[string]any{
		"status": map[string]any{
			"phase": string(corev1.PodRunning),
			"conditions": []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastProbeTime: now, LastTransitionTime: now},
				{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastProbeTime: now, LastTransitionTime: now},
			},
		},
	})
	if err != nil {
		return err
	}
	_, err = c.CoreV1().Pods(ns).Patch(ctx, name, types.StrategicMergePatchType, patch, metav1.PatchOptions{}, "status")
	return err
}

// ownedPod maps a kubelet to the pods bound to it. Ownership is derived rather
// than discovered on purpose: a LIST per kubelet at 5,000 kubelets would be a
// large load of its own and would contaminate the measurement.
//
// pods assigns spec.nodeName as i%nodes, so kubelet j owns pods
// j, j+nodes, j+2*nodes, ... and the namespace follows the same i%namespaces
// rule the pods command uses.
func ownedPod(index, namespaces int) (ns, name string) {
	return fmt.Sprintf("%s-%03d", podNamespacePrefix, index%namespaces),
		fmt.Sprintf("wf-pod-%07d", index)
}
