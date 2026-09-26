#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-vibe-5k}"
NUM_NODES="${NUM_NODES:-5000}"
NODE_PREFIX="${NODE_PREFIX:-vibe-node-}"
KIND_IMAGE="${KIND_IMAGE:-kindest/node:v1.36.1}"
SHM_DIR="${SHM_DIR:-/dev/shm/vibe-5k}"
ETCD_DATA_DIR="${ETCD_DATA_DIR:-/var/lib/vibe-5k}"
SPLIT_ETCD_LEASES="${SPLIT_ETCD_LEASES:-false}"
CUSTOM_APISERVER_BIN="${CUSTOM_APISERVER_BIN:-}"
CP_CPUS="${CP_CPUS:-0-95}"
CP_GOMAXPROCS="${CP_GOMAXPROCS:-96}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== [1/5] Preparing split persistent etcd directories (${ETCD_DATA_DIR}) and token directory (${SHM_DIR}) ==="
sudo mkdir -p "${ETCD_DATA_DIR}/etcd-main" "${ETCD_DATA_DIR}/etcd-events" "${ETCD_DATA_DIR}/etcd-leases"
sudo rm -rf "${ETCD_DATA_DIR}/etcd-main/"* "${ETCD_DATA_DIR}/etcd-events/"* "${ETCD_DATA_DIR}/etcd-leases/"*
sudo chmod -R 755 "${ETCD_DATA_DIR}"

rm -rf "${SHM_DIR}/tokens"
mkdir -p "${SHM_DIR}/tokens"

echo "=== [2/5] Generating ${NUM_NODES} per-node static tokens (system:node:<name> in system:nodes) ==="
python3 -c '
import sys
n = int(sys.argv[1])
prefix = sys.argv[2]
with open(sys.argv[3], "w") as f:
    for i in range(n):
        name = f"{prefix}{i:04d}"
        f.write(f"token-{name},system:node:{name},uid-{name},\"system:nodes\"\n")
' "${NUM_NODES}" "${NODE_PREFIX}" "${SHM_DIR}/tokens/known_tokens.csv"
chmod -R a+rX "${SHM_DIR}/tokens"


echo "=== [3/5] Creating 1-node kind control-plane (${CLUSTER_NAME}) with 5k scale flags ==="
if kind get clusters 2>/dev/null | grep -qx "${CLUSTER_NAME}"; then
  echo "Deleting existing kind cluster ${CLUSTER_NAME}..."
  kind delete cluster --name "${CLUSTER_NAME}"
fi

# Start kind create in background and inject etcd-events static pod as soon as kubelet starts
(
  for _ in $(seq 1 60); do
    if docker inspect "${CLUSTER_NAME}-control-plane" >/dev/null 2>&1; then
      ETCD_IMAGE=$(docker exec "${CLUSTER_NAME}-control-plane" sh -c "grep -m1 'image:.*etcd:' /etc/kubernetes/manifests/etcd.yaml 2>/dev/null | awk '{print \$2}'" || true)
      if [[ -n "${ETCD_IMAGE}" ]]; then
        echo "Injecting split etcd-events static pod (${ETCD_IMAGE} on http://127.0.0.1:2383)..."
        docker exec -i "${CLUSTER_NAME}-control-plane" sh -c "cat > /etc/kubernetes/manifests/etcd-events.yaml" <<EOF
apiVersion: v1
kind: Pod
metadata:
  labels:
    component: etcd-events
    tier: control-plane
  name: etcd-events
  namespace: kube-system
spec:
  hostNetwork: true
  priority: 2000001000
  priorityClassName: system-node-critical
  containers:
  - name: etcd-events
    image: ${ETCD_IMAGE}
    imagePullPolicy: IfNotPresent
    command:
    - etcd
    - --name=etcd-events
    - --data-dir=/var/lib/etcd-events
    - --listen-client-urls=http://127.0.0.1:2383
    - --advertise-client-urls=http://127.0.0.1:2383
    - --listen-peer-urls=http://127.0.0.1:2384
    - --initial-advertise-peer-urls=http://127.0.0.1:2384
    - --initial-cluster=etcd-events=http://127.0.0.1:2384
    - --listen-metrics-urls=http://0.0.0.0:2386
    - --quota-backend-bytes=21474836480
    - --snapshot-count=10000
    - --watch-progress-notify-interval=5s
    - --enable-pprof=true
    volumeMounts:
    - mountPath: /var/lib/etcd-events
      name: etcd-events-data
  volumes:
  - hostPath:
      path: /var/lib/etcd-events
      type: DirectoryOrCreate
    name: etcd-events-data
EOF
        break
      fi
    fi
    sleep 1
  done
) &
INJECT_PID=$!

kind create cluster \
  --name "${CLUSTER_NAME}" \
  --image "${KIND_IMAGE}" \
  --config "${SCRIPT_DIR}/kind-5k-scale.yaml"
wait "${INJECT_PID}" || true

echo "=== [3.5/5] Pinning control-plane container to CPUs ${CP_CPUS} and GOMAXPROCS=${CP_GOMAXPROCS} (matching c4-standard-96) ==="
docker update --cpuset-cpus="${CP_CPUS}" "${CLUSTER_NAME}-control-plane" || true
docker exec "${CLUSTER_NAME}-control-plane" sh -c "
for p in /etc/kubernetes/manifests/kube-apiserver.yaml /etc/kubernetes/manifests/kube-controller-manager.yaml /etc/kubernetes/manifests/kube-scheduler.yaml; do
  if ! grep -q GOMAXPROCS \"\$p\"; then
    sed -i 's/    image: /    env:\n    - name: GOMAXPROCS\n      value: \"${CP_GOMAXPROCS}\"\n    image: /' \"\$p\"
  fi
done
"

echo "=== [4/5] Configuring RBAC & pinning system DaemonSets to control-plane ==="
CTX="kind-${CLUSTER_NAME}"
until kubectl --context "${CTX}" get --raw=/readyz >/dev/null 2>&1; do
  sleep 1
done

# Extract admin client cert & key inside the control-plane container for localhost scheduler/KCM pprof & metrics scraping
docker exec "${CLUSTER_NAME}-control-plane" sh -c '
  awk "/client-certificate-data:/ {print \$2}" /etc/kubernetes/admin.conf | base64 -d > /tmp/admin.crt
  awk "/client-key-data:/ {print \$2}" /etc/kubernetes/admin.conf | base64 -d > /tmp/admin.key
'

# Pin kindnet and kube-proxy DaemonSets to the real control-plane node so they don't spawn 10,000 pods on simulated nodes
kubectl --context "${CTX}" -n kube-system patch daemonset kindnet \
  -p '{"spec":{"template":{"spec":{"nodeSelector":{"node-role.kubernetes.io/control-plane":""}}}}}'
kubectl --context "${CTX}" -n kube-system patch daemonset kube-proxy \
  -p '{"spec":{"template":{"spec":{"nodeSelector":{"node-role.kubernetes.io/control-plane":""}}}}}'

# Taint the control-plane node NoSchedule so 100% of ClusterLoader2 workload pods schedule onto simulated nodes
kubectl --context "${CTX}" taint nodes "${CLUSTER_NAME}-control-plane" \
  node-role.kubernetes.io/control-plane:NoSchedule --overwrite

# Allow system:nodes to watch EndpointSlices, ServiceCIDRs, Services, ConfigMaps (kube-root-ca.crt), CSINodes, CSIDrivers, RuntimeClasses
kubectl --context "${CTX}" create clusterrolebinding vibe-node-proxier \
  --clusterrole=system:node-proxier --group=system:nodes --dry-run=client -o yaml | kubectl --context "${CTX}" apply -f -

kubectl --context "${CTX}" apply -f - <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: vibe-node-extra-watches
rules:
- apiGroups: [""]
  resources: ["configmaps", "services", "nodes", "events"]
  verbs: ["get", "list", "watch", "create", "patch"]
- apiGroups: ["storage.k8s.io"]
  resources: ["csidrivers", "csinodes"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["node.k8s.io"]
  resources: ["runtimeclasses"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["discovery.k8s.io"]
  resources: ["endpointslices"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["networking.k8s.io"]
  resources: ["servicecidrs"]
  verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: vibe-node-extra-watches
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: vibe-node-extra-watches
subjects:
- apiGroup: rbac.authorization.k8s.io
  kind: Group
  name: system:nodes
EOF

# Optional: hot-swap custom kube-apiserver binary if requested
if [[ -n "${CUSTOM_APISERVER_BIN}" && -f "${CUSTOM_APISERVER_BIN}" ]]; then
  echo "=== Hot-swapping custom kube-apiserver binary: ${CUSTOM_APISERVER_BIN} ==="
  docker cp "${CUSTOM_APISERVER_BIN}" "${CLUSTER_NAME}-control-plane:/usr/local/bin/kube-apiserver-custom"
  docker exec "${CLUSTER_NAME}-control-plane" sh -c '
    chmod +x /usr/local/bin/kube-apiserver-custom
    cp /etc/kubernetes/manifests/kube-apiserver.yaml /tmp/kube-apiserver.yaml.bak
    python3 -c "
import sys
p = \"/etc/kubernetes/manifests/kube-apiserver.yaml\"
with open(p) as f: s = f.read()
s = s.replace(\"- kube-apiserver\n\", \"- /usr/local/bin/kube-apiserver-custom\n\", 1)
s = s.replace(\"  volumes:\n\", \"    - mountPath: /usr/local/bin/kube-apiserver-custom\n      name: custom-apiserver\n      readOnly: true\n  volumes:\n  - hostPath:\n      path: /usr/local/bin/kube-apiserver-custom\n      type: File\n    name: custom-apiserver\n\", 1)
with open(p, \"w\") as f: f.write(s)
"
  '
fi

echo "=== [5/5] Control plane ready! ==="
kind get kubeconfig --name "${CLUSTER_NAME}" > "${SHM_DIR}/kubeconfig"
chmod 644 "${SHM_DIR}/kubeconfig"
CP_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${CLUSTER_NAME}-control-plane")
echo "  Context:           ${CTX}"
echo "  Kubeconfig:        ${SHM_DIR}/kubeconfig"
echo "  Control Plane IP:  https://${CP_IP}:6443 (direct bridge IP, bypasses docker-proxy)"
echo "  Tokens File:       ${SHM_DIR}/tokens/known_tokens.csv"
echo "  Etcd Main Dir:     ${ETCD_DATA_DIR}/etcd-main"
echo "  Etcd Events Dir:   ${ETCD_DATA_DIR}/etcd-events"
