#!/usr/bin/env bash
# Copyright The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Starts etcd + kube-apiserver as plain local processes. No docker, no sudo, no
# kubelet, no controller-manager -- the rig only needs the watch cache and the
# serve path.

set -euo pipefail

K8S_ROOT="${K8S_ROOT:-$HOME/workspace/kubernetes}"
RUN_DIR="${RUN_DIR:-/tmp/watch-fanout}"
SECURE_PORT="${SECURE_PORT:-6443}"
ETCD_PORT="${ETCD_PORT:-2379}"
# Match the production control plane so GOMAXPROCS-sensitive dispatch behavior
# transfers; override downward for a laptop.
APISERVER_GOMAXPROCS="${APISERVER_GOMAXPROCS:-0}"

ETCD_BIN="${ETCD_BIN:-$K8S_ROOT/third_party/etcd/etcd}"
APISERVER_BIN="${APISERVER_BIN:-$K8S_ROOT/_output/bin/kube-apiserver}"

for bin in "$ETCD_BIN" "$APISERVER_BIN"; do
  if [[ ! -x "$bin" ]]; then
    echo "missing binary: $bin" >&2
    echo "build with: cd $K8S_ROOT && go build -o _output/bin/kube-apiserver ./cmd/kube-apiserver" >&2
    exit 1
  fi
done

mkdir -p "$RUN_DIR"/{etcd,certs,logs}

cleanup() {
  local code=$?
  [[ -n "${APISERVER_PID:-}" ]] && kill "$APISERVER_PID" 2>/dev/null || true
  [[ -n "${ETCD_PID:-}" ]] && kill "$ETCD_PID" 2>/dev/null || true
  exit $code
}
trap cleanup EXIT INT TERM

echo "starting etcd on 127.0.0.1:$ETCD_PORT"
"$ETCD_BIN" \
  --data-dir "$RUN_DIR/etcd" \
  --listen-client-urls "http://127.0.0.1:$ETCD_PORT" \
  --advertise-client-urls "http://127.0.0.1:$ETCD_PORT" \
  --listen-peer-urls "http://127.0.0.1:2380" \
  --initial-advertise-peer-urls "http://127.0.0.1:2380" \
  --initial-cluster "default=http://127.0.0.1:2380" \
  --quota-backend-bytes 8589934592 \
  --log-level warn \
  >"$RUN_DIR/logs/etcd.log" 2>&1 &
ETCD_PID=$!

for _ in $(seq 1 60); do
  if "$K8S_ROOT/third_party/etcd/etcdctl" --endpoints "http://127.0.0.1:$ETCD_PORT" endpoint health >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done
echo "etcd up (pid $ETCD_PID)"

# Service account signing key: required by the apiserver even though the rig
# never mints a service account token.
if [[ ! -f "$RUN_DIR/certs/sa.key" ]]; then
  openssl genrsa -out "$RUN_DIR/certs/sa.key" 2048 2>/dev/null
fi

# Static bearer token. Anonymous auth is not enough on its own -- unauthenticated
# requests still come back 401 -- so give every client one real identity.
TOKEN="${TOKEN:-watchfanout0000000000000000000000}"
printf '%s,admin,admin,"system:masters"\n' "$TOKEN" >"$RUN_DIR/certs/tokens.csv"

cat >"$RUN_DIR/kubeconfig" <<EOF
apiVersion: v1
kind: Config
clusters:
- name: local
  cluster:
    server: https://127.0.0.1:$SECURE_PORT
    insecure-skip-tls-verify: true
contexts:
- name: local
  context: {cluster: local, user: admin}
current-context: local
users:
- name: admin
  user:
    token: $TOKEN
EOF

echo "starting kube-apiserver on https://127.0.0.1:$SECURE_PORT"
GOMAXPROCS="$APISERVER_GOMAXPROCS" "$APISERVER_BIN" \
  --etcd-servers "http://127.0.0.1:$ETCD_PORT" \
  --bind-address 127.0.0.1 \
  --secure-port "$SECURE_PORT" \
  --service-cluster-ip-range 10.96.0.0/12 \
  --cert-dir "$RUN_DIR/certs" \
  --service-account-key-file "$RUN_DIR/certs/sa.key" \
  --service-account-signing-key-file "$RUN_DIR/certs/sa.key" \
  --service-account-issuer "https://kubernetes.default.svc.cluster.local" \
  --authorization-mode AlwaysAllow \
  --token-auth-file "$RUN_DIR/certs/tokens.csv" \
  --disable-admission-plugins ServiceAccount \
  --max-requests-inflight 3000 \
  --max-mutating-requests-inflight 1000 \
  --profiling=true \
  --v "${APISERVER_V:-2}" \
  >"$RUN_DIR/logs/apiserver.log" 2>&1 &
APISERVER_PID=$!

for _ in $(seq 1 120); do
  if curl -sk -H "Authorization: Bearer $TOKEN" "https://127.0.0.1:$SECURE_PORT/readyz" 2>/dev/null | grep -q ok; then
    break
  fi
  sleep 0.5
done

if ! curl -sk -H "Authorization: Bearer $TOKEN" "https://127.0.0.1:$SECURE_PORT/readyz" 2>/dev/null | grep -q ok; then
  echo "apiserver did not become ready; see $RUN_DIR/logs/apiserver.log" >&2
  tail -30 "$RUN_DIR/logs/apiserver.log" >&2
  exit 1
fi

echo "apiserver ready (pid $APISERVER_PID)"
echo
echo "  kubeconfig: $RUN_DIR/kubeconfig"
echo "  metrics:    curl -sk -H \"Authorization: Bearer $TOKEN\" https://127.0.0.1:$SECURE_PORT/metrics"
echo "  logs:       $RUN_DIR/logs/"
echo
echo "ctrl-c to tear down"
wait
