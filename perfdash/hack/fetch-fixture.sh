#!/usr/bin/env bash
# Downloads a small fixture of recent SUCCESS builds from a public k8s CI job
# into the layout perfdash's --mode=local expects:
#   <OUT>/<job>/<build>/artifacts/<perf-artifact>.json
#   <OUT>/<job>/<build>/finished.json     (for the metric timestamp)
# No GCS credentials needed: kubernetes-ci-logs is a public bucket.
set -euo pipefail

JOB="${JOB:-ci-kubernetes-e2e-gce-scale-performance-100}"
BUCKET="${BUCKET:-kubernetes-ci-logs}"
WANT="${WANT:-8}"                 # number of SUCCESS builds to collect
SCAN="${SCAN:-30}"               # how many recent build dirs to scan
OUT="${OUT:-$(cd "$(dirname "$0")/.." && pwd)/testdata/fixture}"

# perfdash OutputFilePrefixes (config.go) we care about for the "performance" job type.
PREFIXES='APIResponsiveness|APIResponsivenessPrometheus|PodStartupLatency|StatelessPodStartupLatency|StatefulPodStartupLatency|ResourceUsageSummary|MetricsForE2E|SchedulingMetrics|SchedulingThroughput|EtcdMetrics|GenericPrometheusQuery|SystemPodMetrics'

api() { curl -fsS "https://storage.googleapis.com/storage/v1/b/${BUCKET}/o?$1"; }
obj() { curl -fsS "https://storage.googleapis.com/${BUCKET}/$1"; }

echo "Fetching up to ${WANT} SUCCESS builds of ${JOB} -> ${OUT}"
mkdir -p "${OUT}"

# Newest build dirs first (build IDs are increasing integers).
builds=$(api "prefix=logs/${JOB}/&delimiter=/&maxResults=400" \
  | jq -r '.prefixes[]?' | sed "s#logs/${JOB}/##; s#/##" \
  | sort -rn | head -n "${SCAN}")

got=0
for b in ${builds}; do
  [ "${got}" -ge "${WANT}" ] && break
  fin=$(obj "logs/${JOB}/${b}/finished.json" 2>/dev/null || true)
  [ -z "${fin}" ] && continue
  result=$(echo "${fin}" | jq -r '.result // empty' 2>/dev/null || true)
  ts=$(echo "${fin}" | jq -r '.timestamp // empty' 2>/dev/null || true)
  [ "${result}" = "SUCCESS" ] || continue
  [ -n "${ts}" ] || continue

  names=$(api "prefix=logs/${JOB}/${b}/artifacts/&maxResults=2000" \
    | jq -r '.items[]?.name' | rg "/(${PREFIXES})[^/]*\.json$" || true)
  [ -z "${names}" ] && { echo "  build ${b}: no perf artifacts, skip"; continue; }

  bdir="${OUT}/${JOB}/${b}"
  mkdir -p "${bdir}/artifacts"
  printf '%s' "${fin}" > "${bdir}/finished.json"
  n=0
  while IFS= read -r name; do
    [ -z "${name}" ] && continue
    base=$(basename "${name}")
    obj "${name}" > "${bdir}/artifacts/${base}" && n=$((n+1))
  done <<< "${names}"
  got=$((got+1))
  echo "  build ${b}: SUCCESS ts=${ts}, ${n} artifacts (${got}/${WANT})"
done

echo "Done. ${got} builds under ${OUT}/${JOB}"
find "${OUT}" -name finished.json | wc -l | xargs echo "finished.json count:"
