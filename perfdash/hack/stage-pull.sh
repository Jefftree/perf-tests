#!/usr/bin/env bash
# Stage new presubmit (pr-logs) builds of the pull 5k scale job into the
# --mode=local layout: <OUT>/<job>/<build>/{finished.json,artifacts/*.json}
set -euo pipefail

BUCKET=kubernetes-ci-logs
JOB=pull-kubernetes-gce-master-scale-performance-5000
OUT="${OUT:?set OUT}"
BUILDS_FILE="${BUILDS_FILE:?set BUILDS_FILE}"

PREFIXES='APIResponsiveness|APIResponsivenessPrometheus|PodStartupLatency|StatelessPodStartupLatency|StatefulPodStartupLatency|ResourceUsageSummary|MetricsForE2E|SchedulingMetrics|SchedulingThroughput|EtcdMetrics|GenericPrometheusQuery|SystemPodMetrics'

api() { curl -fsS "https://storage.googleapis.com/storage/v1/b/${BUCKET}/o?$1"; }
obj() { curl -fsS "https://storage.googleapis.com/${BUCKET}/$1"; }
# Object download: percent-encode spaces (some artifact names contain them).
objenc() { local p="${1// /%20}"; curl -fsS "https://storage.googleapis.com/${BUCKET}/${p}"; }

while IFS= read -r b; do
  [ -z "$b" ] && continue
  ptr=$(obj "pr-logs/directory/${JOB}/${b}.txt" 2>/dev/null || true)
  [ -z "$ptr" ] && { echo "  ${b}: no pointer, skip"; continue; }
  rel="${ptr#gs://${BUCKET}/}"   # pr-logs/pull/<PR>/<job>/<build>
  fin=$(obj "${rel}/finished.json" 2>/dev/null || true)
  [ -z "$fin" ] && { echo "  ${b}: no finished.json, skip"; continue; }
  ts=$(echo "$fin" | jq -r '.timestamp // empty' 2>/dev/null || true)
  res=$(echo "$fin" | jq -r '.result // empty' 2>/dev/null || true)
  [ -n "$ts" ] || { echo "  ${b}: no timestamp, skip"; continue; }

  names=$(api "prefix=${rel}/artifacts/&maxResults=3000" \
    | jq -r '.items[]?.name' | rg "/(${PREFIXES})[^/]*\.json$" || true)

  bdir="${OUT}/${JOB}/${b}"
  mkdir -p "${bdir}/artifacts"
  printf '%s' "$fin" > "${bdir}/finished.json"
  n=0
  while IFS= read -r name; do
    [ -z "$name" ] && continue
    objenc "$name" > "${bdir}/artifacts/$(basename "$name")" && n=$((n+1))
  done <<< "$names"
  echo "  ${b}: ${res} ts=${ts}, ${n} artifacts"
done < "$BUILDS_FILE"

echo "Staged under ${OUT}/${JOB}:"
find "${OUT}/${JOB}" -maxdepth 1 -mindepth 1 -type d | wc -l | xargs echo "  build dirs:"