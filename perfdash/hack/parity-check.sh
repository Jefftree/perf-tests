#!/usr/bin/env bash
#
# parity-check.sh — prove the new VictoriaMetrics-backed stack reports the SAME
# numbers as the legacy perfdash, for the same fixture builds.
#
# Two services must already be up against the SAME fixture (this script never
# starts them):
#   LEGACY perfdash in www mode (local fixture)   -> $PERFDASH_URL (default http://localhost:8081)
#   VictoriaMetrics with the fixture ingested      -> $VM_URL       (default http://localhost:8428)
#
# Usage:
#   # 1) Discover what is comparable on both sides (no value comparison):
#   ./hack/parity-check.sh
#
#   # 2) Compare values for one (prefix, category, metric), optional SERIES filter:
#   PREFIX=gce-100Nodes CATEGORY=APIServer METRIC=LoadResponsiveness \
#     ./hack/parity-check.sh
#
# Schema (must match the writer + Grafana dashboard exactly):
#   __name__ = "perfdash_value", differentiated only by labels.
#   reserved labels: prefix, job, category, metric, series, build_number, unit
#   plus one label per DataItem.Labels entry (sanitized key).
#   On the legacy side those map to:
#     prefix       = buildsdata jobname  (top-level JobToCategoryData key)
#     category     = metriccategoryname
#     metric       = metricname
#     build_number = key of buildsdata .builds
#     series       = key of DataItem.data
#     unit         = DataItem.unit
#     extra labels = DataItem.labels (key + value)
#
# Comparison key per data point:
#   (build_number, series, sorted "k=v" of extra labels) -> value
# "job" and "unit" are intentionally NOT part of the key: they are carried for
# context but a value should match regardless. Set STRICT_UNIT=1 to fold unit
# into the key as well.
#
# Exit status: 0 = PASS (every tuple matched within epsilon), non-zero = FAIL.

set -uo pipefail

PERFDASH_URL="${PERFDASH_URL:-http://localhost:8081}"
VM_URL="${VM_URL:-http://localhost:8428}"
# Fixture builds carry their real (often weeks/months old) finish timestamps,
# so bound every VM query with a wide window or VM's recent-window default hides them.
VM_START="${VM_START:-1700000000}"    # 2023-11-14
VM_END="${VM_END:-1900000000}"        # 2030-03-17
EPSILON="${EPSILON:-0.000001}"        # 1e-6 relative tolerance
MAX_EXAMPLES="${MAX_EXAMPLES:-10}"
STRICT_UNIT="${STRICT_UNIT:-0}"       # 1 => include unit in the comparison key

PREFIX="${PREFIX:-}"
CATEGORY="${CATEGORY:-}"
METRIC="${METRIC:-}"
SERIES="${SERIES:-}"                  # optional: restrict comparison to one series

err() { printf '%s\n' "$*" >&2; }
die() { err "ERROR: $*"; exit 2; }

command -v curl >/dev/null 2>&1 || die "curl not found on PATH"
command -v jq   >/dev/null 2>&1 || die "jq not found on PATH"

# curl helper: fail loudly on HTTP errors, keep the body for jq.
fetch() {
  # $1 = url ; prints body to stdout, returns curl's exit code
  curl -fsS --max-time "${CURL_TIMEOUT:-30}" "$1"
}

# VM label_values for a given label, optionally constrained by a match[] selector.
vm_label_values() {
  # $1 = label name ; $2 = optional match[] selector
  local label="$1" sel="${2:-}"
  local url="${VM_URL}/api/v1/label/${label}/values?start=${VM_START}&end=${VM_END}"
  if [[ -n "${sel}" ]]; then
    url+="&match[]=$(jq -rn --arg s "${sel}" '$s|@uri')"
  fi
  fetch "${url}" | jq -r '.data[]?' 2>/dev/null | sort -u
}

# ---------------------------------------------------------------------------
# Discovery mode: no PREFIX/CATEGORY/METRIC -> show what both sides expose.
# ---------------------------------------------------------------------------
if [[ -z "${PREFIX}" || -z "${CATEGORY}" || -z "${METRIC}" ]]; then
  err "Discovery mode (set PREFIX, CATEGORY and METRIC to run a value comparison)."
  err ""
  err "PERFDASH_URL=${PERFDASH_URL}   VM_URL=${VM_URL}"
  err ""

  legacy_prefixes="$(fetch "${PERFDASH_URL}/jobnames" | jq -r '.[]?' 2>/dev/null | sort -u)" \
    || die "could not read legacy /jobnames from ${PERFDASH_URL}"
  vm_prefixes="$(vm_label_values prefix 'perfdash_value')" \
    || die "could not read VM prefix label_values from ${VM_URL}"

  err "== prefix (legacy /jobnames vs VM prefix=) =="
  comm_overlap() {
    # $1 = "left set" newline list (legacy) ; $2 = "right set" (VM)
    # prints three sections: BOTH / LEGACY-ONLY / VM-ONLY
    local left="$1" right="$2"
    printf '  in BOTH:\n'
    comm -12 <(printf '%s\n' "${left}") <(printf '%s\n' "${right}") | sed 's/^/    /'
    printf '  legacy only:\n'
    comm -23 <(printf '%s\n' "${left}") <(printf '%s\n' "${right}") | sed 's/^/    /'
    printf '  VM only:\n'
    comm -13 <(printf '%s\n' "${left}") <(printf '%s\n' "${right}") | sed 's/^/    /'
  }
  comm_overlap "${legacy_prefixes}" "${vm_prefixes}" >&2

  err ""
  err "== VM label_values (whole perfdash_value metric) =="
  err "  categories:"
  vm_label_values category 'perfdash_value' | sed 's/^/    /' >&2
  err "  metrics:"
  vm_label_values metric 'perfdash_value' | sed 's/^/    /' >&2

  err ""
  err "Pick a prefix that is in BOTH, then drill down on the legacy side, e.g.:"
  err "  curl -s '${PERFDASH_URL}/metriccategorynames?jobname=<PREFIX>' | jq ."
  err "  curl -s '${PERFDASH_URL}/metricnames?jobname=<PREFIX>&metriccategoryname=<CATEGORY>' | jq ."
  err ""
  err "Then re-run:"
  err "  PREFIX=<P> CATEGORY=<C> METRIC=<M> $0"
  exit 0
fi

# ---------------------------------------------------------------------------
# Comparison mode.
# ---------------------------------------------------------------------------
err "Comparing prefix=${PREFIX} category=${CATEGORY} metric=${METRIC}${SERIES:+ series=${SERIES}}"
err "  legacy: ${PERFDASH_URL}   VM: ${VM_URL}   epsilon(rel)=${EPSILON}  strict_unit=${STRICT_UNIT}"

# --- LEGACY side -----------------------------------------------------------
# /buildsdata returns: {"builds":{"<build>":[{data,unit,labels},...]},"job":..}
# Flatten to one TSV row per (build, series, extra-labels) data point:
#   key <TAB> value <TAB> unit
legacy_raw="$(
  fetch "${PERFDASH_URL}/buildsdata?jobname=$(jq -rn --arg s "${PREFIX}"   '$s|@uri')&metriccategoryname=$(jq -rn --arg s "${CATEGORY}" '$s|@uri')&metricname=$(jq -rn --arg s "${METRIC}" '$s|@uri')"
)" || die "legacy /buildsdata fetch failed"

legacy_tsv="$(
  printf '%s' "${legacy_raw}" | jq -r --arg series "${SERIES}" '
    # extra labels -> sorted "k=v|k=v" string (reserved keys are not in .labels)
    # Prometheus/VM treats an empty label value as the label being absent, so
    # drop empty-valued labels here to compare keys on equal footing.
    def extralabels:
      ( .labels // {} ) | to_entries | map(select(.value != "")) | sort_by(.key)
        | map("\(.key)=\(.value)") | join("|");
    ( .builds // {} ) | to_entries[]
    | .key as $build
    | .value[]                                   # each DataItem in the build array
    | .unit as $unit
    | (extralabels) as $extra
    | ( .data // {} ) | to_entries[]
    | select($series == "" or .key == $series)
    | [ ($build + "" + .key + "" + $extra),   # comparison key
        (.value|tostring),
        $unit ] | @tsv
  '
)" || die "could not parse legacy buildsdata JSON (unexpected shape)"

# --- VM side ---------------------------------------------------------------
# /api/v1/export streams one JSON object per series:
#   {"metric":{__name__,prefix,job,category,metric,series,build_number,unit,...},
#    "values":[...],"timestamps":[...]}
# Same key construction; extra labels = everything except the reserved set + __name__.
vm_selector="perfdash_value{prefix=\"${PREFIX}\",category=\"${CATEGORY}\",metric=\"${METRIC}\"}"
vm_raw="$(
  fetch "${VM_URL}/api/v1/export?start=${VM_START}&end=${VM_END}&match[]=$(jq -rn --arg s "${vm_selector}" '$s|@uri')"
)" || die "VM /api/v1/export fetch failed"

# Export is JSON-lines (one object per line). -c keeps it that way; we reduce each.
vm_tsv="$(
  printf '%s' "${vm_raw}" | jq -rc --arg series "${SERIES}" '
    .metric as $m
    | ($m.series // "") as $s
    | select($series == "" or $s == $series)
    | ( $m
        | del(.__name__, .prefix, .job, .category, .metric,
              .series, .build_number, .unit)
      ) as $extramap
    | ( $extramap | to_entries | map(select(.value != "")) | sort_by(.key)
        | map("\(.key)=\(.value)") | join("|") ) as $extra
    | ($m.build_number // "") as $build
    | ($m.unit // "") as $unit
    | ( .values[] ) as $v               # a fixture build emits one sample/series
    | [ ($build + "" + $s + "" + $extra),
        ($v|tostring),
        $unit ] | @tsv
  '
)" || die "could not parse VM export JSON (unexpected shape)"

# --- Normalize: emit EVERY (key,value) point (no collapse), optional unit fold.
# Keeping all points lets us detect duplicate-labelset collisions: perfdash's
# suite-overlap categories can emit the same labelset twice with different values
# in one build, which a labels-keyed TSDB (VM) cannot represent (it keeps one).
norm() {
  # stdin = key\tvalue\tunit ; STRICT_UNIT folds unit into the key
  awk -F'\t' -v strict="${STRICT_UNIT}" '
    NF >= 2 {
      k=$1; v=$2; u=(NF>=3?$3:"")
      if (strict=="1") k=k "\x01unit=" u
      printf "%s\t%s\n", k, v
    }
  '
}

legacy_norm="$(printf '%s\n' "${legacy_tsv}" | norm)"
vm_norm="$(printf '%s\n'    "${vm_tsv}"    | norm)"

legacy_count="$(printf '%s' "${legacy_norm}" | grep -c . || true)"
vm_count="$(printf '%s'    "${vm_norm}"    | grep -c . || true)"

# --- Diff with awk. Per key, track min/max/last on each side (relative eps).
# A key whose legacy (or VM) values span more than eps is a duplicate-labelset
# COLLISION, not a value error. For those we only check that VM kept one of the
# legitimate legacy values (faithful); otherwise it is a genuine MISMATCH.
report="$(
  awk -F'\t' -v eps="${EPSILON}" -v maxex="${MAX_EXAMPLES}" '
    function absf(x){ return x<0 ? -x : x }
    function rel(a,b,  d){ d=absf(a); if(d<1)d=1; return absf(a-b)/d }
    FNR==NR {
      k=$1; v=$2+0
      if(!(k in Lc)){ Lmin[k]=v; Lmax[k]=v } else { if(v<Lmin[k])Lmin[k]=v; if(v>Lmax[k])Lmax[k]=v }
      Lc[k]++; Llast[k]=v; next
    }
    {
      k=$1; v=$2+0
      if(!(k in Vc)){ Vmin[k]=v; Vmax[k]=v } else { if(v<Vmin[k])Vmin[k]=v; if(v>Vmax[k])Vmax[k]=v }
      Vc[k]++; Vlast[k]=v
    }
    END {
      matched=0; mismatched=0; ambiguous=0; ambiguous_lost=0; legacy_only=0; vm_only=0; shown=0
      for (k in Lc) {
        if (!(k in Vc)) {
          legacy_only++
          if (shown<maxex){ printf "  LEGACY-ONLY key=%s legacy=%s\n", k, Llast[k]; shown++ }
          continue
        }
        lAmb = (rel(Lmax[k], Lmin[k]) > eps)
        vAmb = (rel(Vmax[k], Vmin[k]) > eps)
        if (lAmb || vAmb) {
          ambiguous++
          tol = absf(Vlast[k])*eps + 1e-9
          faithful = (Vlast[k] >= Lmin[k]-tol && Vlast[k] <= Lmax[k]+tol)
          if (!faithful) {
            ambiguous_lost++
            if (shown<maxex){ printf "  AMBIG-LOST key=%s legacy_range=[%s..%s] vm=%s\n", k, Lmin[k], Lmax[k], Vlast[k]; shown++ }
          }
        } else if (rel(Llast[k], Vlast[k]) <= eps) {
          matched++
        } else {
          mismatched++
          if (shown<maxex){ printf "  MISMATCH key=%s legacy=%s vm=%s\n", k, Llast[k], Vlast[k]; shown++ }
        }
      }
      for (k in Vc) {
        if (!(k in Lc)) {
          vm_only++
          if (shown<maxex){ printf "  VM-ONLY key=%s vm=%s\n", k, Vlast[k]; shown++ }
        }
      }
      printf "@@COUNTS matched=%d mismatched=%d ambiguous=%d ambiguous_lost=%d legacy_only=%d vm_only=%d\n", \
             matched, mismatched, ambiguous, ambiguous_lost, legacy_only, vm_only
    }
  ' <(printf '%s\n' "${legacy_norm}") <(printf '%s\n' "${vm_norm}")
)"

# Pull the summary counts line out; the rest are example lines (\x01 -> | for reading).
counts_line="$(printf '%s\n' "${report}" | grep '^@@COUNTS' | head -1)"
examples="$(printf '%s\n' "${report}" | grep -v '^@@COUNTS' | tr '\001' '|')"

eval "$(printf '%s\n' "${counts_line}" | sed 's/^@@COUNTS //; s/ /\n/g' | sed 's/^/CMP_/')"
# now: CMP_matched CMP_mismatched CMP_ambiguous CMP_ambiguous_lost CMP_legacy_only CMP_vm_only

err ""
err "legacy data points:    ${legacy_count}"
err "VM data points:        ${vm_count}"
err "matched:               ${CMP_matched:-0}"
err "mismatched:            ${CMP_mismatched:-0}"
err "ambiguous (dup-label): ${CMP_ambiguous:-0} (of which not-faithful: ${CMP_ambiguous_lost:-0})"
err "legacy-only:           ${CMP_legacy_only:-0}"
err "VM-only:               ${CMP_vm_only:-0}"

bad=$(( ${CMP_mismatched:-0} + ${CMP_ambiguous_lost:-0} + ${CMP_legacy_only:-0} + ${CMP_vm_only:-0} ))
if [[ "${bad}" -gt 0 && -n "${examples}" ]]; then
  err ""
  err "examples (up to ${MAX_EXAMPLES}):"
  printf '%s\n' "${examples}" | sed '/^$/d' >&2
fi

err ""
if [[ "${bad}" -eq 0 && "${CMP_matched:-0}" -gt 0 ]]; then
  echo "PASS prefix=${PREFIX} category=${CATEGORY} metric=${METRIC}${SERIES:+ series=${SERIES}}: ${CMP_matched} tuples match, 0 real diffs (${CMP_ambiguous:-0} dup-label collisions, all faithful)"
  exit 0
elif [[ "${CMP_matched:-0}" -eq 0 && "${bad}" -eq 0 ]]; then
  echo "FAIL prefix=${PREFIX} category=${CATEGORY} metric=${METRIC}: no comparable data points on either side"
  exit 1
else
  echo "FAIL prefix=${PREFIX} category=${CATEGORY} metric=${METRIC}: ${CMP_mismatched:-0} mismatched, ${CMP_ambiguous_lost:-0} dup-label-not-faithful, ${CMP_legacy_only:-0} legacy-only, ${CMP_vm_only:-0} VM-only (${CMP_matched:-0} matched)"
  exit 1
fi
