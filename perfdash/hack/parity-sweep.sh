#!/usr/bin/env bash
# Runs parity-check.sh across every (category, metric) the legacy perfdash exposes
# for a prefix, and tallies value matches/mismatches. Drives the local test only.
set -uo pipefail
cd "$(dirname "$0")/.."

PERFDASH_URL="${PERFDASH_URL:-http://localhost:8081}"
VM_URL="${VM_URL:-http://localhost:8428}"
PREFIX="${PREFIX:-gce-100Nodes}"
export PERFDASH_URL VM_URL

enc() { jq -rn --arg s "$1" '$s|@uri'; }
results="$(mktemp)"

printf "%-34s %-24s %7s %7s %7s %5s %6s %6s %6s\n" CATEGORY METRIC L_pts V_pts match MISM ambig Lonly Vonly

cats="$(curl -s "${PERFDASH_URL}/metriccategorynames?jobname=$(enc "$PREFIX")" | jq -r '.[]')"
while IFS= read -r c; do
  [ -z "$c" ] && continue
  mets="$(curl -s "${PERFDASH_URL}/metricnames?jobname=$(enc "$PREFIX")&metriccategoryname=$(enc "$c")" | jq -r '.[]?')"
  while IFS= read -r m; do
    [ -z "$m" ] && continue
    out="$(PREFIX="$PREFIX" CATEGORY="$c" METRIC="$m" ./hack/parity-check.sh 2>&1)"
    val() { printf '%s\n' "$out" | sed -n "s/^$1: *//p"; }
    l="$(val 'legacy data points')"; v="$(val 'VM data points')"
    ma="$(val matched)"; mm="$(val mismatched)"; lo="$(val legacy-only)"; vo="$(val 'VM-only')"
    amb="$(printf '%s\n' "$out" | sed -n 's/^ambiguous (dup-label): *\([0-9]*\).*/\1/p')"
    al="$(printf '%s\n' "$out" | sed -n 's/.*not-faithful: *\([0-9]*\).*/\1/p')"
    l="${l:-0}"; v="${v:-0}"; ma="${ma:-0}"; mm="${mm:-0}"; lo="${lo:-0}"; vo="${vo:-0}"; amb="${amb:-0}"; al="${al:-0}"
    flag=""; { [ "$mm" != 0 ] || [ "$al" != 0 ] || [ "$lo" != 0 ] || [ "$vo" != 0 ]; } && flag=" FAIL"
    printf "%-34s %-24s %7s %7s %7s %5s %6s %6s %6s%s\n" "${c:0:34}" "${m:0:24}" "$l" "$v" "$ma" "$mm" "$amb" "$lo" "$vo" "$flag"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$l" "$v" "$ma" "$mm" "$amb" "$al" "$lo" "$vo" >> "$results"
  done <<< "$mets"
done <<< "$cats"

echo "---------------------------------------------------------------------------------------------------"
awk -F'\t' '{pl+=$1; pv+=$2; pm+=$3; pmm+=$4; pamb+=$5; pal+=$6; plo+=$7; pvo+=$8; n++; if($4!=0||$6!=0||$7!=0||$8!=0)vf++}
END{printf "pairs=%d legacy_pts=%d vm_pts=%d matched=%d MISMATCHED=%d ambiguous_duplabel=%d (not_faithful=%d) legacy_only=%d vm_only=%d fail_pairs=%d\n", n,pl,pv,pm,pmm,pamb,pal,plo,pvo,vf}' "$results"
rm -f "$results"
