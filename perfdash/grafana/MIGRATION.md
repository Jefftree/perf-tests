# Perfdash to Grafana migration plan

This is the plan of record for moving perfdash off its bespoke in-memory
frontend and onto the VictoriaMetrics + Grafana stack in this directory. It runs
as a strangler-fig migration: the new stack runs alongside legacy perfdash,
reproduces it faithfully first, earns trust through parity, and only then
gradually replaces the legacy navigation with Grafana-native idioms. Nothing in
legacy perfdash is removed until the new path has carried its traffic.

## Goal and non-goals

Goal: every number a user reads today in perfdash is queryable in Grafana,
backed by a real time-series store that outlives the process, with the legacy
in-memory map (and its multi-GB heap) retired.

Non-goals: this is not a redesign of what perfdash measures. The parsers, the
artifacts, and the metric definitions are unchanged. We are moving the storage
and the presentation, not the measurement.

## The seam (what makes the strangler work)

Both UIs read the same data: the single metric `perfdash_value` in
VictoriaMetrics, with everything carried in labels (schema in
[README.md](README.md#schema)). That metric is the stable interface, the
equivalent of a frozen FFI boundary in a code migration. Legacy perfdash and the
Grafana board can run concurrently against identical data because they read the
same store. Every migration step is gated by the parity check
([hack/parity-check.sh](../hack/parity-check.sh) and
[hack/parity-sweep.sh](../hack/parity-sweep.sh)), which compares legacy
`/buildsdata` values against the matching `perfdash_value` samples. Because both
sides read the same bucket with the same parsers, any mismatch is an ingestion
or schema bug, not a data difference.

Parity is about values, not layout. The parity check compares
`(prefix, category, metric, series, build_number)` cells. UI shape is orthogonal
to it. This is what frees us to restructure the dashboard however reads best in
Grafana without ever breaking the parity bar.

## Parity is behavioral, not pixel

"UI parity" in phase 1 means a user lands on the same chart through the same
clicks, using native Grafana primitives. It does not mean rebuilding the legacy
Chart.js page. We reproduce the navigation model (the cascade job -> category ->
metric -> resource -> verb, and the two views: value over time and value over
builds), not the pixels. Faithfully reproducing a legacy wart (for example empty
resource/verb dropdowns on metrics that have no such labels) is honest parity,
not a bug to fix in phase 1.

## Schema reality that shapes the end state

The `perfdash_value` label set is bimodal, measured on a real import:

- 16 of 43 metrics carry `Resource`/`Verb` (the API-responsiveness,
  request-count, and resources families). These dominate series volume (~92% of
  series) through the Resource x Verb x Scope fan-out.
- The other 27 metrics (pod startup, scheduling latency and throughput, DNS, WAL
  fsync, snapshot save, and so on) have no `Resource`/`Verb` at all, only the
  percentile series.

So a single universal five-level cascade has dead dropdowns for most metrics.
That is acceptable in phase 1 (legacy had the same emptiness) and is the thing
the later phases strangle away.

## Phases

### Phase 0 — Freeze the interface

The data contract is `perfdash_value` plus its reserved labels. It is done. Do
not churn it. Everything downstream depends on it being stable, the same way a
strangler migration depends on a stable boundary between old and new.

Exit criteria: schema documented in README, ingester writes it, dashboard and
parity check read it. (Already met.)

### Phase 1 — Ship the faithful cascade, gate on parity

The current [dashboards/perfdash.json](dashboards/perfdash.json) is the phase-1
artifact. It reproduces the legacy cascade (`$prefix -> $category -> $metric ->
$resource -> $verb`) and the two panels. Do not trim it yet.

Steps:
1. Bring up VictoriaMetrics + Grafana and a fresh GCS import (see README "Run
   locally").
2. Run legacy perfdash (`--www=true`) and the ingester against the same window.
3. Pass `hack/parity-sweep.sh` across the metric families with zero unexplained
   mismatches.
4. Announce that Grafana mirrors perfdash.

Exit criteria: parity sweep is green and reproducible, both stacks visibly agree
on a few hand-checked metrics. This is the trust milestone. Treat it as the
release gate, not an afterthought.

### Phase 2 — Add native affordances without removing anything

Legacy navigation stays the default. Add Grafana-native paths alongside it so
power users discover the better ones:

- Surface the existing adhoc filter (`$adhoc`) next to the cascade. It
  auto-discovers label keys and values for the current selection, populated for
  the 16 API metrics and silent for the other 27. This is the native fix for the
  bimodal schema.
- Add an opt-in repeated-panel row (repeat by `Verb` or `Resource`) so one
  selection fans out into a grid instead of one series at a time.
- Link out to a focused API-responsiveness board as a new dashboard. Do not
  touch the main board's cascade.

Exit criteria: native paths exist and are linked, parity sweep still green, no
legacy selector removed.

### Phase 3 — Strangle the cascade one dropdown at a time

Once the native paths carry real usage, retire the dead selectors in order:
`$verb` first, then `$resource` (the two that are empty for ~63% of metrics),
leaving `prefix/category/metric` plus the adhoc filter. Each removal is an
independent, git-revertible dashboard edit. Re-run parity after each.

Exit criteria: cascade reduced to `prefix/category/metric` + adhoc, parity still
green, no user-reported navigation regression.

### Phase 4 — Retire legacy perfdash

When nothing routes to the legacy UI, delete it: the `--www` server path, the
vendored Chart.js frontend under `www/`, and the serving Deployment. The Go ETL
and parsers stay, repurposed as the stateless ingester. This is where the net
line-count drop lands (killing the ~3,477-line vendored Chart.js for the
~357-line dashboard JSON).

Exit criteria: legacy Deployment removed, ingester CronJob is the only data
path, Grafana is the only UI.

## Why this analogy is easier than a code migration

In a bun/rust port the new code path is expensive and risky to change, so you
strangle cautiously. Here the UI layer is throwaway JSON in git. Changing a
dashboard costs almost nothing and is fully reversible. So once phase 1 clears
the trust gate, phases 2 and 3 can be far more experimental than a code
migration would ever allow. The only things that must be protected are the data
contract (phase 0) and the parity bar.

## Rollback

Every phase is reversible. Phase 0-3 changes are dashboard JSON or additive
provisioning, revert with git. Phase 4 (deleting legacy) is the only one-way
door, and it is gated on the new path already carrying all traffic. Until then,
legacy perfdash keeps serving from its own in-memory state and is unaffected by
anything in this directory.

## Open question: repo home

Tracked separately. The stack lives in `kubernetes/perf-tests/perfdash` today
because it reuses that exact Go parsing code as the ingester. Whether it should
graduate to its own repo is a real question once phase 4 removes the legacy
coupling. See the discussion thread / follow-up rather than deciding it here.
