# perfdash on VictoriaMetrics and Grafana

This directory deploys a new perfdash stack that runs **alongside** the legacy
perfdash, not in place of it. The legacy `perfdash` Deployment
(`perfdash/deployment.yaml`) keeps serving its own UI from in-memory state. The
new stack reuses the exact same parsing code but stores results in a real
time-series database so the data outlives the process and is queryable in
Grafana. Both run in parallel so we can compare them until we trust the new one.

## Architecture

```
GCS logs bucket ──> perfdash ingester (CronJob) ──> VictoriaMetrics ──> Grafana
   (artifacts)        legacy binary, --www=false      perfdash_value         dashboards
```

- **Ingester.** The legacy perfdash binary, rebuilt with a `--vmImportURL` flag
  and run as a batch job with `--www=false`. It walks the GCS logs bucket,
  parses the same artifacts perfdash always parsed, and POSTs samples to
  VictoriaMetrics via `/api/v1/import`. It is fully stateless: no web server, no
  long-lived cache. It runs every 10 minutes (`k8s/ingester-cronjob.yaml`).
- **VictoriaMetrics.** A single-replica StatefulSet with a 50Gi PVC and
  `-retentionPeriod=10y` (`k8s/victoriametrics.yaml`). It holds every sample
  under one metric name, `perfdash_value`, with everything else carried in
  labels.
- **Grafana.** A Deployment that provisions a Prometheus-type datasource named
  `VictoriaMetrics` (uid `victoriametrics`, `httpMethod: POST`) and the perfdash
  dashboard from ConfigMaps (`k8s/grafana.yaml`).

### Schema

Every sample is the single metric `perfdash_value`. The structure perfdash
nests in `JobToCategoryData` is flattened into reserved labels on each sample:

| label          | source                                                        |
| -------------- | ------------------------------------------------------------- |
| `prefix`       | top-level key of `JobToCategoryData` (the UI "job" dropdown)  |
| `job`          | real Prow job name (`BuildData.Job`), used for build-log links |
| `category`     | 2nd-level key (e.g. `APIServer`, `E2E`)                       |
| `metric`       | 3rd-level key (the test/metric, e.g. `LoadResponsiveness`)    |
| `series`       | a key of `DataItem.Data` (e.g. `Perc50`, `Perc99`, `<= 0.5s`) |
| `build_number` | the build id, key of the `Builds.builds` map                  |
| `unit`         | `DataItem.Unit` (e.g. `ms`, `cores`, `MiB`, or empty)         |

Plus one label per `DataItem.Labels` entry (`Resource`, `Verb`, `Scope`, ...),
with the key sanitized to `^[a-zA-Z_][a-zA-Z0-9_]*$` and `label_`-prefixed if it
would collide with a reserved label. The value is `DataItem.Data[series]`,
skipping NaN and Inf. The sample timestamp is the build's `finished.json`
`timestamp` (unix seconds) times 1000, falling back to `started.json`. Builds
where neither parses are skipped with a warning.

## Deploy

```sh
kubectl create namespace perfdash   # if it does not already exist

# Dashboard JSON is too large to inline, so create it from the checked-in source:
kubectl -n perfdash create configmap grafana-dashboard-perfdash \
  --from-file=perfdash.json=dashboards/perfdash.json \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -f k8s/victoriametrics.yaml
kubectl apply -f k8s/grafana.yaml
kubectl apply -f k8s/ingester-cronjob.yaml
```

The ingester image in `k8s/ingester-cronjob.yaml` is a placeholder
(`gcr.io/k8s-staging-perf-tests/perfdash-ingester:dev`). Build and push the
rebuilt perfdash image from `perfdash/Dockerfile`, then set it there.

## Run locally

A `docker-compose.yaml` in this directory brings up VictoriaMetrics and Grafana
with the same provisioning the cluster uses.

```sh
docker compose up -d
```

Grafana is then on `http://localhost:3000` (anonymous admin) and VictoriaMetrics
on `http://localhost:8428`. Run the ingester against the local store from the
`perfdash` directory:

```sh
go run ./... \
  --mode=gcs \
  --www=false \
  --logsBucket=kubernetes-ci-logs \
  --logsPath=logs \
  --githubConfigDir=https://api.github.com/repos/kubernetes/test-infra/contents/config/jobs/kubernetes/sig-scalability \
  --allow-parsers-matching-all-tests=false \
  --vmImportURL=http://localhost:8428/api/v1/import
```

For offline runs, first download a small fixture with
`hack/fetch-fixture.sh`, then point the ingester at it with `--mode=local` and
the checked-in local config (`testdata/config.yaml`, or
`testdata/config-5000.yaml` for the 5k job). Run from the `perfdash` directory:

```sh
hack/fetch-fixture.sh                       # -> testdata/fixture/<job>/<build>/...

go run ./... \
  --mode=local \
  --www=false \
  --logsPath=testdata/fixture \
  --configPath=testdata/config.yaml \
  --allow-parsers-matching-all-tests=false \
  --vmImportURL=http://localhost:8428/api/v1/import
```

The same fixture and config drive the legacy perfdash (`--www=true`) for the
parity check below.

## Parity check

Parity confirms the new stack reproduces what legacy perfdash shows. The
ingester and the parity tool share the schema above, so a sample
`perfdash_value{prefix,job,category,metric,series,build_number,...}` maps back to
exactly one cell legacy perfdash would render.

1. Bring up the legacy perfdash with `--www=true` and run the ingester against a
   local VictoriaMetrics over the same `--logsBucket`/`--builds` window.
2. For each `(prefix, category, metric, series, build_number)`, compare the
   value legacy perfdash returns from its `/buildsdata` endpoint against the
   matching `perfdash_value` sample queried from VictoriaMetrics. Values must
   match (modulo float formatting), and the build-log link must resolve to the
   same `https://prow.k8s.io/view/gcs/kubernetes-ci-logs/logs/<job>/<build_number>/`.

Because both stacks read the same bucket with the same parsers, any mismatch is
an ingestion or schema bug, not a data difference.

## Dropped feature

The legacy UI let you **CTRL+click a series to cap outliers** (rescale the plot
by clamping extreme points). Grafana has no equivalent click-to-clamp gesture,
so this is the one feature not carried over. Outliers are instead handled in
Grafana the usual way: panel-level min/max, soft axis limits, or a percentile
query. The underlying samples are unchanged, so nothing is lost from the data
itself.
