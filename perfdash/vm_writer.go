/*
Copyright 2026 The Kubernetes Authors.

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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"

	"k8s.io/klog"
)

// vmMetricName is the single VictoriaMetrics metric name. Every perfdash data
// point is exported under this name and differentiated entirely by labels.
const vmMetricName = "perfdash_value"

// vmMaxLinesPerPost caps how many newline-delimited JSON lines are sent in one
// POST to the import endpoint.
const vmMaxLinesPerPost = 2000

// vmReservedLabels are the label keys that the schema always sets. A sanitized
// DataItem.Labels key that collides with one of these is prefixed with
// "label_" to avoid clobbering it.
var vmReservedLabels = map[string]bool{
	"__name__":     true,
	"prefix":       true,
	"job":          true,
	"category":     true,
	"metric":       true,
	"series":       true,
	"build_number": true,
	"unit":         true,
}

// vmSample is one line of the VictoriaMetrics JSON import format.
type vmSample struct {
	Metric     map[string]string `json:"metric"`
	Values     []float64         `json:"values"`
	Timestamps []int64           `json:"timestamps"`
}

// finishedJSON is the relevant subset of finished.json / started.json.
type finishedJSON struct {
	Timestamp int64 `json:"timestamp"`
}

// writeToVictoriaMetrics walks the entire JobToCategoryData and POSTs every
// data point to VictoriaMetrics' JSON import endpoint (importURL, e.g.
// http://localhost:8428/api/v1/import). Each emitted line is one sample under
// the perfdash_value metric, timestamped by the build's finished.json.
func writeToVictoriaMetrics(bkt MetricsBucket, data JobToCategoryData, importURL string) error {
	tsCache := newBuildTimestampCache(bkt)

	var batch bytes.Buffer
	batchLines := 0
	totalSeries := 0
	skippedBuilds := 0

	flush := func() error {
		if batchLines == 0 {
			return nil
		}
		if err := postImport(importURL, batch.Bytes()); err != nil {
			return err
		}
		batch.Reset()
		batchLines = 0
		return nil
	}

	enc := json.NewEncoder(&batch)

	for prefix, categories := range data {
		for category, metrics := range categories {
			for metricName, buildData := range metrics {
				if buildData == nil {
					continue
				}
				job := buildData.Job
				buildData.Builds.mu.RLock()
				builds := buildData.Builds.builds
				for buildKey, items := range builds {
					tsMillis, ok := tsCache.get(job, buildKey)
					if !ok {
						skippedBuilds++
						continue
					}
					for i := range items {
						item := &items[i]
						for seriesKey, value := range item.Data {
							if math.IsNaN(value) || math.IsInf(value, 0) {
								continue
							}
							sample := vmSample{
								Metric:     buildSampleLabels(prefix, job, category, metricName, seriesKey, buildKey, item.Unit, item.Labels),
								Values:     []float64{value},
								Timestamps: []int64{tsMillis},
							}
							// json.Encoder.Encode appends a trailing newline,
							// which is exactly the line delimiter the import
							// endpoint expects.
							if err := enc.Encode(&sample); err != nil {
								buildData.Builds.mu.RUnlock()
								return fmt.Errorf("failed to encode sample for prefix=%q job=%q build=%q: %v", prefix, job, buildKey, err)
							}
							batchLines++
							totalSeries++
							if batchLines >= vmMaxLinesPerPost {
								if err := flush(); err != nil {
									buildData.Builds.mu.RUnlock()
									return err
								}
							}
						}
					}
				}
				buildData.Builds.mu.RUnlock()
			}
		}
	}

	if err := flush(); err != nil {
		return err
	}

	klog.Infof("VictoriaMetrics import complete: wrote %d series to %s (skipped %d builds with no usable timestamp)", totalSeries, importURL, skippedBuilds)
	return nil
}

// buildSampleLabels builds the full label set for a sample, applying key
// sanitization and reserved-name collision avoidance to DataItem.Labels.
func buildSampleLabels(prefix, job, category, metricName, series, buildNumber, unit string, extra map[string]string) map[string]string {
	labels := map[string]string{
		"__name__":     vmMetricName,
		"prefix":       prefix,
		"job":          job,
		"category":     category,
		"metric":       metricName,
		"series":       series,
		"build_number": buildNumber,
		"unit":         unit,
	}
	for k, v := range extra {
		key := sanitizeLabelKey(k)
		if vmReservedLabels[key] {
			key = "label_" + key
		}
		labels[key] = v
	}
	return labels
}

// sanitizeLabelKey rewrites a label key to satisfy Prometheus' rule
// ^[a-zA-Z_][a-zA-Z0-9_]*$: non-conforming characters become "_", and a
// leading digit gets an "_" prefix. Label values are left untouched.
func sanitizeLabelKey(key string) string {
	if key == "" {
		return "_"
	}
	out := make([]byte, 0, len(key)+1)
	for i := 0; i < len(key); i++ {
		c := key[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c == '_':
			out = append(out, c)
		case c >= '0' && c <= '9':
			if i == 0 {
				out = append(out, '_')
			}
			out = append(out, c)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}

// postImport sends one batch of newline-delimited JSON to the import endpoint.
// Any non-2xx response is treated as an error and includes the response body.
func postImport(importURL string, body []byte) error {
	req, err := http.NewRequest(http.MethodPost, importURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to build import request: %v", err)
	}
	req.Header.Set("Content-Type", "text/plain")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to POST to %s: %v", importURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("import endpoint %s returned %s: %s", importURL, resp.Status, string(respBody))
	}
	return nil
}

// buildTimestampCache resolves and caches the millisecond sample timestamp for
// each (job, build) pair so finished.json is read at most once per build.
type buildTimestampCache struct {
	bkt   MetricsBucket
	cache map[string]cachedTimestamp
}

type cachedTimestamp struct {
	millis int64
	ok     bool
}

func newBuildTimestampCache(bkt MetricsBucket) *buildTimestampCache {
	return &buildTimestampCache{
		bkt:   bkt,
		cache: make(map[string]cachedTimestamp),
	}
}

// get returns the sample timestamp in milliseconds for a build, reading
// finished.json (falling back to started.json) on first access. The second
// return value is false if neither file yields a usable timestamp, in which
// case the build's samples should be skipped.
func (c *buildTimestampCache) get(job, buildKey string) (int64, bool) {
	cacheKey := job + "\x00" + buildKey
	if entry, found := c.cache[cacheKey]; found {
		return entry.millis, entry.ok
	}

	millis, ok := c.resolve(job, buildKey)
	c.cache[cacheKey] = cachedTimestamp{millis: millis, ok: ok}
	return millis, ok
}

func (c *buildTimestampCache) resolve(job, buildKey string) (int64, bool) {
	buildNumber, err := strconv.Atoi(buildKey)
	if err != nil {
		klog.Warningf("skipping build with non-numeric id job=%q build=%q: %v", job, buildKey, err)
		return 0, false
	}

	for _, name := range []string{"finished.json", "started.json"} {
		raw, err := c.bkt.ReadFile(job, buildNumber, name)
		if err != nil {
			continue
		}
		var parsed finishedJSON
		if err := json.Unmarshal(raw, &parsed); err != nil {
			klog.Warningf("failed to parse %s for job=%q build=%q: %v", name, job, buildKey, err)
			continue
		}
		if parsed.Timestamp <= 0 {
			continue
		}
		return parsed.Timestamp * 1000, true
	}

	klog.Warningf("no usable timestamp from finished.json/started.json for job=%q build=%q; skipping its samples", job, buildKey)
	return 0, false
}
