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

package kindharness

import (
	"math"
	"sort"
	"strconv"
	"strings"
)

// Prometheus text parsing for the two-snapshot deltas the harness reports.
//
// Every helper here takes an explicit label filter. That is not incidental: the
// awk version of this in the throwaway A/B scripts matched "<name>_sum" for
// every resource and let the last line in the file win, so a delivery-latency
// figure reported as endpointslices was silently the services series. Requiring
// the caller to name the series is the fix.

type sample struct {
	labels string
	value  float64
}

func parse(text, name string) []sample {
	var out []sample
	for _, line := range strings.Split(text, "\n") {
		if len(line) == 0 || line[0] == '#' || !strings.HasPrefix(line, name) {
			continue
		}
		rest := line[len(name):]
		// Guard against a prefix match on a longer metric name: what follows
		// the name must be a label block or whitespace.
		if len(rest) == 0 || (rest[0] != '{' && rest[0] != ' ') {
			continue
		}
		i := strings.LastIndex(line, " ")
		if i < 0 {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(line[i+1:]), 64)
		if err != nil {
			continue
		}
		out = append(out, sample{labels: line[:i], value: v})
	}
	return out
}

// match reports whether every one of the given label filters appears in the
// series identifier, e.g. `resource="endpointslices"`.
func match(labels string, filters []string) bool {
	for _, f := range filters {
		if !strings.Contains(labels, f) {
			return false
		}
	}
	return true
}

// CounterSum totals every series of a counter family matching the filters.
func CounterSum(text, name string, filters ...string) float64 {
	var total float64
	for _, s := range parse(text, name) {
		if match(s.labels, filters) {
			total += s.value
		}
	}
	return total
}

// Rate is the per-second delta of a counter between two snapshots.
func Rate(before, after, name string, window float64, filters ...string) float64 {
	if window <= 0 {
		return 0
	}
	return (CounterSum(after, name, filters...) - CounterSum(before, name, filters...)) / window
}

// HistDelta returns the observation count and mean, in milliseconds, of a
// histogram over the interval between two snapshots.
func HistDelta(before, after, name string, filters ...string) (count float64, meanMillis float64) {
	c0 := CounterSum(before, name+"_count", filters...)
	c1 := CounterSum(after, name+"_count", filters...)
	s0 := CounterSum(before, name+"_sum", filters...)
	s1 := CounterSum(after, name+"_sum", filters...)
	n := c1 - c0
	if n <= 0 {
		return 0, 0
	}
	return n, (s1 - s0) / n * 1000
}

// bucketDeltas returns cumulative bucket counts over the interval, keyed by the
// upper bound. Bound strings are parsed, never reformatted: rewriting them with
// %g turned 0.06103515625 into 0.0610352 and broke a quantile lookup.
func bucketDeltas(before, after, name string, filters ...string) (bounds []float64, cumulative map[float64]float64, total float64) {
	prev := map[float64]float64{}
	for _, s := range parse(before, name+"_bucket") {
		if b, ok := bucketBound(s.labels, filters); ok {
			prev[b] = s.value
		}
	}
	cumulative = map[float64]float64{}
	for _, s := range parse(after, name+"_bucket") {
		b, ok := bucketBound(s.labels, filters)
		if !ok {
			continue
		}
		cumulative[b] = s.value - prev[b]
		bounds = append(bounds, b)
	}
	sort.Float64s(bounds)
	if len(bounds) > 0 {
		total = cumulative[bounds[len(bounds)-1]]
	}
	return bounds, cumulative, total
}

func bucketBound(labels string, filters []string) (float64, bool) {
	if !match(labels, filters) {
		return 0, false
	}
	i := strings.Index(labels, `le="`)
	if i < 0 {
		return 0, false
	}
	rest := labels[i+4:]
	j := strings.Index(rest, `"`)
	if j < 0 {
		return 0, false
	}
	raw := rest[:j]
	if raw == "+Inf" {
		return math.Inf(1), true
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// TailFraction is the share of observations above threshold seconds.
func TailFraction(before, after, name string, threshold float64, filters ...string) float64 {
	bounds, cum, total := bucketDeltas(before, after, name, filters...)
	if total <= 0 {
		return 0
	}
	var below float64
	for _, b := range bounds {
		if b <= threshold {
			below = cum[b]
		}
	}
	return (total - below) / total
}

// Quantile interpolates a quantile, in milliseconds, from the bucket deltas.
func Quantile(before, after, name string, q float64, filters ...string) float64 {
	bounds, cum, total := bucketDeltas(before, after, name, filters...)
	if total <= 0 {
		return 0
	}
	target := q * total
	var prevCount, prevBound float64
	for _, b := range bounds {
		if cum[b] >= target {
			if b == math.Inf(1) {
				return prevBound * 1000
			}
			frac := 0.0
			if cum[b] > prevCount {
				frac = (target - prevCount) / (cum[b] - prevCount)
			}
			return (prevBound + (b-prevBound)*frac) * 1000
		}
		prevCount, prevBound = cum[b], b
	}
	return prevBound * 1000
}
