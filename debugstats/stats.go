// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package debugstats

import (
	"fmt"
	"math"
	"runtime"
	"sort"
	"sync"
	"time"
)

type CallStats struct {
	// protect elap
	mu sync.Mutex

	// track how much time each call took.
	elap map[string]*elapsed
}

type elapsed struct {
	dur []float64
}

func NewCallStats() *CallStats {
	w := &CallStats{}
	w.Reset()
	return w
}

func (w *CallStats) Reset() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.elap = make(map[string]*elapsed)
}

type LineSorter struct {
	Line string
	Tot  float64
}

type SortByTot []*LineSorter

func (p SortByTot) Len() int {
	return len(p)
}
func (p SortByTot) Less(i, j int) bool {
	return p[i].Tot < p[j].Tot
}
func (p SortByTot) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (c *CallStats) Report(title string) (r string) {
	r = fmt.Sprintf("CallStats: (%v)\n", title)
	c.mu.Lock()
	defer c.mu.Unlock()
	var lines []*LineSorter
	for id, elap := range c.elap {
		slc := elap.dur
		n := len(slc)
		if n == 0 {
			continue
		}
		mean, sd, totaltm := computeMeanSd(slc)
		if n == 1 {
			sd = 0
			mean = slc[0]
			totaltm = slc[0]
		}
		line := fmt.Sprintf("  %20v  N=%8v   avg/op: %12v   sd: %12v  total: %12v\n", id, n, time.Duration(mean), time.Duration(sd), time.Duration(totaltm))
		lines = append(lines, &LineSorter{Line: line, Tot: totaltm})
	}
	sort.Sort(SortByTot(lines))
	for i := range lines {
		r += lines[i].Line
	}

	if false {
		var m1 runtime.MemStats
		runtime.ReadMemStats(&m1)
		r += fmt.Sprintf("\n m1.TotalAlloc = %v\n", m1.TotalAlloc)
	}

	return
}

var NaN = math.NaN()

func computeMeanSd(slc []float64) (mean, sd, tot float64) {
	if len(slc) < 2 {
		return NaN, NaN, NaN
	}
	for _, v := range slc {
		tot += v
	}
	n := float64(len(slc))
	mean = tot / n

	variance := 0.0
	for _, v := range slc {
		tmp := (v - mean)
		variance += tmp * tmp
	}
	variance = variance / n // biased, but we don't care b/c we can have very small n
	sd = math.Sqrt(variance)
	if sd < 1e-8 {
		// sd is super close to zero, NaN out the z-score rather than +/- Inf
		sd = NaN
	}
	return
}

func (c *CallStats) Add(k string, dur time.Duration) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.elap[k]
	if !ok {
		e = &elapsed{}
		c.elap[k] = e
	}
	e.dur = append(e.dur, float64(dur))
}
