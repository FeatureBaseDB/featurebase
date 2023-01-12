package pilosa

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// PerformanceCounter holds data about a performance counter for external consumers
type PerformanceCounter struct {
	NameSpace   string
	SubSystem   string
	CounterName string
	Help        string
	Value       int64
	CounterType int64
}

// constants for the counter types
const (
	// raw - for when you just want a count of something
	CTR_TYPE_RAW = 0
	// per second - for when you accumulate counts of things
	// a consumer would sample this at intervals to arrive at a delta
	// then divide by the time in seconds between the samples to get a
	// per-second value
	CTR_TYPE_PER_SECOND = 1
	// ratio - for when you accumulate a count of something that you
	// want to use as a numerator in a ratio calculation
	// e.g. 'cache hits' could be a counter of this type and you could
	// divide it by a 'cache lookups' counter to get the hit ratio (see below)
	CTR_TYPE_RATIO = 2
	// ratio base - for when you accumulate a count of something that you
	// want to use as a denominator in a ratio calculation
	// e.g. 'cache lookups' could be a counter of this type and you could
	// use it as the denominator in a division with a 'cache hits' counter
	// as the numerator to get the hit ratio
	CTR_TYPE_RATIO_BASE = 3
)

type PerformanceCounters struct {
	counterValues [5]perfCtrWrapper
}

type perfCtr struct {
	nameSpace   string
	subSystem   string
	name        string
	help        string
	value       int64
	counterType int64
}

func (p *perfCtr) Add(increment int64) {
	atomic.AddInt64(&p.value, increment)
}

type perfCtrWrapper struct {
	ctr *perfCtr
	fun prometheus.CounterFunc
}

var PerfCounterSQLRequestSec = perfCtr{
	nameSpace:   "pilosa",
	subSystem:   "sql_statistics",
	name:        "sql_requests_sec",
	help:        "TODO",
	value:       0,
	counterType: CTR_TYPE_PER_SECOND,
}

var PerfCounterSQLInsertsSec = perfCtr{
	nameSpace:   "pilosa",
	subSystem:   "sql_statistics",
	name:        "sql_inserts_sec",
	help:        "TODO",
	value:       0,
	counterType: CTR_TYPE_PER_SECOND,
}

var PerfCounterSQLBulkInsertsSec = perfCtr{
	nameSpace:   "pilosa",
	subSystem:   "sql_statistics",
	name:        "sql_bulk_inserts_sec",
	help:        "TODO",
	value:       0,
	counterType: CTR_TYPE_PER_SECOND,
}

var PerfCounterSQLBulkInsertBatchesSec = perfCtr{
	nameSpace:   "pilosa",
	subSystem:   "sql_statistics",
	name:        "sql_bulk_insert_batches_sec",
	help:        "TODO",
	value:       0,
	counterType: CTR_TYPE_PER_SECOND,
}

var PerfCounterSQLDeletesSec = perfCtr{
	nameSpace:   "pilosa",
	subSystem:   "sql_statistics",
	name:        "sql_deletes_sec",
	help:        "TODO",
	value:       0,
	counterType: CTR_TYPE_PER_SECOND,
}

var PerfCounters *PerformanceCounters = newPerformanceCounters()

func newPerformanceCounters() *PerformanceCounters {
	ctrs := &PerformanceCounters{
		counterValues: [5]perfCtrWrapper{
			{
				&PerfCounterSQLRequestSec,
				prometheus.NewCounterFunc(
					prometheus.CounterOpts{
						Namespace: PerfCounterSQLRequestSec.nameSpace,
						Subsystem: PerfCounterSQLRequestSec.subSystem,
						Name:      PerfCounterSQLRequestSec.name,
						Help:      PerfCounterSQLRequestSec.help,
					},
					func() float64 {
						return float64(atomic.LoadInt64(&PerfCounterSQLRequestSec.value))
					}),
			},
			{
				&PerfCounterSQLInsertsSec,
				prometheus.NewCounterFunc(
					prometheus.CounterOpts{
						Namespace: PerfCounterSQLInsertsSec.nameSpace,
						Subsystem: PerfCounterSQLInsertsSec.subSystem,
						Name:      PerfCounterSQLInsertsSec.name,
						Help:      PerfCounterSQLInsertsSec.help,
					},
					func() float64 {
						return float64(atomic.LoadInt64(&PerfCounterSQLInsertsSec.value))
					}),
			},
			{
				&PerfCounterSQLBulkInsertsSec,
				prometheus.NewCounterFunc(
					prometheus.CounterOpts{
						Namespace: PerfCounterSQLBulkInsertsSec.nameSpace,
						Subsystem: PerfCounterSQLBulkInsertsSec.subSystem,
						Name:      PerfCounterSQLBulkInsertsSec.name,
						Help:      PerfCounterSQLBulkInsertsSec.help,
					},
					func() float64 {
						return float64(atomic.LoadInt64(&PerfCounterSQLBulkInsertsSec.value))
					}),
			},
			{
				&PerfCounterSQLBulkInsertBatchesSec,
				prometheus.NewCounterFunc(
					prometheus.CounterOpts{
						Namespace: PerfCounterSQLBulkInsertBatchesSec.nameSpace,
						Subsystem: PerfCounterSQLBulkInsertBatchesSec.subSystem,
						Name:      PerfCounterSQLBulkInsertBatchesSec.name,
						Help:      PerfCounterSQLBulkInsertBatchesSec.help,
					},
					func() float64 {
						return float64(atomic.LoadInt64(&PerfCounterSQLBulkInsertBatchesSec.value))
					}),
			},
			{
				&PerfCounterSQLDeletesSec,
				prometheus.NewCounterFunc(
					prometheus.CounterOpts{
						Namespace: PerfCounterSQLDeletesSec.nameSpace,
						Subsystem: PerfCounterSQLDeletesSec.subSystem,
						Name:      PerfCounterSQLDeletesSec.name,
						Help:      PerfCounterSQLDeletesSec.help,
					},
					func() float64 {
						return float64(atomic.LoadInt64(&PerfCounterSQLDeletesSec.value))
					}),
			},
		},
	}

	for _, w := range ctrs.counterValues {
		prometheus.MustRegister(w.fun)
	}

	return ctrs
}

// list all the counters
// we can just read here without locking because if the counters get changed
// midway thru the loop, absent evidence to the contrary, the world will not end
func (p *PerformanceCounters) ListCounters() ([]PerformanceCounter, error) {
	result := make([]PerformanceCounter, len(p.counterValues))
	for i, c := range p.counterValues {
		result[i] = PerformanceCounter{
			NameSpace:   c.ctr.nameSpace,
			SubSystem:   c.ctr.subSystem,
			CounterName: c.ctr.name,
			Value:       c.ctr.value,
			CounterType: c.ctr.counterType,
		}
	}
	return result, nil
}
