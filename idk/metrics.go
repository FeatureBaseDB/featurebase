package idk

import "github.com/prometheus/client_golang/prometheus"

const (
	MetricDeleterRowsAdded      = "deleter_rows_added_total"
	MetricIngesterRowsAdded     = "ingester_rows_added_total"
	MetricIngesterSchemaChanges = "ingester_schema_changes_total"
	MetricCommittedRecords      = "committed_records"
)

var CounterIngesterSchemaChanges = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "ingester",
		Name:      MetricIngesterSchemaChanges,
		Help:      "TODO",
	},
)

var CounterIngesterRowsAdded = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "ingester",
		Name:      MetricIngesterRowsAdded,
		Help:      "TODO",
	},
)

var CounterCommittedRecords = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "ingester",
		Name:      MetricCommittedRecords,
		Help:      "TODO",
	},
)

var CounterDeleterRowsAdded = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "ingester",
		Name:      MetricDeleterRowsAdded,
		Help:      "TODO",
	},
	[]string{
		"type",
	},
)

func init() {
	prometheus.MustRegister(CounterIngesterSchemaChanges)
	prometheus.MustRegister(CounterIngesterRowsAdded)
	prometheus.MustRegister(CounterCommittedRecords)
	prometheus.MustRegister(CounterDeleterRowsAdded)
}
