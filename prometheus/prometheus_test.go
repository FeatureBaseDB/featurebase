// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package prometheus_test

import (
	"testing"


	"github.com/featurebasedb/featurebase/v3/test"
	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

func TestPrometheusClient_Methods(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	metricFams, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, metricName := range []string{
		"pilosa_sql_statistics_sql_bulk_insert_batches_sec",
		"pilosa_sql_statistics_sql_bulk_inserts_sec",
		"pilosa_sql_statistics_sql_deletes_sec",
		"pilosa_sql_statistics_sql_inserts_sec",
		"pilosa_sql_statistics_sql_requests_sec",
	} {
		if metricExists(metricName, metricFams) {
			continue
		}
		t.Fatalf("metric does not exist: %s", metricName)
	}
}

func metricExists(metricName string, metricFams []*io_prometheus_client.MetricFamily) bool {
	for _, metricFam := range metricFams {
		if metricFam.GetName() == metricName {
			return true
		}
	}
	return false
}
