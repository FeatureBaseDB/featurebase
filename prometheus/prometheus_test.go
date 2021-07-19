// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prometheus_test

import (
	"reflect"
	"testing"
	"time"

	pilosaPrometheus "github.com/molecula/featurebase/v2/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

func TestPrometheusClient_WithTags(t *testing.T) {
	// Create a new client.
	c, err := pilosaPrometheus.NewPrometheusClient()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Create a new client with additional tags.
	c1 := c.WithTags("foo", "bar")
	if tags := c1.Tags(); !reflect.DeepEqual(tags, []string{"bar", "foo"}) {
		t.Fatalf("unexpected tags: %+v", tags)
	}

	// Create a new client from the clone with more tags.
	c2 := c1.WithTags("bar", "baz")
	if tags := c2.Tags(); !reflect.DeepEqual(tags, []string{"bar", "baz", "foo"}) {
		t.Fatalf("unexpected tags: %+v", tags)
	}
}

func TestPrometheusClient_Methods(t *testing.T) {
	// Create a new client.
	c, err := pilosaPrometheus.NewPrometheusClient(
		pilosaPrometheus.OptClientNamespace("testns"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	dur, _ := time.ParseDuration("123us")
	c.CountWithCustomTags("ct", 1, 1.0, []string{"foo:bar"})
	c.Count("cc", 1, 1.0)
	c.Gauge("gg", 10, 1.0)
	c.Histogram("hh", 1, 1.0)
	c.Timing("tt", dur, 1.0)

	metricFams, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, metricName := range []string{"testns_ct", "testns_cc", "testns_gg", "testns_hh", "testns_tt"} {
		if metricExists(metricName, metricFams) {
			continue
		}
		t.Fatalf("Metric was not recorded: %s", metricName)
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
