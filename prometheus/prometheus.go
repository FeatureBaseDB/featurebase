// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package prometheus

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/featurebasedb/featurebase/v3/stats"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// namespace is prepended to each metric event name with "_"
	defaultNamespace = "general"
)

// Ensure client implements interface.
var _ stats.StatsClient = &prometheusClient{}

// Module-level mutex to avoid copying in WithTags()
var mu sync.Mutex

// prometheusClient represents a Prometheus implementation of pilosa.statsClient.
type prometheusClient struct {
	tags        []string
	logger      logger.Logger
	counters    map[string]prometheus.Counter
	counterVecs map[string]*prometheus.CounterVec
	gauges      map[string]prometheus.Gauge
	gaugeVecs   map[string]*prometheus.GaugeVec
	observers   map[string]prometheus.Observer
	summaryVecs map[string]*prometheus.SummaryVec
	namespace   string
}

// ClientOption is a functional option type for prometheusClient
type ClientOption func(c *prometheusClient)

// OptClientPrefix is a functional option on prometheusClient used to set the namespace
func OptClientNamespace(namespace string) ClientOption {
	return func(c *prometheusClient) {
		c.namespace = namespace
	}
}

// NewPrometheusClient returns a new instance of StatsClient.
func NewPrometheusClient(opts ...ClientOption) (*prometheusClient, error) {
	client := &prometheusClient{
		logger:      logger.NopLogger,
		counters:    make(map[string]prometheus.Counter),
		counterVecs: make(map[string]*prometheus.CounterVec),
		gauges:      make(map[string]prometheus.Gauge),
		gaugeVecs:   make(map[string]*prometheus.GaugeVec),
		observers:   make(map[string]prometheus.Observer),
		summaryVecs: make(map[string]*prometheus.SummaryVec),
		namespace:   defaultNamespace,
	}

	for _, opt := range opts {
		opt(client)
	}

	return client, nil
}

// Open no-op to satisfy interface
func (c *prometheusClient) Open() {}

// Close no-op to satisfy interface
func (c *prometheusClient) Close() error {
	return nil
}

// Tags returns a sorted list of tags on the client.
func (c *prometheusClient) Tags() []string {
	return c.tags
}

// labels returns an instance of prometheus.Labels with the value of the set tags.
func (c *prometheusClient) labels() prometheus.Labels {
	return tagsToLabels(c.tags, c.logger)
}

// WithTags returns a new client with additional tags appended.
func (c *prometheusClient) WithTags(tags ...string) stats.StatsClient {
	return &prometheusClient{
		tags:        unionStringSlice(c.tags, tags),
		logger:      c.logger,
		counters:    c.counters,
		counterVecs: c.counterVecs,
		gauges:      c.gauges,
		gaugeVecs:   c.gaugeVecs,
		observers:   c.observers,
		summaryVecs: c.summaryVecs,
		namespace:   c.namespace,
	}
}

// Count tracks the number of times something occurs per second.
func (c *prometheusClient) Count(name string, value int64, rate float64) {
	mu.Lock()
	defer mu.Unlock()

	var counter prometheus.Counter
	var ok bool
	name = strings.Replace(name, ".", "_", -1)
	labels := c.labels()
	opts := prometheus.CounterOpts{
		Namespace: c.namespace,
		Name:      name,
	}
	if len(labels) == 0 {
		counter, ok = c.counters[name]
		if !ok {
			counter = prometheus.NewCounter(opts)
			c.counters[name] = counter
			prometheus.MustRegister(counter)
		}
	} else {
		var counterVec *prometheus.CounterVec
		counterVec, ok = c.counterVecs[name]
		if !ok {
			counterVec = prometheus.NewCounterVec(
				opts,
				labelKeys(labels),
			)
			c.counterVecs[name] = counterVec
			prometheus.MustRegister(counterVec)
		}
		var err error
		counter, err = counterVec.GetMetricWith(labels)
		if err != nil {
			c.logger.Errorf("counterVec.GetMetricWith error: %s", err)
		}
	}
	if value == 1 {
		counter.Inc()
	} else {
		counter.Add(float64(value))
	}
}

// CountWithCustomTags tracks the number of times something occurs per second with custom tags.
func (c *prometheusClient) CountWithCustomTags(name string, value int64, rate float64, t []string) {
	c.WithTags(append(c.tags, t...)...).Count(name, value, rate)
}

// Gauge sets the value of a metric.
func (c *prometheusClient) Gauge(name string, value float64, rate float64) {
	mu.Lock()
	defer mu.Unlock()

	var gauge prometheus.Gauge
	var ok bool
	name = strings.Replace(name, ".", "_", -1)
	labels := c.labels()
	opts := prometheus.GaugeOpts{
		Namespace: c.namespace,
		Name:      name,
	}
	if len(labels) == 0 {
		gauge, ok = c.gauges[name]
		if !ok {
			gauge = prometheus.NewGauge(opts)
			c.gauges[name] = gauge
			prometheus.MustRegister(gauge)
		}
	} else {
		var gaugeVec *prometheus.GaugeVec
		gaugeVec, ok = c.gaugeVecs[name]
		if !ok {
			gaugeVec = prometheus.NewGaugeVec(
				opts,
				labelKeys(labels),
			)
			c.gaugeVecs[name] = gaugeVec
			prometheus.MustRegister(gaugeVec)
		}
		var err error
		gauge, err = gaugeVec.GetMetricWith(labels)
		if err != nil {
			c.logger.Errorf("gaugeVec.GetMetricWith error: %s", err)
			return
		}
	}
	gauge.Set(float64(value))
}

// Histogram tracks statistical distribution of a metric.
func (c *prometheusClient) Histogram(name string, value float64, rate float64) {
	mu.Lock()
	defer mu.Unlock()

	var observer prometheus.Observer
	var ok bool
	name = strings.Replace(name, ".", "_", -1)
	labels := c.labels()
	opts := prometheus.SummaryOpts{
		Namespace:  c.namespace,
		Name:       name,
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	}
	if len(labels) == 0 {
		observer, ok = c.observers[name]
		if !ok {
			summary := prometheus.NewSummary(opts)
			observer = summary
			c.observers[name] = observer
			prometheus.MustRegister(summary)
		}
	} else {
		var summaryVec *prometheus.SummaryVec
		summaryVec, ok = c.summaryVecs[name]
		if !ok {
			summaryVec = prometheus.NewSummaryVec(
				opts,
				labelKeys(labels),
			)
			c.summaryVecs[name] = summaryVec
			prometheus.MustRegister(summaryVec)
		}
		var err error
		observer, err = summaryVec.GetMetricWith(labels)
		if err != nil {
			c.logger.Errorf("summaryVec.GetMetricWith error: %s", err)
			return
		}
	}
	observer.Observe(value)
}

// Set tracks number of unique elements.
func (c *prometheusClient) Set(name string, value string, rate float64) {
	c.logger.Infof("prometheusClient.Set unimplemented: %s=%s", name, value)
}

// Timing tracks timing information for a metric.
func (c *prometheusClient) Timing(name string, value time.Duration, rate float64) {
	c.Histogram(name, value.Seconds(), rate)
}

// SetLogger sets the logger for client.
func (c *prometheusClient) SetLogger(logger logger.Logger) {
	c.logger = logger
}

// unionStringSlice returns a sorted set of tags which combine a & b.
func unionStringSlice(a, b []string) []string {
	// Sort both sets first.
	sort.Strings(a)
	sort.Strings(b)

	// Find size of largest slice.
	n := len(a)
	if len(b) > n {
		n = len(b)
	}

	// Exit if both sets are empty.
	if n == 0 {
		return nil
	}

	// Iterate over both in order and merge.
	other := make([]string, 0, n)
	for len(a) > 0 || len(b) > 0 {
		if len(a) == 0 {
			other, b = append(other, b[0]), b[1:]
		} else if len(b) == 0 {
			other, a = append(other, a[0]), a[1:]
		} else if a[0] < b[0] {
			other, a = append(other, a[0]), a[1:]
		} else if b[0] < a[0] {
			other, b = append(other, b[0]), b[1:]
		} else {
			other, a, b = append(other, a[0]), a[1:], b[1:]
		}
	}
	return other
}

func tagsToLabels(tags []string, logger logger.Logger) (labels prometheus.Labels) {
	labels = make(prometheus.Labels)
	for _, tag := range tags {
		tagParts := strings.SplitAfterN(tag, ":", 2)
		if len(tagParts) != 2 {
			// only process tags in "key:value" form
			logger.Errorf("invalid Prometheus label: %v\n", tag)
			continue
		}
		labels[tagParts[0][0:len(tagParts[0])-1]] = tagParts[1]
	}
	return labels
}

func labelKeys(labels prometheus.Labels) (keys []string) {
	keys = make([]string, len(labels))
	i := 0
	for k := range labels {
		keys[i] = k
		i++
	}
	return keys
}
