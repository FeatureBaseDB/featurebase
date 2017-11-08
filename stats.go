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

package pilosa

import (
	"expvar"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

func init() {
	NopStatsClient = &nopStatsClient{}
}

// Expvar global expvar map.
var Expvar = expvar.NewMap("index")

// StatsClient represents a client to a stats server.
type StatsClient interface {
	// Returns a sorted list of tags on the client.
	Tags() []string

	// Returns a new client with additional tags appended.
	WithTags(tags ...string) StatsClient

	// Tracks the number of times something occurs per second.
	Count(name string, value int64, rate float64)

	// Tracks the number of times something occurs per second with custom tags
	CountWithCustomTags(name string, value int64, rate float64, tags []string)

	// Sets the value of a metric.
	Gauge(name string, value float64, rate float64)

	// Tracks statistical distribution of a metric.
	Histogram(name string, value float64, rate float64)

	// Tracks number of unique elements.
	Set(name string, value string, rate float64)

	// Tracks timing information for a metric.
	Timing(name string, value time.Duration, rate float64)

	// SetLogger Set the logger output type
	SetLogger(logger io.Writer)

	// Starts the service
	Open()

	// Closes the client
	Close() error
}

// NopStatsClient represents a client that doesn't do anything.
var NopStatsClient StatsClient

type nopStatsClient struct{}

func (c *nopStatsClient) Tags() []string                                                            { return nil }
func (c *nopStatsClient) WithTags(tags ...string) StatsClient                                       { return c }
func (c *nopStatsClient) Count(name string, value int64, rate float64)                              {}
func (c *nopStatsClient) CountWithCustomTags(name string, value int64, rate float64, tags []string) {}
func (c *nopStatsClient) Gauge(name string, value float64, rate float64)                            {}
func (c *nopStatsClient) Histogram(name string, value float64, rate float64)                        {}
func (c *nopStatsClient) Set(name string, value string, rate float64)                               {}
func (c *nopStatsClient) Timing(name string, value time.Duration, rate float64)                     {}
func (c *nopStatsClient) SetLogger(logger io.Writer)                                                {}
func (c *nopStatsClient) Open()                                                                     {}
func (c *nopStatsClient) Close() error                                                              { return nil }

// ExpvarStatsClient writes stats out to expvars.
type ExpvarStatsClient struct {
	mu   sync.Mutex
	m    *expvar.Map
	tags []string
}

// NewExpvarStatsClient returns a new instance of ExpvarStatsClient.
// This client points at the root of the expvar index map.
func NewExpvarStatsClient() *ExpvarStatsClient {
	return &ExpvarStatsClient{
		m: Expvar,
	}
}

// Tags returns a sorted list of tags on the client.
func (c *ExpvarStatsClient) Tags() []string {
	return nil
}

// WithTags returns a new client with additional tags appended.
func (c *ExpvarStatsClient) WithTags(tags ...string) StatsClient {
	m := &expvar.Map{}
	m.Init()
	c.m.Set(strings.Join(tags, ","), m)

	return &ExpvarStatsClient{
		m:    m,
		tags: UnionStringSlice(c.tags, tags),
	}
}

// Count tracks the number of times something occurs.
func (c *ExpvarStatsClient) Count(name string, value int64, rate float64) {
	c.m.Add(name, value)
}

// CountWithCustomTags Tracks the number of times something occurs per second with custom tags
func (c *ExpvarStatsClient) CountWithCustomTags(name string, value int64, rate float64, tags []string) {
	c.m.Add(name, value)
}

// Gauge sets the value of a metric.
func (c *ExpvarStatsClient) Gauge(name string, value float64, rate float64) {
	var f expvar.Float
	f.Set(value)
	c.m.Set(name, &f)
}

// Histogram tracks statistical distribution of a metric.
// This works the same as gauge for this client.
func (c *ExpvarStatsClient) Histogram(name string, value float64, rate float64) {
	c.Gauge(name, value, rate)
}

// Set tracks number of unique elements.
func (c *ExpvarStatsClient) Set(name string, value string, rate float64) {
	var s expvar.String
	s.Set(value)
	c.m.Set(name, &s)
}

// Timing tracks timing information for a metric.
func (c *ExpvarStatsClient) Timing(name string, value time.Duration, rate float64) {
	c.mu.Lock()
	d, _ := c.m.Get(name).(time.Duration)
	c.m.Set(name, d+value)
	c.mu.Unlock()
}

// SetLogger has no logger.
func (c *ExpvarStatsClient) SetLogger(logger io.Writer) {
}

// Open no-op.
func (c *ExpvarStatsClient) Open() {}

// Close no-op.
func (c *ExpvarStatsClient) Close() error { return nil }

// MultiStatsClient joins multiple stats clients together.
type MultiStatsClient []StatsClient

// Tags returns tags from the first client.
func (a MultiStatsClient) Tags() []string {
	if len(a) > 0 {
		return a[0].Tags()
	}
	return nil
}

// WithTags returns a new set of clients with the additional tags.
func (a MultiStatsClient) WithTags(tags ...string) StatsClient {
	other := make(MultiStatsClient, len(a))
	for i := range a {
		other[i] = a[i].WithTags(tags...)
	}
	return other
}

// Count tracks the number of times something occurs per second on all clients.
func (a MultiStatsClient) Count(name string, value int64, rate float64) {
	for _, c := range a {
		c.Count(name, value, rate)
	}
}

// CountWithCustomTags Tracks the number of times something occurs per second with custom tags
func (a MultiStatsClient) CountWithCustomTags(name string, value int64, rate float64, tags []string) {
	for _, c := range a {
		c.CountWithCustomTags(name, value, rate, tags)
	}
}

// Gauge sets the value of a metric on all clients.
func (a MultiStatsClient) Gauge(name string, value float64, rate float64) {
	for _, c := range a {
		c.Gauge(name, value, rate)
	}
}

// Histogram tracks statistical distribution of a metric on all clients.
func (a MultiStatsClient) Histogram(name string, value float64, rate float64) {
	for _, c := range a {
		c.Histogram(name, value, rate)
	}
}

// Set tracks number of unique elements on all clients.
func (a MultiStatsClient) Set(name string, value string, rate float64) {
	for _, c := range a {
		c.Set(name, value, rate)
	}
}

// Timing tracks timing information for a metric on all clients.
func (a MultiStatsClient) Timing(name string, value time.Duration, rate float64) {
	for _, c := range a {
		c.Timing(name, value, rate)
	}
}

// SetLogger Sets the StatsD logger output type.
func (a MultiStatsClient) SetLogger(logger io.Writer) {
	for _, c := range a {
		c.SetLogger(logger)
	}
}

// Open starts the stat service.
func (a MultiStatsClient) Open() {
	for _, c := range a {
		c.Open()
	}
}

// Close shuts down the stats clients.
func (a MultiStatsClient) Close() error {
	for _, c := range a {
		err := c.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// UnionStringSlice returns a sorted set of tags which combine a & b.
func UnionStringSlice(a, b []string) []string {
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
