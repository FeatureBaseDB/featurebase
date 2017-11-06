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

package statsd

import (
	"io"
	"io/ioutil"
	"log"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/pilosa/pilosa"
)

// StatsD protocal wrapper using the DataDog library that added Tags to the StatsD protocal
// statsD defailt host is "127.0.0.1:8125"

const (
	// Prefix is appended to each metric event name
	Prefix = "pilosa."

	// BufferLen Stats lient buffer size.
	BufferLen = 1024
)

// Ensure client implements interface.
var _ pilosa.StatsClient = &StatsClient{}

// StatsClient represents a StatsD implementation of pilosa.StatsClient.
type StatsClient struct {
	client    *statsd.Client
	tags      []string
	logOutput io.Writer
}

// NewStatsClient returns a new instance of StatsClient.
func NewStatsClient(host string) (*StatsClient, error) {
	c, err := statsd.NewBuffered(host, BufferLen)
	if err != nil {
		return nil, err
	}

	return &StatsClient{
		client:    c,
		logOutput: ioutil.Discard,
	}, nil
}

// Open no-op
func (c *StatsClient) Open() {}

// Close closes the connection to the agent.
func (c *StatsClient) Close() error {
	return c.client.Close()
}

// Tags returns a sorted list of tags on the client.
func (c *StatsClient) Tags() []string {
	return c.tags
}

// WithTags returns a new client with additional tags appended.
func (c *StatsClient) WithTags(tags ...string) pilosa.StatsClient {
	return &StatsClient{
		client:    c.client,
		tags:      pilosa.UnionStringSlice(c.tags, tags),
		logOutput: c.logOutput,
	}
}

// Count tracks the number of times something occurs per second.
func (c *StatsClient) Count(name string, value int64, rate float64) {
	if err := c.client.Count(Prefix+name, value, c.tags, rate); err != nil {
		c.logger().Printf("statsd.StatsClient.Count error: %s", err)
	}
}

// CountWithCustomTags tracks the number of times something occurs per second with custom tags.
func (c *StatsClient) CountWithCustomTags(name string, value int64, rate float64, t []string) {
	tags := append(c.tags, t...)
	if err := c.client.Count(Prefix+name, value, tags, rate); err != nil {
		c.logger().Printf("statsd.StatsClient.Count error: %s", err)
	}
}

// Gauge sets the value of a metric.
func (c *StatsClient) Gauge(name string, value float64, rate float64) {
	if err := c.client.Gauge(Prefix+name, value, c.tags, rate); err != nil {
		c.logger().Printf("statsd.StatsClient.Gauge error: %s", err)
	}
}

// Histogram tracks statistical distribution of a metric.
func (c *StatsClient) Histogram(name string, value float64, rate float64) {
	if err := c.client.Histogram(Prefix+name, value, c.tags, rate); err != nil {
		c.logger().Printf("statsd.StatsClient.Histogram error: %s", err)
	}
}

// Set tracks number of unique elements.
func (c *StatsClient) Set(name string, value string, rate float64) {
	if err := c.client.Set(Prefix+name, value, c.tags, rate); err != nil {
		c.logger().Printf("statsd.StatsClient.Set error: %s", err)
	}
}

// Timing tracks timing information for a metric.
func (c *StatsClient) Timing(name string, value time.Duration, rate float64) {
	if err := c.client.Timing(Prefix+name, value, c.tags, rate); err != nil {
		c.logger().Printf("statsd.StatsClient.Timing error: %s", err)
	}
}

// SetLogger has no logger
func (c *StatsClient) SetLogger(logger io.Writer) {
	c.logOutput = logger
}

// logger returns a logger that writes to LogOutput
func (c *StatsClient) logger() *log.Logger {
	return log.New(c.logOutput, "", log.LstdFlags)
}
