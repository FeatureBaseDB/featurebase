package datadog

import (
	"sort"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/umbel/pilosa"
)

const (
	// Rate represents a metric rate of 1/sec.
	Rate = 1

	// Client buffer size.
	BufferLen = 256
)

// Ensure client implements interface.
var _ pilosa.StatsClient = &StatsClient{}

// StatsClient represents a DataDog implementation of pilosa.StatsClient.
type StatsClient struct {
	client *statsd.Client
	tags   []string
}

// NewStatsClient returns a new instance of StatsClient.
func NewStatsClient() (*StatsClient, error) {
	c, err := statsd.NewBuffered("127.0.0.1:8125", BufferLen)
	if err != nil {
		return nil, err
	}
	return &StatsClient{client: c}, nil
}

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
		client: c.client,
		tags:   unionTags(c.tags, tags),
	}
}

// Count tracks the number of times something occurs per second.
func (c *StatsClient) Count(name string, value int64) error {
	return c.client.Count(name, value, c.tags, Rate)
}

// Gauge sets the value of a metric.
func (c *StatsClient) Gauge(name string, value float64) error {
	return c.client.Gauge(name, value, c.tags, Rate)
}

// Histogram tracks statistical distribution of a metric.
func (c *StatsClient) Histogram(name string, value float64) error {
	return c.client.Histogram(name, value, c.tags, Rate)
}

// Set tracks number of unique elements.
func (c *StatsClient) Set(name string, value string) error {
	return c.client.Set(name, value, c.tags, Rate)
}

// Timing tracks timing information for a metric.
func (c *StatsClient) Timing(name string, value time.Duration) error {
	return c.client.Timing(name, value, c.tags, Rate)
}

// unionTags returns a sorted set of tags which combine a & b.
func unionTags(a, b []string) []string {
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
