package datadog

import (
	"io"
	"io/ioutil"
	"log"
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

	LogOutput io.Writer
}

// NewStatsClient returns a new instance of StatsClient.
func NewStatsClient() (*StatsClient, error) {
	c, err := statsd.NewBuffered("127.0.0.1:8125", BufferLen)
	if err != nil {
		return nil, err
	}

	return &StatsClient{
		client:    c,
		LogOutput: ioutil.Discard,
	}, nil
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
		tags:   pilosa.UnionStringSlice(c.tags, tags),
	}
}

// Count tracks the number of times something occurs per second.
func (c *StatsClient) Count(name string, value int64) {
	if err := c.client.Count(name, value, c.tags, Rate); err != nil {
		c.logger.Printf("datadog.StatsClient.Count error: %s", err)
	}
}

// Gauge sets the value of a metric.
func (c *StatsClient) Gauge(name string, value float64) {
	if err := c.client.Gauge(name, value, c.tags, Rate); err != nil {
		c.logger.Printf("datadog.StatsClient.Gauge error: %s", err)
	}
}

// Histogram tracks statistical distribution of a metric.
func (c *StatsClient) Histogram(name string, value float64) error {
	if err := c.client.Histogram(name, value, c.tags, Rate); err != nil {
		c.logger.Printf("datadog.StatsClient.Histogram error: %s", err)
	}
}

// Set tracks number of unique elements.
func (c *StatsClient) Set(name string, value string) error {
	if err := c.client.Set(name, value, c.tags, Rate); err != nil {
		c.logger.Printf("datadog.StatsClient.Set error: %s", err)
	}
}

// Timing tracks timing information for a metric.
func (c *StatsClient) Timing(name string, value time.Duration) error {
	if err := c.client.Timing(name, value, c.tags, Rate); err != nil {
		c.logger.Printf("datadog.StatsClient.Timing error: %s", err)
	}
}

// logger returns a logger that writes to LogOutput
func (c *StatsClient) logger() *log.Logger {
	return log.New(c.LogOutput, "", log.LstdFlags)
}
