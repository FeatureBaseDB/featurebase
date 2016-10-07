package pilosa

import "time"

// StatsClient represents a client to a stats server.
type StatsClient interface {
	// Returns a sorted list of tags on the client.
	Tags() []string

	// Returns a new client with additional tags appended.
	WithTags(tags ...string) StatsClient

	// Tracks the number of times something occurs per second.
	Count(name string, value int64) error

	// Sets the value of a metric.
	Gauge(name string, value float64) error

	// Tracks statistical distribution of a metric.
	Histogram(name string, value float64) error

	// Tracks number of unique elements.
	Set(name string, value string) error

	// Tracks timing information for a metric.
	Timing(name string, value time.Duration) error
}

// NopStatsClient represents a client that doesn't do anything.
type NopStatsClient struct{}

func (c *NopStatsClient) Tags() []string                                { return nil }
func (c *NopStatsClient) WithTags(tags ...string) StatsClient           { return c }
func (c *NopStatsClient) Count(name string, value int64) error          { return nil }
func (c *NopStatsClient) Gauge(name string, value float64) error        { return nil }
func (c *NopStatsClient) Histogram(name string, value float64) error    { return nil }
func (c *NopStatsClient) Set(name string, value string) error           { return nil }
func (c *NopStatsClient) Timing(name string, value time.Duration) error { return nil }
