package statsd

type NoopClient struct {
	// prefix for statsd name
	prefix string
}

// Close closes the connection and cleans up.
func (s *NoopClient) Close() error {
	return nil
}

// Increments a statsd count type.
// stat is a string name for the metric.
// value is the integer value
// rate is the sample rate (0.0 to 1.0)
func (s *NoopClient) Inc(stat string, value int64, rate float32) error {
	return nil
}

// Decrements a statsd count type.
// stat is a string name for the metric.
// value is the integer value.
// rate is the sample rate (0.0 to 1.0).
func (s *NoopClient) Dec(stat string, value int64, rate float32) error {
	return nil
}

// Submits/Updates a statsd gauge type.
// stat is a string name for the metric.
// value is the integer value.
// rate is the sample rate (0.0 to 1.0).
func (s *NoopClient) Gauge(stat string, value int64, rate float32) error {
	return nil
}

// Submits a delta to a statsd gauge.
// stat is the string name for the metric.
// value is the (positive or negative) change.
// rate is the sample rate (0.0 to 1.0).
func (s *NoopClient) GaugeDelta(stat string, value int64, rate float32) error {
	return nil
}

// Submits a statsd timing type.
// stat is a string name for the metric.
// value is the integer value.
// rate is the sample rate (0.0 to 1.0).
func (s *NoopClient) Timing(stat string, delta int64, rate float32) error {
	return nil
}

// Raw formats the statsd event data, handles sampling, prepares it,
// and sends it to the server.
// stat is the string name for the metric.
// value is the preformatted "raw" value string.
// rate is the sample rate (0.0 to 1.0).
func (s *NoopClient) Raw(stat string, value string, rate float32) error {
	return nil
}

// Sets/Updates the statsd client prefix
func (s *NoopClient) SetPrefix(prefix string) {
	s.prefix = prefix
}

// Returns a pointer to a new NoopClient, and an error (always nil, just
// supplied to support api convention).
// Use variadic arguments to support identical format as New, or a more
// conventional no argument form.
func NewNoop(a ...interface{}) (*NoopClient, error) {
	noopClient := &NoopClient{}
	return noopClient, nil
}
