package kinesis

import (
	"time"

	"github.com/pkg/errors"

	"github.com/molecula/featurebase/v3/idk"
)

// Main is the holder of all configurations for a Kinesis stream consumer.
//
// Along with the additional configuration fields, kinesis.Main also gains all
// fields and methods from idk.Main via composition.
type Main struct {
	idk.Main           `flag:"!embed"`
	Timeout            time.Duration `help:"Time to wait for more records from Kinesis before flushing a batch. 0 to disable."`
	Header             string        `help:"Path to the static schema, in JSON header format. May be a path on the local filesystem, or an S3 URI."`
	AWSRegion          string        `help:"AWS Region. Alternatively, use environment variable AWS_REGION."`
	AllowMissingFields bool          `help:"Will proceed with ingest even if a field is missing from a record but specified in the JSON config file. Default false"`
	StreamName         string        `help:"Name of AWS Kinesis stream to consume records from."`
	OffsetsPath        string        `help:"Path where the offsets file will be written. May be a path on the local filesystem, or an S3 URI."`
	AWSProfile         string        `help:"Name of AWS profile to use. Alternatively, use environment variable AWS_PROFILE."`
	ErrorQueueName     string        `help:"SQS queue name to send error and panic/runtime errors to."`
}

// NewMain returns a new instance of a Kinesis stream consumer configuration object.
//
// It specifies a callback NewSource that can be invoked to create a kinesis.Source object.
// This callback implicitly initializes an AWS session and uses that session to initialize
// clients to the following AWS resources: S3, Kinesis, and SQS. Client creation happens regardless
// of configuration. (ex: OffsetsPath and Header are local paths -> S3 client is created.)
//
// The default BatchSize is 20000 and Concurrency is 1. Any Concurrency value > 1 is NOT supported.
// These values are set on the returned kinesis.Main instance.
//
// The Logger instance on the kinesis.Source is always decorated when NewSource is invoked.
// Assuming no errors occur during AWS client initialization, the decorated Logger instance is
// propagated back to the kinesis.Main so that callers that configured it can also emit errors and
// panics to the SQS queue specified by ErrorQueueName. The behavior of the wrapped Logger depends on
// a non-empty ErrorQueueName, the existence of an SQS queue instance in AWSRegion with that name
// the StreamName field being of a particular format 'PREFIX'-VALID_UUID, and if a valid SQS queue URL
// can be resolved at the time of Logger initialization. If any of these are false, the error
// emission to an SQS queue functionality is not activated and the Logger instance behaves identically
// to its wrapped Logger and emits a warning to the caller that errors are not propagated to SQS.
func NewMain() *Main {
	m := &Main{
		Main:    *idk.NewMain(),
		Timeout: time.Second,
	}
	m.Concurrency = 1 // only a concurrency of 1 is supported for the Kinesis IDK ingester
	m.BatchSize = 20000
	m.OffsetMode = true
	m.Main.Namespace = "ingester_kinesis"
	m.Main.Pprof = "" // don't initialize pprof until we actually use it in tests

	m.NewSource = func() (idk.Source, error) {
		source := NewSource()
		source.Timeout = m.Timeout
		source.Header = m.Header
		source.AWSRegion = m.AWSRegion
		source.AllowMissingFields = m.AllowMissingFields
		source.StreamName = m.StreamName
		source.OffsetsPath = m.OffsetsPath
		source.AWSProfile = m.AWSProfile

		// This Logger instance is wrapped in `Open` -> `initAWS`.
		source.Log = m.Main.Log()
		source.ErrorQueueName = m.ErrorQueueName

		err := source.Open()
		if err != nil {
			return nil, errors.Wrap(err, "opening source")
		}

		// `Open` succeeded -> AWS resources successfully initialized ->
		// Logger instance was successfully wrapped. Now assign the wrapped
		// Logger instance back to main so executables invoking this
		// (ex: `molecula-consumer-kinesis`) will propagate errors and
		// panics on failure using the wrapped Logger instance.
		m.Main.SetLog(source.Log)
		return source, nil
	}
	return m
}
