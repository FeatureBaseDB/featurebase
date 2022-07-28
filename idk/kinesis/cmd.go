package kinesis

import (
	"time"

	"github.com/molecula/featurebase/v3/idk"
	"github.com/pkg/errors"
)

type Main struct {
	idk.Main           `flag:"!embed"`
	Timeout            time.Duration `help:"Time to wait for more records from Kinesis before flushing a batch. 0 to disable."`
	Header             string        `help:"Path to the static schema, in JSON header format. May be a path on the local filesystem, or an S3 URI."`
	AWSRegion          string        `help:"AWS Region. Alternatively, use environment variable AWS_REGION."`
	AllowMissingFields bool          `help:"Will proceed with ingest even if a field is missing from a record but specified in the JSON config file. Default false"`
	StreamName         string        `help:"Name of AWS Kinesis stream to consume records from."`
	OffsetsPath        string        `help:"Path where the offsets file will be written. May be a path on the local filesystem, or an S3 URI."`
	AWSProfile         string        `help:"Name of AWS profile to use. Alternatively, use environment variable AWS_PROFILE."`
}

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
		source.Log = m.Main.Log()
		source.Timeout = m.Timeout
		source.Header = m.Header
		source.AWSRegion = m.AWSRegion
		source.AllowMissingFields = m.AllowMissingFields
		source.StreamName = m.StreamName
		source.OffsetsPath = m.OffsetsPath
		source.AWSProfile = m.AWSProfile

		err := source.Open()
		if err != nil {
			return nil, errors.Wrap(err, "opening source")
		}
		return source, nil
	}
	return m
}
