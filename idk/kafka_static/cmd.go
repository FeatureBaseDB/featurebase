package kafka_static

import (
	"time"

	"github.com/molecula/featurebase/v3/idk"
	"github.com/pkg/errors"
)

type Main struct {
	idk.Main           `flag:"!embed"`
	KafkaHosts         []string      `help:"Comma separated list of host:port pairs for Kafka."`
	Group              string        `help:"Kafka group."`
	Topics             []string      `help:"Kafka topics to read from."`
	Timeout            time.Duration `help:"Time to wait for more records from Kafka before flushing a batch. 0 to disable."`
	SkipOld            bool          `short:"" help:"Skip to the most recent Kafka message rather than starting at the beginning."`
	Header             string        `help:"Path to the static schema, in JSON header format. May be a path on the local filesystem, or an S3 URI (requires setting --s3-region or environment variable AWS_REGION)."`
	S3Region           string        `help:"S3 Region, optionally used when header is specified as an S3 URI. Alternatively, use environment variable AWS_REGION."`
	AllowMissingFields bool          `help:"Will proceed with ingest even if a field is missing from a record but specified in the JSON config file. Default false"`

	KafkaTLS idk.TLSConfig
}

func NewMain() *Main {
	m := &Main{
		Main:       *idk.NewMain(),
		KafkaHosts: []string{"localhost:9092"},
		Group:      "defaultgroup",
		Topics:     []string{"defaulttopic"},
		Timeout:    time.Second,
	}
	m.OffsetMode = true
	m.Main.Namespace = "ingester_kafka_static"
	m.Main.Pprof = "" // don't initialize pprof until we actually use it in tests
	m.NewSource = func() (idk.Source, error) {
		source := NewSource()
		source.Hosts = m.KafkaHosts
		source.Group = m.Group
		source.Topics = m.Topics
		source.Log = m.Main.Log()
		source.TLS = m.KafkaTLS
		source.Timeout = m.Timeout
		source.SkipOld = m.SkipOld
		source.Header = m.Header
		source.S3Region = m.S3Region
		source.AllowMissingFields = m.AllowMissingFields

		err := source.Open()
		if err != nil {
			return nil, errors.Wrap(err, "opening source")
		}
		return source, nil
	}
	return m
}
