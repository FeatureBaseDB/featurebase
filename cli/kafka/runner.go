package kafka

import (
	"io"
	"time"

	fbbatch "github.com/featurebasedb/featurebase/v3/batch"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/featurebasedb/featurebase/v3/idk/kafka_static"
	"github.com/featurebasedb/featurebase/v3/logger"
)

// Runner is a CLI-specific kafka consumer. It's similar to
// idk.kafka_static.Main in that it embeds idk.Main and contains additional
// functionality specific to its use case.
type Runner struct {
	idk.Main   `flag:"!embed"`
	KafkaHosts []string       `help:"Comma separated list of host:port pairs for Kafka."`
	Group      string         `help:"Kafka group."`
	Topics     []string       `help:"Kafka topics to read from."`
	Timeout    time.Duration  `help:"Time to wait for more records from Kafka before flushing a batch. 0 to disable."`
	Header     []idk.RawField `help:"Header configuration."`
}

func NewRunner(cfg ConfigForIDK, batcher fbbatch.Batcher, logWriter io.Writer) *Runner {
	idkMain := idk.NewMain()
	idkMain.IDField = cfg.IDField
	idkMain.Index = cfg.Table
	idkMain.Batcher = batcher
	idkMain.BatchSize = cfg.BatchSize
	idkMain.BatchMaxStaleness = cfg.BatchMaxStaleness
	idkMain.Basic()
	idkMain.SetLog(logger.NewStandardLogger(logWriter))

	kr := &Runner{
		Main:       *idkMain,
		KafkaHosts: cfg.Hosts,
		Group:      cfg.Group,
		Topics:     cfg.Topics,
		Header:     cfg.Fields,
		Timeout:    cfg.Timeout,
	}
	kr.OffsetMode = true
	kr.Main.Namespace = "cli_kafka_runner"
	kr.Main.Pprof = "" // don't initialize pprof until we actually use it in tests
	kr.NewSource = func() (idk.Source, error) {
		source := kafka_static.NewSource()
		source.Hosts = kr.KafkaHosts
		source.Group = kr.Group
		source.Topics = kr.Topics
		source.Log = kr.Main.Log()
		// source.TLS = m.KafkaTLS
		source.Timeout = kr.Timeout
		// source.SkipOld = m.SkipOld
		source.HeaderFields = kr.Header
		// source.S3Region = m.S3Region
		// source.AllowMissingFields = m.AllowMissingFields

		err := source.Open()
		if err != nil {
			return nil, errors.Wrap(err, "opening source")
		}
		return source, nil
	}
	return kr
}
