package kafka

import (
	"io"
	"time"

	fbbatch "github.com/featurebasedb/featurebase/v3/batch"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/featurebasedb/featurebase/v3/idk/common"
	"github.com/featurebasedb/featurebase/v3/idk/kafka"
	"github.com/featurebasedb/featurebase/v3/idk/kafka_sasl"
	"github.com/featurebasedb/featurebase/v3/logger"
)

// Runner is a CLI-specific kafka consumer. It's similar to
// idk.kafka_static.Main in that it embeds idk.Main and contains additional
// functionality specific to its use case.
type Runner struct {
	idk.Main             `flag:"!embed"`
	idk.ConfluentCommand `flag:"!embed"`
	Hosts                []string       `help:"Comma separated list of host:port pairs for Kafka."`
	Group                string         `help:"Kafka group."`
	Topics               []string       `help:"Kafka topics to read from."`
	Timeout              time.Duration  `help:"Time to wait for more records from Kafka before flushing a batch. 0 to disable."`
	Header               []idk.RawField `help:"Header configuration."`
}

// Configure and return *runner with common elements of all kafka runners
func NewRunner(cfg ConfigForIDK, batcher fbbatch.Batcher, logWriter io.Writer) *Runner {
	idkMain := idk.NewMain()
	idkMain.IDField = cfg.IDField
	idkMain.PrimaryKeyFields = cfg.PrimaryKeys
	idkMain.Index = cfg.Table
	idkMain.Batcher = batcher
	idkMain.BatchSize = cfg.BatchSize
	idkMain.BatchMaxStaleness = cfg.BatchMaxStaleness
	idkMain.MaxMsgs = uint64(cfg.MaxMessages)
	idkMain.SetBasic()
	idkMain.SetLog(logger.NewStandardLogger(logWriter))
	idkMain.OffsetMode = true
	idkMain.Namespace = "sql_kafka_runner"
	idkMain.Pprof = "" // don't initialize pprof until we actually use it in tests
	kr := &Runner{
		Main:    *idkMain,
		Hosts:   cfg.Hosts,
		Group:   cfg.Group,
		Topics:  cfg.Topics,
		Header:  cfg.Fields,
		Timeout: cfg.Timeout,
	}

	// NewSource should be set based on the encoding of the source (e.g. JSON, Avro)
	if cfg.Encode == encodingTypeAvro {
		kr.GetAvroNewSource(cfg)
	} else if cfg.Encode == encodingTypeJSON {
		kr.GetJSONNewSource(cfg)
	}

	return kr
}

func (r *Runner) GetJSONNewSource(cfg ConfigForIDK) {
	r.NewSource = func() (idk.Source, error) {
		source := kafka_sasl.NewSource()
		source.KafkaBootstrapServers = r.Hosts
		source.Group = r.Group
		source.Topics = r.Topics
		source.Log = r.Main.Log()
		source.Timeout = r.Timeout
		source.HeaderFields = r.Header
		source.AllowMissingFields = cfg.AllowMissingFields
		source.KafkaConfiguration = cfg.ConfluentConfig
		confluentCfg, err := common.SetupConfluent(&r.ConfluentCommand)
		if err != nil {
			return nil, err
		}
		source.ConfigMap = confluentCfg

		err = source.Open()
		if err != nil {
			return nil, errors.Wrap(err, "opening source")
		}
		return source, nil
	}
}

func (r *Runner) GetAvroNewSource(cfg ConfigForIDK) {
	r.NewSource = func() (idk.Source, error) {
		source := kafka.NewSource()
		source.KafkaBootstrapServers = r.Hosts
		source.Group = r.Group
		source.Topics = r.Topics
		source.Log = r.Main.Log()
		source.Timeout = r.Timeout
		source.KafkaConfiguration = cfg.ConfluentConfig
		confluentcfg, err := common.SetupConfluent(&r.ConfluentCommand)
		if err != nil {
			return nil, err
		}
		source.ConfigMap = confluentcfg

		err = source.Open()
		if err != nil {
			return nil, errors.Wrap(err, "opening source")
		}
		return source, nil
	}
}
