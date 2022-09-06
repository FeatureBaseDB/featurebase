package kafka_sasl

import (
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/featurebasedb/featurebase/v3/idk/common"
	"github.com/pkg/errors"
)

type Main struct {
	idk.Main             `flag:"!embed"`
	idk.ConfluentCommand `flag:"!embed"`
	Group                string               `help:"Kafka group."`
	Topics               []string             `help:"Kafka topics to read from."`
	Timeout              time.Duration        `help:"Time to wait for more records from Kafka before flushing a batch. 0 to disable."`
	SkipOld              bool                 `short:"" help:"Skip to the most recent Kafka message rather than starting at the beginning."`
	Header               string               `help:"Path to the static schema, in JSON header format."`
	AllowMissingFields   bool                 `help:"Will proceed with ingest even if a field is missing from a record but specified in the JSON config file. Default false"`
	ConfigMap            *confluent.ConfigMap `flag:"-"`
}

func (m *Main) CopyIn(config idk.ConfluentCommand) {
	m.KafkaSaslUsername = config.KafkaSaslUsername
	m.KafkaSaslPassword = config.KafkaSaslPassword
	m.KafkaSaslMechanism = config.KafkaSaslMechanism
	m.KafkaSecurityProtocol = config.KafkaSecurityProtocol
	m.KafkaSslKeyPassword = config.KafkaSslKeyPassword
	m.KafkaSslCaLocation = config.KafkaSslCaLocation
	m.KafkaSslCertificateLocation = config.KafkaSslCertificateLocation
	m.KafkaSslKeyLocation = config.KafkaSslKeyLocation
}

func NewMain() (*Main, error) {
	var err error
	m := Main{}
	m.Main = *idk.NewMain()
	m.ConfluentCommand = idk.ConfluentCommand{}
	m.KafkaBootstrapServers = []string{"localhost:9092"}
	m.Group = "defaultgroup"
	m.Topics = []string{"defaulttopic"}
	m.Timeout = time.Second
	m.OffsetMode = true
	m.Main.Namespace = "ingester_kafka_confluent"
	m.Main.Pprof = "" // don't initialize pprof until we actually use it in tests
	if err != nil {
		return nil, err
	}
	m.NewSource = func() (idk.Source, error) {
		source := NewSource()
		source.Group = m.Group
		source.Topics = m.Topics
		source.Log = m.Main.Log()
		source.Timeout = m.Timeout
		source.SkipOld = m.SkipOld
		source.Header = m.Header
		source.AllowMissingFields = m.AllowMissingFields
		cfg, err := common.SetupConfluent(&m.ConfluentCommand)
		if err != nil {
			return nil, err
		}

		source.ConfigMap = cfg
		err = source.Open()
		if err != nil {
			return nil, errors.Wrap(err, "opening source")
		}
		return source, nil
	}
	return &m, nil
}
