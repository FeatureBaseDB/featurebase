package kafka

import (
	"time"

	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/pkg/errors"
)

type Main struct {
	idk.Main             `flag:"!embed"`
	idk.ConfluentCommand `flag:"!embed"`
	Group                string        `help:"Kafka group."`
	Topics               []string      `help:"Kafka topics to read from."`
	Timeout              time.Duration `help:"Time to wait for more records from Kafka before flushing a batch. 0 to disable."`
	SkipOld              bool          `short:"" help:"False sets kafka consumer configuration auto.offset.reset to earliest, True sets it to latest."`
	ConsumerCloseTimeout time.Duration `help:"The amount of time in seconds to wait for the consumer to close properly."`
}

func NewMain() (*Main, error) {
	m := &Main{
		Main: *idk.NewMain(),
		ConfluentCommand: idk.ConfluentCommand{
			KafkaBootstrapServers: []string{"localhost:9092"},
		},
		Group:                "defaultgroup",
		Topics:               []string{"defaulttopic"},
		Timeout:              time.Second,
		ConsumerCloseTimeout: 30,
	}

	m.SchemaRegistryURL = "http://" + defaultRegistryHost
	m.Main.Namespace = "ingester_kafka"
	//m.Main.OffsetMode = m.OffsetMode
	m.OffsetMode = true
	m.NewSource = func() (idk.Source, error) {
		source := NewSource()
		source.KafkaBootstrapServers = m.KafkaBootstrapServers
		source.SchemaRegistryURL = m.SchemaRegistryURL
		source.Group = m.Group
		source.Topics = m.Topics
		source.Log = m.Main.Log()
		source.Timeout = m.Timeout
		source.KafkaSocketTimeoutMs = int(m.Timeout / time.Millisecond)
		source.SkipOld = m.SkipOld
		source.ConfluentCommand = m.ConfluentCommand
		source.SchemaRegistryUsername = m.SchemaRegistryUsername
		source.SchemaRegistryPassword = m.SchemaRegistryPassword
		source.Verbose = m.Verbose
		source.KafkaMaxPollInterval = m.KafkaMaxPollInterval
		source.KafkaSessionTimeout = m.KafkaSessionTimeout
		source.KafkaGroupInstanceId = m.KafkaGroupInstanceId
		source.KafkaDebug = m.KafkaDebug
		source.KafkaSocketKeepaliveEnable = m.KafkaSocketKeepaliveEnable
		source.consumerCloseTimeout = m.ConsumerCloseTimeout

		if err := source.Open(); err != nil {
			return nil, errors.Wrap(err, "opening source")
		}
		return source, nil
	}
	return m, nil
}
