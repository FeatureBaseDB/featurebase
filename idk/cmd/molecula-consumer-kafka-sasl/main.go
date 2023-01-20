package main

import (
	"log"
	"os"

	"github.com/featurebasedb/featurebase/v3/idk/kafka_sasl"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/jaffee/commandeer/pflag"
)

func main() {
	m, err := kafka_sasl.NewMain()
	if err != nil {
		log.Fatal(err)
	}
	if err := pflag.LoadEnv(m, "CONSUMER_", nil); err != nil {
		log.Fatal(err)
	}
	m.Rename()
	if m.DryRun {
		log.Printf("%+v\n", m)
		return
	}

	if err := m.Run(); err != nil {
		log := m.Log()
		if log == nil {
			// if we fail before a logger was instantiated
			logger.NewStandardLogger(os.Stderr).Errorf("Error running command: %v", err)
			os.Exit(1)
		}
		log.Errorf("Error running command: %v", err)
		os.Exit(1)
	}
}
