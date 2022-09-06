package main

import (
	"log"
	"os"

	"github.com/jaffee/commandeer/pflag"
	"github.com/featurebasedb/featurebase/v3/idk/kafka"
	"github.com/featurebasedb/featurebase/v3/logger"
)

func main() {
	m, err := kafka.NewPutCmd()
	if err != nil {
		log.Fatal(err)
	}
	if err := pflag.LoadEnv(m, "KPUT_", nil); err != nil {
		log.Fatal(err)
	}
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
