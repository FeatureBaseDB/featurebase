package main

import (
	"log"
	"os"

	"github.com/jaffee/commandeer/pflag"
	"github.com/molecula/featurebase/v3/idk/kafkagen"
	"github.com/molecula/featurebase/v3/logger"
)

func main() {
	m, err := kafkagen.NewMain()
	if err != nil {

		log.Fatal(err)
	}
	if err := pflag.LoadEnv(m, "KGEN_", nil); err != nil {
		log.Fatal(err)
	}
	if m.DryRun {
		log.Printf("%+v\n", m)
		return
	}

	if err := m.Run(); err != nil {
		logger.NewStandardLogger(os.Stderr).Errorf("Error running command: %v", err)
		os.Exit(1)
	}
}
