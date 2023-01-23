package main

import (
	"log"
	"os"

	"github.com/featurebasedb/featurebase/v3/idk/sql"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/jaffee/commandeer/pflag"
)

func main() {
	m := sql.NewMain()
	if err := pflag.LoadEnv(m, "IDK_", nil); err != nil {
		log.Fatal(err)
	}
	m.Rename()
	if m.DryRun {
		log.Printf("%+v\n", m)
		return
	}

	if m.Concurrency != 1 {
		m.Log().Infof("Concurrency is not supported for sql ingest. '--concurrency' flag will be ignored.")
		m.Concurrency = 1
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
