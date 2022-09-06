package main

import (
	"fmt"
	"log"
	"os"

	"github.com/jaffee/commandeer/pflag"
	"github.com/featurebasedb/featurebase/v3/idk/csv"
	"github.com/featurebasedb/featurebase/v3/logger"
)

func main() {
	m := csv.NewMain()
	if err := pflag.LoadEnv(m, "IDKCSV_", nil); err != nil {
		log.Fatal(err)
	}
	m.Rename()
	if m.DryRun {
		fields, err := m.ValidateHeaders()
		if err != nil {
			log.Printf("validation error: %v\n", err)
			return
		}
		fmt.Printf("%+v\n", m)
		fmt.Printf("Parsed fields:\n")
		for _, f := range fields {
			fmt.Printf("  %-20T %+[1]v\n", f)
		}
		return
	}

	if m.Concurrency != 1 {
		m.Log().Infof("Concurrency is not supported for csv ingest. '--concurrency' flag will be ignored.")
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
