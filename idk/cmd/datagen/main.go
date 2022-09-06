package main

import (
	"log"
	"os"

	"github.com/jaffee/commandeer/pflag"
	"github.com/featurebasedb/featurebase/v3/idk/datagen"
	"github.com/featurebasedb/featurebase/v3/logger"
	"gopkg.in/DataDog/dd-trace-go.v1/profiler"
)

func main() {

	m := datagen.NewMain()

	if err := pflag.LoadEnv(m, "GEN_", nil); err != nil {
		log.Fatal(err)
	}

	if m.Datadog {
		err := profiler.Start(
			profiler.WithService("datagen"),
			profiler.WithEnv("fb-1253"),
			profiler.WithVersion("v1"),
			profiler.WithProfileTypes(
				profiler.CPUProfile,
				profiler.HeapProfile,
				profiler.BlockProfile,
				profiler.MutexProfile,
				profiler.GoroutineProfile,
			),
		)
		if err != nil {
			log.Fatal(err)
		}
		defer profiler.Stop()
	}

	if err := m.Preload(); err != nil {
		log.Fatal(err)
	}
	m.PrintPlan()
	if m.DryRun {
		log.Printf("%+v\n", m)
		return
	}
	if err := m.Run(); err != nil {
		logger.NewStandardLogger(os.Stderr).Errorf("Error running command: %v", err)
		os.Exit(1)
	}
}
