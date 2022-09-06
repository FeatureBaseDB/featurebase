package main

import (
	"log"

	"github.com/jaffee/commandeer/pflag"
	"github.com/featurebasedb/featurebase/v3/idk/kafka"
)

func main() {
	dm, err := kafka.NewMain()
	if err != nil {
		log.Fatal(err)
	}
	dm.Delete = true

	if err := pflag.LoadEnv(dm, "CONSUMER_DEL_", nil); err != nil {
		log.Fatal(err)
	}
	dm.Rename()
	if dm.DryRun {
		log.Printf("%+v\n", dm)
		return
	}
	if err := dm.Run(); err != nil {
		logger := dm.Log()
		if logger != nil {
			logger.Printf("Error running command: %v", err)
		}
		log.Fatal(err) // make sure we log to stderr if the error happened before logging was set up
	}
}
