package main

import (
	"log"

	"github.com/featurebasedb/featurebase/v3/idk/kafka"
	"github.com/jaffee/commandeer/pflag"
)

func main() {
	delete_consumer, err := kafka.NewMain()
	if err != nil {
		log.Fatal(err)
	}
	delete_consumer.Delete = true

	if err := pflag.LoadEnv(delete_consumer, "CONSUMER_DEL_", nil); err != nil {
		log.Fatal(err)
	}
	delete_consumer.Rename()
	if delete_consumer.DryRun {
		log.Printf("%+v\n", delete_consumer)
		return
	}
	if err := delete_consumer.Run(); err != nil {
		logger := delete_consumer.Log()
		if logger != nil {
			logger.Printf("Error running command: %v", err)
		}
		log.Fatal(err) // make sure we log to stderr if the error happened before logging was set up
	}
}
