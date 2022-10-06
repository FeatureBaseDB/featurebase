package main

import (
	"log"
	"os"

	"github.com/jaffee/commandeer/pflag"
	"github.com/molecula/featurebase/v3/idk/kinesis"
	"github.com/molecula/featurebase/v3/logger"
)

func logError(m *kinesis.Main, err error) {
	log := m.Log()

	if log == nil {
		log = logger.NewStandardLogger(os.Stderr)
	}
	log.Errorf("Error running command: %s", err)

}

func logPanic(m *kinesis.Main, v interface{}) {
	log := m.Log()

	if log == nil {
		log = logger.NewStandardLogger(os.Stderr)
	}
	log.Panicf("Panic running command: %+v", v)

}

func main() {
	m := kinesis.NewMain()
	if err := pflag.LoadEnv(m, "CONSUMER_", nil); err != nil {
		log.Fatal(err)
	}
	m.Rename()

	// Capture any panic and log it before dying.
	defer func() {
		if r := recover(); r != nil {
			logPanic(m, r)
			os.Exit(1)
		}
	}()

	if m.DryRun {
		log.Printf("%+v\n", m)
		return
	}

	if err := m.Run(); err != nil {
		logError(m, err)
		os.Exit(1)
	}
}
