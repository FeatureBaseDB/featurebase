package main

import (
	"log"
	"os"

	"github.com/jaffee/commandeer/pflag"
	"github.com/molecula/featurebase/v3/idk/kinesis"
	"github.com/molecula/featurebase/v3/logger"
)

func logFailure(errorType kinesis.ErrorType, m *kinesis.Main, v interface{}) {
	log := m.Log()

	if log == nil {
		log = logger.NewStandardLogger(os.Stderr)
	}

	if errorType == kinesis.RecoverableErrorType {
		log.Errorf("Error running command: %+v", v)
	} else {
		log.Panicf("Panic running command: %+v", v)
	}
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
			logFailure(kinesis.PanicErrorType, m, r)
			os.Exit(1)
		}
	}()

	if m.DryRun {
		log.Printf("%+v\n", m)
		return
	}

	if err := m.Run(); err != nil {
		logFailure(kinesis.RecoverableErrorType, m, err)
		os.Exit(1)
	}
}
