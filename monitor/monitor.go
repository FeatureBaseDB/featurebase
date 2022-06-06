// Copyright 2021 Molecula Corp. All rights reserved.
package monitor

import (
	"context"
	"fmt"
	"log"
	"time"

	sentry "github.com/getsentry/sentry-go"
)

const (
	LevelPanic = iota
	LevelError
	LevelWarn
	LevelInfo
	LevelDebug
)

var IsOn bool

// Initialiazing Sentry with particular settings
func InitErrorMonitor() {
	IsOn = true
	err := sentry.Init(sentry.ClientOptions{
		Dsn:              "https://a6c854a5aa2a4c5cb5baaf01e6968a77@o1007484.ingest.sentry.io/6448164",
		AttachStacktrace: true,
		Debug:            false,
		TracesSampleRate: 1,
	})
	sentry.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetUser(sentry.User{IPAddress: "{{auto}}"})
	})
	if err != nil {
		log.Fatalf("sentry.Init: %s", err)
	}
	CaptureMessage("Session:Started")
	go monitorRun()

}

// CaptureMessage sends a message to Sentry.
func CaptureMessage(message string) {
	if !IsOn {
		return
	}
	sentry.CaptureMessage(message)
	defer sentry.Flush(2 * time.Second)
}

// CaptureException sends an error to Sentry.
func CaptureException(level int, format string, v ...interface{}) {
	if !IsOn {
		return
	}
	if level > LevelWarn {
		return
	}
	err := fmt.Errorf(format, v...)

	sentry.CaptureException(err)
	defer sentry.Flush(2 * time.Second)
}

// CapturePerformance spans a function to capture performance metrics.
func CapturePerformance(ctx context.Context, txType, txName string, fn func()) {
	if !IsOn {
		return
	}
	span := sentry.StartSpan(ctx, txType, sentry.TransactionName(txName))
	fn()
	span.Finish()
}

// monitorRun runs in a goroutine and sends a heartbeat to Sentry every 24 hours.
func monitorRun() {
	for i := 0; ; i++ {
		CaptureMessage(fmt.Sprintf("Session:%d", i))
		time.Sleep(24 * time.Hour)
	}
}
