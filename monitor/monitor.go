// Copyright 2021 Molecula Corp. All rights reserved.
package monitor

import (
	"context"
	"flag"
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

var isOn bool

// Initialiazing Sentry with particular settings
func InitErrorMonitor(version string) {
	isOn = true
	err := sentry.Init(sentry.ClientOptions{
		Dsn:              getDSN(),
		AttachStacktrace: true,
		Debug:            false,
		TracesSampleRate: 1,
		Release:          version,
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
	if !isOn || isTest() {
		return
	}
	sentry.CaptureMessage(message)
	defer sentry.Flush(2 * time.Second)
}

// CaptureException sends an error to Sentry.
func CaptureException(level int, format string, v ...interface{}) {
	if !isOn || isTest() {
		return
	}
	if level > LevelWarn {
		return
	}
	err := fmt.Errorf(format, v...)

	sentry.CaptureException(err)
	defer sentry.Flush(2 * time.Second)
}

// monitorRun runs in a goroutine and sends a heartbeat to Sentry every 24 hours.
func monitorRun() {
	for i := 0; ; i++ {
		CaptureMessage(fmt.Sprintf("Session:%d", i))
		time.Sleep(24 * time.Hour)
	}
}

// IsOn returns true if the monitor is enabled.
func IsOn() bool {
	return isOn
}

// isTest returns true if execution is part of test
func isTest() bool {
	return flag.Lookup("test.v") != nil
}

// DSN identifies which sentry project to report to
func getDSN() string {
	if isTest() {
		return "https://13194f56bf7b4e049fab181d489dd297@o1007484.ingest.sentry.io/6493546"
	}
	return "https://a6c854a5aa2a4c5cb5baaf01e6968a77@o1007484.ingest.sentry.io/6448164"
}

// Wrappers around Sentry's span to minimize exposure of sentry elsewhere in the codebase and for single-responsibility
func StartSpan(ctx context.Context, txType, txName string) *sentry.Span {
	if !isOn || isTest() {
		return &sentry.Span{}
	}
	return sentry.StartSpan(ctx, txType, sentry.TransactionName(txName))
}

func Finish(span *sentry.Span) {
	if !isOn || isTest() {
		return
	}
	span.Finish()
}
