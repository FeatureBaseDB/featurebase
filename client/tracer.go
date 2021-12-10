// Copyright 2021 Molecula Corp. All rights reserved.
// package ctl contains all pilosa subcommands other than 'server'. These are
// generally administration, testing, and debugging tools.

package client

import (
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
)

type NoopTracer struct{}

type NoopSpan struct{}

func (s NoopSpan) Finish() {
	// pass
}
func (s NoopSpan) FinishWithOptions(opts opentracing.FinishOptions) {
	// pass
}

func (s NoopSpan) Context() opentracing.SpanContext {
	return nil
}
func (s NoopSpan) SetOperationName(operationName string) opentracing.Span {
	return s
}

func (s NoopSpan) SetTag(key string, value interface{}) opentracing.Span {
	return s
}

func (s NoopSpan) LogFields(fields ...log.Field) {
	// pass
}

func (s NoopSpan) LogKV(alternatingKeyValues ...interface{}) {
	// pass
}

func (s NoopSpan) SetBaggageItem(restrictedKey, value string) opentracing.Span {
	return s
}

func (s NoopSpan) BaggageItem(restrictedKey string) string {
	return ""
}

func (s NoopSpan) Tracer() opentracing.Tracer {
	return nil
}

func (s NoopSpan) LogEvent(event string) {
	// pass
}

func (s NoopSpan) LogEventWithPayload(event string, payload interface{}) {
	// pass
}

func (s NoopSpan) Log(data opentracing.LogData) {
	// pass
}

func (t NoopTracer) StartSpan(operationName string, opts ...opentracing.StartSpanOption) opentracing.Span {
	return NoopSpan{}
}

func (t NoopTracer) Inject(sm opentracing.SpanContext, format interface{}, carrier interface{}) error {
	return nil
}

func (t NoopTracer) Extract(format interface{}, carrier interface{}) (opentracing.SpanContext, error) {
	return nil, nil
}
