// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
