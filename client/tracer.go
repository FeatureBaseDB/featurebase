// Copyright 2017 Pilosa Corp.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// 1. Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
// contributors may be used to endorse or promote products derived
// from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
// CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.

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
