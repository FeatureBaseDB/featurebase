// Copyright 2021 Molecula Corp. All rights reserved.
package tracing

import (
	"context"
	"net/http"
	"time"
)

// GlobalTracer is a single, global instance of Tracer.
var GlobalTracer Tracer = NopTracer()

// StartSpanFromContext returns a new child span and context from a given
// context using the global tracer.
func StartSpanFromContext(ctx context.Context, operationName string) (Span, context.Context) {
	return startMaybeProfiledSpanFromContext(ctx, operationName, false)
}

// StartProfiledSpanFromContext returns a new child span and context from a given
// context using the global tracer.
func StartProfiledSpanFromContext(ctx context.Context, operationName string) (ProfiledSpan, context.Context) {
	span, ctx := startMaybeProfiledSpanFromContext(ctx, operationName, true)
	return span.(ProfiledSpan), ctx
}

// startMaybeProfiledSpanFromContext figures out whether it needs to make a profiling span.
func startMaybeProfiledSpanFromContext(ctx context.Context, operationName string, startProfiling bool) (Span, context.Context) {
	var parent ProfiledSpan
	makeProfile := startProfiling
	// Parent context may or may not be profiled.
	// If it is, we need to make a sub-profile. If it isn't, we need to make a new profile if
	// startProfiling is set.
	span, ok := spanFromContext(ctx)
	if ok {
		if parent, ok = span.(ProfiledSpan); ok {
			makeProfile = true
		}
	}
	// if we aren't making a profile, this is easy:
	if !makeProfile {
		return GlobalTracer.StartSpanFromContext(ctx, operationName)
	}
	newProf := &Profile{Name: operationName, Begin: time.Now(), KV: make(map[string]interface{})}
	if parent != nil {
		parent.AddChild(newProf)
	}
	inner, ctx := GlobalTracer.StartSpanFromContext(ctx, operationName)
	newProf.inner = inner
	// insert ourselves in the context
	ctx = context.WithValue(ctx, arbitraryContextKey, newProf)
	return newProf, ctx
}

// Tracer implements a generic distributed tracing interface.
type Tracer interface {
	// Returns a new child span and context from a given context.
	StartSpanFromContext(ctx context.Context, operationName string) (Span, context.Context)

	// Adds the required HTTP headers to pass context between nodes.
	InjectHTTPHeaders(r *http.Request)

	// Reads the HTTP headers to derive incoming context.
	ExtractHTTPHeaders(r *http.Request) (Span, context.Context)
}

// Span represents a single span in a distributed trace.
type Span interface {
	// Sets the end timestamp and finalizes Span state.
	Finish()

	// Adds key/value pairs to the span.
	LogKV(alternatingKeyValues ...interface{})
}

// ProfiledSpan represents a span which profiles itself and its children.
type ProfiledSpan interface {
	Span
	Dump() interface{} // suitable for marshaling
	AddChild(ProfiledSpan)
}

// Profile represents the profiling data for a span. It also handles
// the bookkeeping for the underlying span, but this is unexported so
// it doesn't get unmarshaled.
type Profile struct {
	inner      Span
	Name       string
	Begin, End time.Time `json:"-"`
	Duration   time.Duration
	Children   []ProfiledSpan         `json:",omitempty"`
	KV         map[string]interface{} `json:",omitempty"`
}

func (p *Profile) Finish() {
	p.inner.Finish()
	p.End = time.Now()
	p.Duration = p.End.Sub(p.Begin)
}

func (p *Profile) LogKV(alternatingKeyValues ...interface{}) {
	for i := 0; i < len(alternatingKeyValues)-1; i += 2 {
		if s, ok := alternatingKeyValues[i].(string); ok {
			p.KV[s] = alternatingKeyValues[i+1]
		}
	}
	p.inner.LogKV(alternatingKeyValues...)
}

// returns something that json could probably marshal.
func (p *Profile) Dump() interface{} {
	return p
}

func (p *Profile) AddChild(child ProfiledSpan) {
	p.Children = append(p.Children, child)
}

// NopTracer returns a tracer that doesn't do anything.
func NopTracer() Tracer {
	return &nopTracer{}
}

type nopTracer struct{}

func (t *nopTracer) StartSpanFromContext(ctx context.Context, operationName string) (Span, context.Context) {
	return &nopSpan{}, ctx
}

func (t *nopTracer) InjectHTTPHeaders(r *http.Request) {}

func (t *nopTracer) ExtractHTTPHeaders(r *http.Request) (Span, context.Context) {
	return &nopSpan{}, r.Context()
}

type nopSpan struct{}

func (s *nopSpan) Finish()                                   {}
func (s *nopSpan) LogKV(alternatingKeyValues ...interface{}) {}

type arbitraryContextKeyType int

var arbitraryContextKey arbitraryContextKeyType

func spanFromContext(ctx context.Context) (Span, bool) {
	span, ok := ctx.Value(arbitraryContextKey).(Span)
	return span, ok
}
