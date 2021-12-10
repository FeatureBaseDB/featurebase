package opentracing

import (
	"context"
	"net/http"

	"github.com/molecula/featurebase/v2/logger"
	"github.com/molecula/featurebase/v2/tracing"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

// Ensure type implements interface.
var _ tracing.Tracer = (*Tracer)(nil)

// Tracer represents a wrapper for OpenTracing that implements tracing.Tracer.
type Tracer struct {
	tracer opentracing.Tracer
	logger logger.Logger
}

// NewTracer returns a new instance of Tracer.
func NewTracer(tracer opentracing.Tracer, logger logger.Logger) *Tracer {
	return &Tracer{tracer: tracer, logger: logger}
}

// StartSpanFromContext returns a new child span and context from a given context.
func (t *Tracer) StartSpanFromContext(ctx context.Context, operationName string) (tracing.Span, context.Context) {
	var opts []opentracing.StartSpanOption
	if parent := opentracing.SpanFromContext(ctx); parent != nil {
		opts = append(opts, opentracing.ChildOf(parent.Context()))
	}
	span := t.tracer.StartSpan(operationName, opts...)
	return span, opentracing.ContextWithSpan(ctx, span)
}

// InjectHTTPHeaders adds the required HTTP headers to pass context between nodes.
func (t *Tracer) InjectHTTPHeaders(r *http.Request) {
	if span := opentracing.SpanFromContext(r.Context()); span != nil {
		if err := t.tracer.Inject(
			span.Context(),
			opentracing.HTTPHeaders,
			opentracing.HTTPHeadersCarrier(r.Header),
		); err != nil {
			t.logger.Errorf("opentracing inject error: %s", err)
		}
	}
}

// ExtractHTTPHeaders reads the HTTP headers to derive incoming context.
func (t *Tracer) ExtractHTTPHeaders(r *http.Request) (tracing.Span, context.Context) {
	// Deserialize tracing context into request.
	wireContext, _ := t.tracer.Extract(
		opentracing.HTTPHeaders,
		opentracing.HTTPHeadersCarrier(r.Header),
	)

	span := t.tracer.StartSpan("HTTP", ext.RPCServerOption(wireContext))
	ctx := opentracing.ContextWithSpan(r.Context(), span)
	return span, ctx
}
