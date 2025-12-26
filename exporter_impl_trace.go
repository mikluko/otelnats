package otelnats

import (
	"context"

	"github.com/nats-io/nats.go"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// NewSpanExporter creates a new trace exporter that publishes to NATS.
//
// The exporter publishes protobuf-serialized OTLP trace data to the configured
// subject (default: "otel.traces"). Use [WithExporterSubjectPrefix] to customize.
func NewSpanExporter(nc *nats.Conn, opts ...ExporterOption) (sdktrace.SpanExporter, error) {
	if nc == nil {
		return nil, ErrNilConnection
	}
	cfg := defaultConfig(nc)
	for _, opt := range opts {
		opt(cfg)
	}
	impl := spanExporterImpl{
		config:    cfg,
		marshaler: cfg.marshaler(),
		publisher: cfg.publisher(),
		lifecycle: &lifecycle{
			nc: nc,
		},
	}
	return &impl, nil
}

// spanExporterImpl exports spans to NATS.
// It implements [go.opentelemetry.io/otel/sdk/trace.SpanExporter].
type spanExporterImpl struct {
	marshaler
	publisher
	*lifecycle
	*config
}

// ExportSpans exports spans to NATS.
//
// Spans are converted to OTLP protobuf format and published to the traces subject.
// The method respects context cancellation.
func (e *spanExporterImpl) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if len(spans) == 0 {
		return nil
	}

	if e.isShutdown() {
		return nil
	}

	// Convert SDK spans to proto
	tracesData := spansToTracesData(spans)

	// Marshal using configured encoding
	data, err := e.marshal(tracesData)
	if err != nil {
		return err
	}

	// Build message with headers
	msg := &nats.Msg{
		Subject: e.config.subject(SignalTraces),
		Data:    data,
		Header:  e.config.buildHeaders(ctx, SignalTraces),
	}

	// Publish message
	return e.publish(ctx, msg)
}
