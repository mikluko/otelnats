package otelnats

import (
	"context"
	"sync"

	"github.com/nats-io/nats.go"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// TraceExporter exports spans to NATS.
// It implements [go.opentelemetry.io/otel/sdk/trace.SpanExporter].
type TraceExporter struct {
	conn   *nats.Conn
	config *config

	mu       sync.Mutex
	shutdown bool
}

// NewTraceExporter creates a new trace exporter that publishes to NATS.
//
// The exporter publishes protobuf-serialized OTLP trace data to the configured
// subject (default: "otel.traces"). Use [WithSubjectPrefix] to customize.
func NewTraceExporter(nc *nats.Conn, opts ...Option) (*TraceExporter, error) {
	if nc == nil {
		return nil, errNilConnection
	}

	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	return &TraceExporter{
		conn:   nc,
		config: cfg,
	}, nil
}

// ExportSpans exports spans to NATS.
//
// Spans are converted to OTLP protobuf format and published to the traces subject.
// The method respects context cancellation and the configured timeout.
func (e *TraceExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	if e.shutdown {
		e.mu.Unlock()
		return nil
	}
	e.mu.Unlock()

	if len(spans) == 0 {
		return nil
	}

	// Convert SDK spans to proto
	tracesData := spansToTracesData(spans)

	// Marshal using configured encoding
	data, err := e.config.marshal(tracesData)
	if err != nil {
		return err
	}

	// Build message with headers
	msg := &nats.Msg{
		Subject: e.config.subject(signalTraces),
		Data:    data,
		Header:  e.config.buildHeaders(ctx, signalTraces),
	}

	// Publish with appropriate method
	if e.config.jetstream != nil {
		return e.publishJetStream(ctx, msg)
	}
	return e.publishCore(ctx, msg)
}

func (e *TraceExporter) publishCore(ctx context.Context, msg *nats.Msg) error {
	if err := e.conn.PublishMsg(msg); err != nil {
		return err
	}
	return e.conn.FlushTimeout(e.config.timeout)
}

func (e *TraceExporter) publishJetStream(ctx context.Context, msg *nats.Msg) error {
	_, err := e.config.jetstream.PublishMsg(ctx, msg)
	return err
}

// Shutdown shuts down the exporter.
//
// After Shutdown is called, ExportSpans will return immediately without error.
func (e *TraceExporter) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	e.shutdown = true
	e.mu.Unlock()
	return nil
}
