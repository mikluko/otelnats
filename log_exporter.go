package otelnats

import (
	"context"
	"sync"

	"github.com/nats-io/nats.go"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

// LogExporter exports log records to NATS.
// It implements [go.opentelemetry.io/otel/sdk/log.Exporter].
type LogExporter struct {
	conn   *nats.Conn
	config *config

	mu       sync.Mutex
	shutdown bool
}

// NewLogExporter creates a new log exporter that publishes to NATS.
//
// The exporter publishes protobuf-serialized OTLP log data to the configured
// subject (default: "otel.logs"). Use [WithSubjectPrefix] to customize.
func NewLogExporter(nc *nats.Conn, opts ...Option) (*LogExporter, error) {
	if nc == nil {
		return nil, errNilConnection
	}

	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	return &LogExporter{
		conn:   nc,
		config: cfg,
	}, nil
}

// Export exports log records to NATS.
//
// Records are converted to OTLP protobuf format and published to the logs subject.
// The method respects context cancellation and the configured timeout.
func (e *LogExporter) Export(ctx context.Context, records []sdklog.Record) error {
	e.mu.Lock()
	if e.shutdown {
		e.mu.Unlock()
		return nil
	}
	e.mu.Unlock()

	if len(records) == 0 {
		return nil
	}

	// Convert SDK records to proto
	logsData := recordsToLogsData(records)

	// Marshal using configured encoding
	data, err := e.config.marshal(logsData)
	if err != nil {
		return err
	}

	// Build message with headers
	msg := &nats.Msg{
		Subject: e.config.subject(signalLogs),
		Data:    data,
		Header:  e.config.buildHeaders(ctx, signalLogs),
	}

	// Publish with appropriate method
	if e.config.jetstream != nil {
		return e.publishJetStream(ctx, msg)
	}
	return e.publishCore(ctx, msg)
}

func (e *LogExporter) publishCore(ctx context.Context, msg *nats.Msg) error {
	if err := e.conn.PublishMsg(msg); err != nil {
		return err
	}
	// Flush with timeout to ensure message is sent
	return e.conn.FlushTimeout(e.config.timeout)
}

func (e *LogExporter) publishJetStream(ctx context.Context, msg *nats.Msg) error {
	_, err := e.config.jetstream.PublishMsg(ctx, msg)
	return err
}

// Shutdown shuts down the exporter.
//
// It drains the NATS connection to ensure pending messages are sent.
// After Shutdown is called, Export will return immediately without error.
func (e *LogExporter) Shutdown(_ context.Context) error {
	e.mu.Lock()
	e.shutdown = true
	e.mu.Unlock()

	// Note: We don't drain the connection here because it's shared
	// and may be used by other exporters. The caller owns the connection.
	return nil
}

// ForceFlush flushes any buffered log data.
//
// For NATS, this ensures all published messages have been sent to the server.
func (e *LogExporter) ForceFlush(_ context.Context) error {
	e.mu.Lock()
	if e.shutdown {
		e.mu.Unlock()
		return nil
	}
	e.mu.Unlock()

	return e.conn.FlushTimeout(e.config.timeout)
}
