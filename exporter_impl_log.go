package otelnats

import (
	"context"

	"github.com/nats-io/nats.go"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

// NewLogExporter creates a new log exporter that publishes to NATS.
//
// The exporter publishes protobuf-serialized OTLP log data to the configured
// subject (default: "otel.logs"). Use [WithExporterSubjectPrefix] to customize.
func NewLogExporter(nc *nats.Conn, opts ...ExporterOption) (sdklog.Exporter, error) {
	if nc == nil {
		return nil, ErrNilConnection
	}
	cfg := defaultConfig(nc)
	for _, opt := range opts {
		opt(cfg)
	}
	impl := logExporterImpl{
		config:    cfg,
		marshaler: cfg.marshaler(),
		publisher: cfg.publisher(),
		lifecycle: &lifecycle{
			nc: nc,
		},
	}
	return &impl, nil
}

// logExporterImpl exports log records to NATS.
// It implements [go.opentelemetry.io/otel/sdk/log.Exporter].
type logExporterImpl struct {
	marshaler
	publisher
	*lifecycle
	*config
}

// Export exports log records to NATS.
//
// Records are converted to OTLP protobuf format and published to the logs subject.
// The method respects context cancellation and the configured timeout.
func (e *logExporterImpl) Export(ctx context.Context, records []sdklog.Record) error {
	if len(records) == 0 {
		return nil
	}

	if e.isShutdown() {
		return nil
	}

	// Convert SDK records to proto
	logsData := recordsToLogsData(records)

	// Marshal using configured encoding
	data, err := e.marshal(logsData)
	if err != nil {
		return err
	}

	// Build message with headers
	msg := &nats.Msg{
		Subject: e.config.subject(SignalLogs),
		Data:    data,
		Header:  e.config.buildHeaders(ctx, SignalLogs),
	}

	// Publish message
	return e.publish(ctx, msg)
}
