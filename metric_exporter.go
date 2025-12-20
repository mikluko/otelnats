package otelnats

import (
	"context"
	"sync"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// MetricExporter exports metrics to NATS.
// It implements [go.opentelemetry.io/otel/sdk/metric.Exporter].
type MetricExporter struct {
	conn   *nats.Conn
	config *config

	temporality metricdata.Temporality

	mu       sync.Mutex
	shutdown bool
}

// NewMetricExporter creates a new metric exporter that publishes to NATS.
//
// The exporter publishes protobuf-serialized OTLP metric data to the configured
// subject (default: "otel.metrics"). Use [WithSubjectPrefix] to customize.
func NewMetricExporter(nc *nats.Conn, opts ...Option) (*MetricExporter, error) {
	if nc == nil {
		return nil, errNilConnection
	}

	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	return &MetricExporter{
		conn:        nc,
		config:      cfg,
		temporality: metricdata.CumulativeTemporality,
	}, nil
}

// Temporality returns the temporality for the given instrument kind.
// By default, cumulative temporality is used for all instruments.
func (e *MetricExporter) Temporality(_ metric.InstrumentKind) metricdata.Temporality {
	return e.temporality
}

// Aggregation returns the aggregation for the given instrument kind.
// The default aggregation is used.
func (e *MetricExporter) Aggregation(kind metric.InstrumentKind) metric.Aggregation {
	return metric.DefaultAggregationSelector(kind)
}

// Export exports metrics to NATS.
//
// Metrics are converted to OTLP protobuf format and published to the metrics subject.
// The method respects context cancellation and the configured timeout.
func (e *MetricExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	e.mu.Lock()
	if e.shutdown {
		e.mu.Unlock()
		return nil
	}
	e.mu.Unlock()

	if rm == nil {
		return nil
	}

	// Convert SDK metrics to proto
	metricsData := resourceMetricsToProto(rm)

	// Marshal using configured encoding
	data, err := e.config.marshal(metricsData)
	if err != nil {
		return err
	}

	// Build message with headers
	msg := &nats.Msg{
		Subject: e.config.subject(signalMetrics),
		Data:    data,
		Header:  e.config.buildHeaders(ctx, signalMetrics),
	}

	// Publish with appropriate method
	if e.config.jetstream != nil {
		return e.publishJetStream(ctx, msg)
	}
	return e.publishCore(ctx, msg)
}

func (e *MetricExporter) publishCore(ctx context.Context, msg *nats.Msg) error {
	if err := e.conn.PublishMsg(msg); err != nil {
		return err
	}
	return e.conn.FlushTimeout(e.config.timeout)
}

func (e *MetricExporter) publishJetStream(ctx context.Context, msg *nats.Msg) error {
	_, err := e.config.jetstream.PublishMsg(ctx, msg)
	return err
}

// ForceFlush flushes any buffered metric data.
func (e *MetricExporter) ForceFlush(ctx context.Context) error {
	e.mu.Lock()
	if e.shutdown {
		e.mu.Unlock()
		return nil
	}
	e.mu.Unlock()

	return e.conn.FlushTimeout(e.config.timeout)
}

// Shutdown shuts down the exporter.
//
// After Shutdown is called, Export will return immediately without error.
func (e *MetricExporter) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	e.shutdown = true
	e.mu.Unlock()
	return nil
}
