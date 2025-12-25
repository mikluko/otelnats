package otelnats

import (
	"context"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// NewMetricExporter creates a new metric exporter that publishes to NATS.
//
// The exporter publishes protobuf-serialized OTLP metric data to the configured
// subject (default: "otel.metrics"). Use [WithExporterSubjectPrefix] to customize.
func NewMetricExporter(nc *nats.Conn, opts ...ExporterOption) (metric.Exporter, error) {
	if nc == nil {
		return nil, ErrNilConnection
	}
	cfg := defaultConfig(nc)
	for _, opt := range opts {
		opt(cfg)
	}
	impl := metricExporterImpl{
		config:      cfg,
		marshaler:   cfg.marshaler(),
		publisher:   cfg.publisher(),
		lifecycle:   &lifecycle{nc: nc},
		temporality: metricdata.CumulativeTemporality,
	}
	return &impl, nil
}

// metricExporterImpl exports metrics to NATS.
// It implements [go.opentelemetry.io/otel/sdk/metric.Exporter].
type metricExporterImpl struct {
	marshaler
	publisher
	*lifecycle
	*config

	temporality metricdata.Temporality
}

// Temporality returns the temporality for the given instrument kind.
// By default, cumulative temporality is used for all instruments.
func (e *metricExporterImpl) Temporality(_ metric.InstrumentKind) metricdata.Temporality {
	return e.temporality
}

// Aggregation returns the aggregation for the given instrument kind.
// The default aggregation is used.
func (e *metricExporterImpl) Aggregation(kind metric.InstrumentKind) metric.Aggregation {
	return metric.DefaultAggregationSelector(kind)
}

// Export exports metrics to NATS.
//
// Metrics are converted to OTLP protobuf format and published to the metrics subject.
// The method respects context cancellation.
func (e *metricExporterImpl) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	if rm == nil {
		return nil
	}

	if e.isShutdown() {
		return nil
	}

	// Convert SDK metrics to proto
	metricsData := resourceMetricsToProto(rm)

	// Marshal using configured encoding
	data, err := e.marshal(metricsData)
	if err != nil {
		return err
	}

	// Build message with headers
	msg := &nats.Msg{
		Subject: e.config.subject(SignalMetrics),
		Data:    data,
		Header:  e.config.buildHeaders(ctx, SignalMetrics),
	}

	// Publish message
	return e.publish(ctx, msg)
}
