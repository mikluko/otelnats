package otelnats

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/sdk/metric"
)

// ExporterOption configures an exporter or receiver.
type ExporterOption func(*config)

// WithExporterSubjectPrefix sets the subject prefix for NATS messages.
// The default prefix is "otel", resulting in subjects like "otel.logs".
func WithExporterSubjectPrefix(prefix string) ExporterOption {
	return func(c *config) {
		c.subjectPrefix = prefix
	}
}

// WithExporterSubjectSuffix appends a suffix to the signal subjects.
// For example, WithExporterSubjectSuffix("tenant-a") with default prefix results in
// subjects like "otel.logs.tenant-a".
//
// This is useful for multi-tenant deployments where each tenant publishes
// to a unique subject hierarchy.
func WithExporterSubjectSuffix(suffix string) ExporterOption {
	return func(c *config) {
		c.subjectSuffix = suffix
	}
}

// WithExporterEncoding sets the serialization format for OTLP messages.
// The default is EncodingProtobuf. Use EncodingJSON for JSON serialization.
//
// The Content-Type header is set automatically based on encoding:
//   - EncodingProtobuf: application/x-protobuf
//   - EncodingJSON: application/json
func WithExporterEncoding(enc Encoding) ExporterOption {
	return func(c *config) {
		c.encoding = enc
	}
}

// WithExporterJetStream enables JetStream publishing with acknowledgments.
// When set, the exporter will use js.Publish() instead of nc.Publish(),
// providing at-least-once delivery guarantees.
func WithExporterJetStream(js jetstream.JetStream) ExporterOption {
	return func(c *config) {
		c.jetstream = js
	}
}

// WithExporterTemporality sets the aggregation temporality the metric exporter
// reports for each instrument kind. A PeriodicReader takes its temporality from
// its exporter, so this is what decides whether the SDK re-exports every series
// it has ever seen on every collection (cumulative) or only what changed since
// the last one (delta).
//
// The default is [metric.DefaultTemporalitySelector], which is cumulative for
// every kind. Delta suits a short-lived attribute set, where cumulative retains
// each one for the process's lifetime:
//
//	otelnats.WithExporterTemporality(func(metric.InstrumentKind) metricdata.Temporality {
//		return metricdata.DeltaTemporality
//	})
//
// It has no effect on the trace or log exporters.
func WithExporterTemporality(sel metric.TemporalitySelector) ExporterOption {
	return func(c *config) {
		if sel != nil {
			c.temporality = sel
		}
	}
}

// WithExporterHeaders sets a function that provides additional headers for each message.
// The function is called for each export operation, allowing dynamic headers
// based on context (e.g., trace propagation, tenant ID).
//
// Built-in headers (Content-Type, Otel-Signal) are always set and cannot be
// overridden by this function.
func WithExporterHeaders(fn func(context.Context) nats.Header) ExporterOption {
	return func(c *config) {
		c.headers = fn
	}
}
