package otelnats

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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
