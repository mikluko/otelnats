package otelnats

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Option configures an exporter or receiver.
type Option func(*config)

// WithSubjectPrefix sets the subject prefix for NATS messages.
// The default prefix is "otel", resulting in subjects like "otel.logs".
func WithSubjectPrefix(prefix string) Option {
	return func(c *config) {
		c.subjectPrefix = prefix
	}
}

// WithTimeout sets the timeout for publish operations.
// The default timeout is 5 seconds.
// For core NATS, this affects the flush timeout.
// For JetStream, this is the publish acknowledgment timeout.
func WithTimeout(d time.Duration) Option {
	return func(c *config) {
		c.timeout = d
	}
}

// WithJetStream enables JetStream publishing with acknowledgments.
// When set, the exporter will use js.Publish() instead of nc.Publish(),
// providing at-least-once delivery guarantees.
func WithJetStream(js jetstream.JetStream) Option {
	return func(c *config) {
		c.jetstream = js
	}
}

// WithHeaders sets a function that provides additional headers for each message.
// The function is called for each export operation, allowing dynamic headers
// based on context (e.g., trace propagation, tenant ID).
//
// Built-in headers (Content-Type, Otel-Signal) are always set and cannot be
// overridden by this function.
func WithHeaders(fn func(context.Context) nats.Header) Option {
	return func(c *config) {
		c.headers = fn
	}
}
