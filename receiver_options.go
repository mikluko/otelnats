package otelnats

import (
	"context"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracespb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// ReceiverOption configures a receiverImpl.
type ReceiverOption func(*receiverConfig)

// WithReceiverSubjectPrefix sets the subject prefix for subscriptions.
// The default prefix is "otel", subscribing to "otel.logs", "otel.traces", "otel.metrics".
func WithReceiverSubjectPrefix(prefix string) ReceiverOption {
	return func(c *receiverConfig) {
		c.subjectPrefix = prefix
	}
}

// WithReceiverSubjectSuffix appends a suffix to the signal subjects.
// For example, WithReceiverSubjectSuffix(">") subscribes to "otel.logs.>",
// which matches all subjects under that hierarchy (e.g., "otel.logs.tenant-a").
//
// This is useful for multi-tenant deployments where a receiver needs to
// consume messages from all tenants.
func WithReceiverSubjectSuffix(suffix string) ReceiverOption {
	return func(c *receiverConfig) {
		c.subjectSuffix = suffix
	}
}

// WithReceiverQueueGroup sets the queue group for load-balanced Core NATS consumption.
// When multiple receivers use the same queue group, each message is
// delivered to only one receiver in the group.
//
// NOTE: This only applies to Core NATS subscriptions. JetStream uses pull consumers
// and does not support queue groups via DeliverGroup (which is for push consumers only).
// For JetStream load balancing, multiple receiver instances should share the same
// durable consumer name via [WithReceiverConsumerName].
func WithReceiverQueueGroup(group string) ReceiverOption {
	return func(c *receiverConfig) {
		c.queueGroup = group
	}
}

// WithReceiverJetStream enables JetStream consumption with at-least-once delivery.
// The stream parameter specifies the JetStream stream to consume from.
// Messages are acknowledged after successful handler execution; on handler error,
// messages are NAK'd to trigger redelivery.
func WithReceiverJetStream(js jetstream.JetStream, stream string) ReceiverOption {
	return func(c *receiverConfig) {
		c.jetstream = js
		c.stream = stream
	}
}

// WithReceiverConsumerName sets the durable consumer name for JetStream subscriptions.
// When set, the consumer state is persisted, allowing receivers to resume
// from where they left off after restart.
//
// NOTE: Multiple receiver instances can share the same consumer name for load balancing.
// JetStream will distribute messages across all active pull consumers of the same durable consumer.
// Only applies when JetStream is enabled via [WithReceiverJetStream].
func WithReceiverConsumerName(name string) ReceiverOption {
	return func(c *receiverConfig) {
		c.consumerName = name
	}
}

// WithReceiverConsumer binds the receiver to an existing JetStream consumer.
// The consumer must already exist (created via the native jetstream API).
// This option stores the consumer directly, avoiding lookups during subscription.
// Only applies when JetStream is enabled via [WithReceiverJetStream].
func WithReceiverConsumer(consumer jetstream.Consumer) ReceiverOption {
	return func(c *receiverConfig) {
		c.consumer = consumer
		c.consumerName = consumer.CachedInfo().Name
	}
}

// WithReceiverAckWait sets the acknowledgment timeout for JetStream messages.
// If a message is not acknowledged within this duration, it will be redelivered.
// The default is 30 seconds.
// Only applies when JetStream is enabled via [WithReceiverJetStream].
func WithReceiverAckWait(d time.Duration) ReceiverOption {
	return func(c *receiverConfig) {
		c.ackWait = d
	}
}

// WithReceiverBacklogSize sets the buffer size for the message backlog channel.
// This channel buffers messages between the JetStream Consume callback and the
// handler goroutine. A larger buffer allows the Consume callback to accept more
// messages without blocking, but uses more memory. The default is 100.
// Only applies when JetStream is enabled via [WithReceiverJetStream].
func WithReceiverBacklogSize(size int) ReceiverOption {
	return func(c *receiverConfig) {
		c.backlogSize = size
	}
}

// WithReceiverBaseContext sets the base context for message handler invocations.
// This context is passed to all message handlers and allows propagating values
// (e.g., trace context, tenant IDs) or implementing custom cancellation strategies.
// If not set, defaults to context.Background().
//
// Example:
//
//	ctx := context.WithValue(context.Background(), "tenant", "acme")
//	otelnats.NewReceiver(nc, otelnats.WithReceiverBaseContext(ctx))
func WithReceiverBaseContext(ctx context.Context) ReceiverOption {
	return func(c *receiverConfig) {
		c.baseContext = ctx
	}
}

func WithReceiverLogsHandler(fn MessageHandler[logspb.LogsData]) ReceiverOption {
	return func(r *receiverConfig) {
		r.logsHandler = fn
	}
}

func WithReceiverTracesHandler(fn MessageHandler[tracespb.TracesData]) ReceiverOption {
	return func(r *receiverConfig) {
		r.tracesHandler = fn
	}
}

func WithReceiverMetricsHandler(fn MessageHandler[metricspb.MetricsData]) ReceiverOption {
	return func(r *receiverConfig) {
		r.metricsHandler = fn
	}
}

// WithReceiverErrorHandler sets a custom error handler for async errors.
// These are errors that occur in goroutines where they cannot be returned to the caller,
// such as Nak(), Ack(), Term() failures, or subscription errors.
//
// The default error handler panics to ensure errors are not silently ignored.
// Set a custom handler to log errors or handle them gracefully.
//
// Example:
//
//	WithReceiverErrorHandler(func(err error) {
//	    log.Printf("receiver error: %v", err)
//	})
func WithReceiverErrorHandler(fn ErrorHandler) ReceiverOption {
	return func(r *receiverConfig) {
		r.errorHandler = fn
	}
}
