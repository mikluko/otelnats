package otelnats

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracespb "go.opentelemetry.io/proto/otlp/trace/v1"
)

type receiverConfig struct {
	subjectPrefix string
	subjectSuffix string
	queueGroup    string

	// Explicit per-signal subjects (override prefix/suffix when set)
	logsSubject    string
	tracesSubject  string
	metricsSubject string

	// JetStream options
	jetstream    jetstream.JetStream
	stream       string
	consumerName string
	consumer     jetstream.Consumer // pre-created consumer from WithReceiverConsumer
	ackWait      time.Duration
	backlogSize  int

	// Rate limiting options (JetStream only)
	rateLimit     float64 // messages per second (0 = disabled)
	rateBurst     int     // token bucket capacity
	rateBatchSize int     // messages per Fetch() call (defaults to rateBurst)

	// Base context for message handlers
	baseContext context.Context

	// handlers
	logsHandler    MessageHandler[logspb.LogsData]
	tracesHandler  MessageHandler[tracespb.TracesData]
	metricsHandler MessageHandler[metricspb.MetricsData]
	errorHandler   ErrorHandler
}

func (c *receiverConfig) buildSubjects() []string {
	var subjects []string

	// Use explicit subjects if set, otherwise build from prefix/suffix
	if c.logsSubject != "" {
		subjects = append(subjects, c.logsSubject)
	} else {
		subjects = append(subjects, BuildSubject(c.subjectPrefix, SignalLogs, c.subjectSuffix))
	}

	if c.metricsSubject != "" {
		subjects = append(subjects, c.metricsSubject)
	} else {
		subjects = append(subjects, BuildSubject(c.subjectPrefix, SignalMetrics, c.subjectSuffix))
	}

	if c.tracesSubject != "" {
		subjects = append(subjects, c.tracesSubject)
	} else {
		subjects = append(subjects, BuildSubject(c.subjectPrefix, SignalTraces, c.subjectSuffix))
	}

	return subjects
}

func (c *receiverConfig) buildConsumerName() string {
	if c.consumerName != "" {
		return c.consumerName
	}
	consumerName := strings.TrimRight(fmt.Sprintf("otelnats-%s-%s", c.subjectPrefix, c.subjectSuffix), "-")
	consumerName = strings.ReplaceAll(consumerName, "*", "any")
	consumerName = strings.ReplaceAll(consumerName, ">", "all")
	return consumerName
}

func defaultReceiverConfig() *receiverConfig {
	return &receiverConfig{
		subjectPrefix: defaultSubjectPrefix,
		subjectSuffix: defaultSubjectSuffix,
		queueGroup:    defaultQueueGroup,
		ackWait:       defaultAckWait,
		backlogSize:   defaultBacklogSize,
		// Handlers default to nil - only subscribe if explicitly set
		logsHandler:    nil,
		tracesHandler:  nil,
		metricsHandler: nil,
		// Default error handler panics to ensure errors are not silently ignored
		errorHandler: func(err error) {
			panic(fmt.Sprintf("otelnats receiver error: %v", err))
		},
	}
}

// ErrorHandler is called when an error occurs in an async context where it cannot be returned.
// This includes errors from Nak(), Ack(), Term(), and other operations in message processing goroutines.
type ErrorHandler func(error)
