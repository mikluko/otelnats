package otelnats

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracespb "go.opentelemetry.io/proto/otlp/trace/v1"
)

type MessageCore interface {
	Subject() string
	Data() []byte
	Headers() nats.Header
}

type MessageFlow interface {
	Ack() error
	Nak() error
	Term() error
}

type Message interface {
	MessageCore
	MessageFlow
}

type MessageSignal[T any] interface {
	Message
	// Signal must return ErrUnmarshal if the message data is invalid
	Signal() (*T, error)
}

func MessageSignalFrom[T any](msg Message) MessageSignal[T] {
	return &messageSignalImpl[T]{Message: msg}
}

type Receiver interface {
	Start(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

type MessageHandler[T any] func(ctx context.Context, msg MessageSignal[T]) error

// receiverImpl consumes OTLP telemetry from NATS subjects.
type receiverImpl struct {
	conn   *nats.Conn
	config *receiverConfig

	mu       sync.Mutex
	started  bool
	shutdown bool

	// MessageSignal handlers
	logsMessageHandler    MessageHandler[logspb.LogsData]
	tracesMessageHandler  MessageHandler[tracespb.TracesData]
	metricsMessageHandler MessageHandler[metricspb.MetricsData]
	errorHandler          ErrorHandler

	// Core NATS subscriptions
	subs []*nats.Subscription

	// JetStream consumers and their contexts
	consumeContexts []jetstream.ConsumeContext

	// Single shared message backlog for buffering all incoming messages
	msgBacklog chan Message

	// In-flight message tracking
	wg sync.WaitGroup
}

// ErrorHandler is called when an error occurs in an async context where it cannot be returned.
// This includes errors from Nak(), Ack(), Term(), and other operations in message processing goroutines.
type ErrorHandler func(error)

type receiverConfig struct {
	subjectPrefix string
	subjectSuffix string
	queueGroup    string

	// JetStream options
	jetstream    jetstream.JetStream
	stream       string
	consumerName string
	consumer     jetstream.Consumer // pre-created consumer from WithReceiverConsumer
	ackWait      time.Duration
	backlogSize  int

	// handlers
	logsHandler    MessageHandler[logspb.LogsData]
	tracesHandler  MessageHandler[tracespb.TracesData]
	metricsHandler MessageHandler[metricspb.MetricsData]
	errorHandler   ErrorHandler
}

func defaultReceiverConfig() *receiverConfig {
	return &receiverConfig{
		subjectPrefix: defaultSubjectPrefix,
		queueGroup:    "",
		ackWait:       30 * time.Second,
		backlogSize:   100,
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

// WithReceiverQueueGroup sets the queue group for load-balanced consumption.
// When multiple receivers use the same queue group, each message is
// delivered to only one receiver in the group.
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

// NewReceiver creates a new receiver that consumes from NATS.
func NewReceiver(nc *nats.Conn, opts ...ReceiverOption) (Receiver, error) {
	if nc == nil {
		return nil, ErrNilConnection
	}

	cfg := defaultReceiverConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	return &receiverImpl{
		conn:                  nc,
		config:                cfg,
		logsMessageHandler:    cfg.logsHandler,
		tracesMessageHandler:  cfg.tracesHandler,
		metricsMessageHandler: cfg.metricsHandler,
		errorHandler:          cfg.errorHandler,
	}, nil
}

// Start begins receiving messages from NATS.
// Creates a single subscription and routes messages based on the Otel-Signal header.
func (r *receiverImpl) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.shutdown {
		return ErrReceiverShutdown
	}
	if r.started {
		return nil
	}

	// Check if at least one handler is configured
	if r.logsMessageHandler == nil && r.tracesMessageHandler == nil && r.metricsMessageHandler == nil {
		return ErrNoHandlers
	}

	// Create a single subscription with broad subject pattern
	// Messages are routed internally based on Otel-Signal header
	if err := r.subscribe(ctx); err != nil {
		return err
	}

	r.started = true
	return nil
}

func (r *receiverImpl) subscribe(ctx context.Context) error {
	// Build subject pattern to match all signals
	subject := r.config.subjectPrefix + ".>"
	if r.config.subjectSuffix != "" {
		subject = r.config.subjectPrefix + ".*." + r.config.subjectSuffix
	}

	// JetStream subscription
	if r.config.jetstream != nil {
		return r.subscribeJetStream(ctx, subject)
	}

	// Core NATS subscription - use shared backlog for buffering
	backlog := r.getOrCreateBacklog()

	natsMsgHandler := func(msg *nats.Msg) {
		// Spool message into backlog channel
		select {
		case backlog <- &messageCoreWrapper{msg: msg}:
		default:
			// Backlog full - drop message (core NATS doesn't support NAK)
			// This prevents blocking the NATS client callback
		}
	}

	var sub *nats.Subscription
	var err error
	if r.config.queueGroup != "" {
		sub, err = r.conn.QueueSubscribe(subject, r.config.queueGroup, natsMsgHandler)
	} else {
		sub, err = r.conn.Subscribe(subject, natsMsgHandler)
	}
	if err != nil {
		return err
	}
	r.subs = append(r.subs, sub)
	return nil
}

// getOrCreateBacklog returns the shared backlog channel, creating it on first call.
// All subscriptions (core NATS and JetStream) share a single backlog for message buffering.
func (r *receiverImpl) getOrCreateBacklog() chan Message {
	// Return existing backlog if already created
	if r.msgBacklog != nil {
		return r.msgBacklog
	}

	// Create shared backlog channel
	r.msgBacklog = make(chan Message, r.config.backlogSize)

	// Start single worker goroutine to route all messages by header
	go func() {
		for msg := range r.msgBacklog {
			r.routeMessage(msg)
		}
	}()

	return r.msgBacklog
}

// routeMessage routes a message to the appropriate handler based on the Otel-Signal header.
// For messages with unconfigured handlers, sends NAK to trigger redelivery.
// For messages with unknown/missing signal headers, terminates the message.
func (r *receiverImpl) routeMessage(msg Message) {
	signal := msg.Headers().Get(HeaderOtelSignal)

	switch signal {
	case SignalLogs:
		msgSignal := MessageSignalFrom[logspb.LogsData](msg)
		if r.logsMessageHandler == nil {
			if err := msgSignal.Term(); err != nil {
				r.errorHandler(err)
			}
			r.errorHandler(fmt.Errorf("%w: %s", ErrNoHandlerForSignal, signal))
			return
		}
		r.handleLogs(msgSignal)

	case SignalTraces:
		msgSignal := MessageSignalFrom[tracespb.TracesData](msg)
		if r.tracesMessageHandler == nil {
			if err := msgSignal.Term(); err != nil {
				r.errorHandler(err)
			}
			r.errorHandler(fmt.Errorf("%w: %s", ErrNoHandlerForSignal, signal))
			return
		}
		r.handleTraces(msgSignal)

	case SignalMetrics:
		msgSignal := MessageSignalFrom[metricspb.MetricsData](msg)
		if r.metricsMessageHandler == nil {
			if err := msgSignal.Term(); err != nil {
				r.errorHandler(err)
			}
			r.errorHandler(fmt.Errorf("%w: %s", ErrNoHandlerForSignal, signal))
			return
		}
		r.handleMetrics(msgSignal)

	default:
		// Unknown or missing signal header - terminate the message
		// This prevents infinite redelivery of malformed messages
		if jsmsg, ok := msg.(interface{ Term() error }); ok {
			if err := jsmsg.Term(); err != nil {
				r.errorHandler(err)
			}
		}
		// For core NATS, just ignore (no ack/nak needed)
	}
}

func (r *receiverImpl) subscribeJetStream(ctx context.Context, subject string) error {
	consumerName := r.config.consumerName
	if consumerName == "" {
		// Generate ephemeral consumer name based on subject
		// Replace dots with dashes and remove wildcards (not valid in consumer names)
		name := strings.ReplaceAll(subject, ".", "-")
		name = strings.ReplaceAll(name, ">", "")
		name = strings.ReplaceAll(name, "*", "")
		for strings.Contains(name, "--") {
			name = strings.ReplaceAll(name, "--", "-")
		}
		name = strings.TrimSuffix(name, "-") // Clean trailing dash from removed wildcard
		consumerName = "otelnats-" + name
	}

	var consumer jetstream.Consumer

	// Use pre-created consumer if available
	if r.config.consumer != nil {
		consumer = r.config.consumer
	} else {
		// Try to get existing consumer first
		stream, err := r.config.jetstream.Stream(ctx, r.config.stream)
		if err != nil {
			return err
		}

		consumer, err = stream.Consumer(ctx, consumerName)
		if err != nil {
			if !errors.Is(err, jetstream.ErrConsumerNotFound) {
				return err
			}
			// Consumer doesn't exist, create it
			consumer, err = stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
				Durable:       consumerName,
				AckPolicy:     jetstream.AckExplicitPolicy,
				AckWait:       r.config.ackWait,
				FilterSubject: subject,
			})
			if err != nil {
				return err
			}
		}
	}

	// Use shared backlog for this consumer
	backlog := r.getOrCreateBacklog()

	// Start consuming messages using the Consume API
	// Messages are spooled into backlog channel for async processing
	consumeCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		// Spool message into backlog channel
		// This returns quickly, preventing Consume callback from blocking
		select {
		case backlog <- msg:
		default:
			if err := msg.Nak(); err != nil {
				r.errorHandler(err)
			}
		}
	})
	if err != nil {
		return err
	}

	r.consumeContexts = append(r.consumeContexts, consumeCtx)

	return nil
}

func (r *receiverImpl) handleLogs(msg MessageSignal[logspb.LogsData]) {
	r.wg.Add(1)
	defer r.wg.Done()

	ctx := context.Background()
	handlerErr := r.logsMessageHandler(ctx, msg)
	r.ackOrNak(msg, handlerErr)
}

func (r *receiverImpl) handleTraces(msg MessageSignal[tracespb.TracesData]) {
	r.wg.Add(1)
	defer r.wg.Done()

	ctx := context.Background()
	handlerErr := r.tracesMessageHandler(ctx, msg)
	r.ackOrNak(msg, handlerErr)
}

func (r *receiverImpl) handleMetrics(msg MessageSignal[metricspb.MetricsData]) {
	r.wg.Add(1)
	defer r.wg.Done()

	ctx := context.Background()
	handlerErr := r.metricsMessageHandler(ctx, msg)
	r.ackOrNak(msg, handlerErr)
}

// ackOrNak acknowledges or negatively acknowledges a message based on error.
func (r *receiverImpl) ackOrNak(msg Message, err error) {
	if err != nil {
		if nakErr := msg.Nak(); nakErr != nil {
			// Ignore "already acknowledged" errors - handler may have ack'd/nak'd manually
			if !isAlreadyAckedError(nakErr) {
				r.errorHandler(nakErr)
			}
		}
	} else {
		if ackErr := msg.Ack(); ackErr != nil {
			// Ignore "already acknowledged" errors - handler may have ack'd/nak'd manually
			if !isAlreadyAckedError(ackErr) {
				r.errorHandler(ackErr)
			}
		}
	}
}

// isAlreadyAckedError checks if an error is due to a message being already acknowledged.
func isAlreadyAckedError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "already acknowledged")
}

// Shutdown stops the receiver and waits for in-flight messages to complete.
func (r *receiverImpl) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	if r.shutdown {
		r.mu.Unlock()
		return nil
	}
	r.shutdown = true
	subs := r.subs
	r.subs = nil
	consumeContexts := r.consumeContexts
	r.consumeContexts = nil
	msgBacklog := r.msgBacklog
	r.msgBacklog = nil
	r.mu.Unlock()

	// Unsubscribe core NATS subscriptions
	for _, sub := range subs {
		if err := sub.Unsubscribe(); err != nil {
			r.errorHandler(err)
		}
	}

	// Drain JetStream consume contexts to stop message delivery gracefully
	for _, cc := range consumeContexts {
		cc.Drain()
	}

	// Wait for in-flight messages with context timeout
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}

	// Close shared backlog channel to allow worker goroutine to exit
	if msgBacklog != nil {
		close(msgBacklog)
	}

	return nil
}

// messageCoreWrapper wraps a nats.Msg and provides generic typed access with lazy unmarshaling
type messageCoreWrapper struct {
	msg *nats.Msg
}

func (m *messageCoreWrapper) Subject() string {
	return m.msg.Subject
}

func (m *messageCoreWrapper) Data() []byte {
	return m.msg.Data
}

func (m *messageCoreWrapper) Headers() nats.Header {
	return m.msg.Header
}

func (m *messageCoreWrapper) Ack() error {
	return nil
}

func (m *messageCoreWrapper) Nak() error {
	return nil
}

func (m *messageCoreWrapper) Term() error {
	return nil
}

// messageSignalImpl wraps a message and provides generic typed access with lazy unmarshaling
type messageSignalImpl[T any] struct {
	Message
	item    T
	itemErr error
	once    sync.Once
}

func (m *messageSignalImpl[T]) Signal() (*T, error) {
	m.once.Do(func() {
		contentType := m.Headers().Get(HeaderContentType)
		// T must be a proto.Message, so we need to convert it
		// This works because logspb.LogsData, tracespb.TracesData, metricspb.MetricsData all implement proto.Message
		if protoMsg, ok := any(&m.item).(proto.Message); ok {
			m.itemErr = Unmarshal(m.Data(), contentType, protoMsg)
			if m.itemErr != nil {
				m.itemErr = fmt.Errorf("%w: %v", ErrUnmarshal, m.itemErr)
			}
		} else {
			panic("invalid type T for MessageFrom, this is always a programming error")
		}
	})
	return &m.item, m.itemErr
}
