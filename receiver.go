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

type Message[T any] interface {
	// Item must return ErrUnmarshall if the message data is invalid
	Item() (*T, error)
	Data() []byte
	Headers() nats.Header
	Ack() error
	Nak() error
}

type Receiver interface {
	Start(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

type MessageHandler[T any] func(ctx context.Context, msg Message[T]) error

// receiverImpl consumes OTLP telemetry from NATS subjects.
type receiverImpl struct {
	conn   *nats.Conn
	config *receiverConfig

	mu       sync.Mutex
	started  bool
	shutdown bool

	// Message handlers
	logsMessageHandler    MessageHandler[logspb.LogsData]
	tracesMessageHandler  MessageHandler[tracespb.TracesData]
	metricsMessageHandler MessageHandler[metricspb.MetricsData]

	// Core NATS subscriptions
	subs []*nats.Subscription

	// JetStream consumers and their contexts
	consumeContexts []jetstream.ConsumeContext

	// Message backlog channels for JetStream
	msgBacklogs []chan message

	// In-flight message tracking
	wg sync.WaitGroup
}

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
	}, nil
}

// Start begins receiving messages from NATS.
// It subscribes to subjects based on registered handlers.
func (r *receiverImpl) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.shutdown {
		return ErrReceiverShutdown
	}
	if r.started {
		return nil
	}

	// Subscribe to logs if handler is set
	if r.logsMessageHandler != nil {
		if err := r.subscribe(ctx, SignalLogs, r.handleLogs); err != nil {
			return err
		}
	}

	// Subscribe to traces if handler is set
	if r.tracesMessageHandler != nil {
		if err := r.subscribe(ctx, SignalTraces, r.handleTraces); err != nil {
			return err
		}
	}

	// Subscribe to metrics if handler is set
	if r.metricsMessageHandler != nil {
		if err := r.subscribe(ctx, SignalMetrics, r.handleMetrics); err != nil {
			return err
		}
	}

	r.started = true
	return nil
}

// message is a minimal interface for both core NATS and JetStream messages.
type message interface {
	Data() []byte
	Headers() nats.Header
	Ack() error
	Nak() error
}

// msgHandler is a generic handler for messages
type msgHandler func(message)

func (r *receiverImpl) subscribe(ctx context.Context, signal string, handler msgHandler) error {
	subject := r.config.subjectPrefix + "." + signal
	if r.config.subjectSuffix != "" {
		subject += "." + r.config.subjectSuffix
	}

	// JetStream subscription
	if r.config.jetstream != nil {
		return r.subscribeJetStream(ctx, subject, handler)
	}

	// Core NATS subscription - wrap handler to work with nats.Msg
	natsMsgHandler := func(msg *nats.Msg) {
		// Create a wrapper that implements basic message interface
		handler(&coreNatsMsg{msg: msg})
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

// coreNatsMsg wraps *nats.Msg to implement a minimal interface for handlers
type coreNatsMsg struct {
	msg *nats.Msg
}

func (m *coreNatsMsg) Data() []byte {
	return m.msg.Data
}

func (m *coreNatsMsg) Headers() nats.Header {
	return m.msg.Header
}

func (m *coreNatsMsg) Ack() error {
	return nil // Core NATS doesn't require ack
}

func (m *coreNatsMsg) Nak() error {
	return nil // Core NATS doesn't support nak
}

// messageImpl wraps a message and provides generic typed access with lazy unmarshaling
type messageImpl[T any] struct {
	msg     message
	item    T
	itemErr error
	once    sync.Once
}

func MessageFrom[T any](msg message) Message[T] {
	return &messageImpl[T]{msg: msg}
}

func (m *messageImpl[T]) Item() (*T, error) {
	m.once.Do(func() {
		contentType := m.msg.Headers().Get(HeaderContentType)
		// T must be a proto.Message, so we need to convert it
		// This works because logspb.LogsData, tracespb.TracesData, metricspb.MetricsData all implement proto.Message
		if protoMsg, ok := any(&m.item).(proto.Message); ok {
			m.itemErr = Unmarshal(m.msg.Data(), contentType, protoMsg)
			if m.itemErr != nil {
				m.itemErr = fmt.Errorf("%w: %v", ErrUnmarshall, m.itemErr)
			}
		} else {
			panic("invalid type T for MessageFrom, this is always a programming error")
		}
	})
	return &m.item, m.itemErr
}

func (m *messageImpl[T]) Data() []byte {
	return m.msg.Data()
}

func (m *messageImpl[T]) Headers() nats.Header {
	return m.msg.Headers()
}

func (m *messageImpl[T]) Ack() error {
	return m.msg.Ack()
}

func (m *messageImpl[T]) Nak() error {
	return m.msg.Nak()
}

func (r *receiverImpl) subscribeJetStream(ctx context.Context, subject string, handler msgHandler) error {
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

	// Create backlog channel for this consumer
	// Buffered channel allows Consume callback to quickly spool messages without blocking
	backlog := make(chan message, r.config.backlogSize)
	r.msgBacklogs = append(r.msgBacklogs, backlog)

	// Start worker goroutine to process messages from backlog
	go func() {
		for msg := range backlog {
			handler(msg)
		}
	}()

	// Start consuming messages using the Consume API
	// Messages are spooled into backlog channel for async processing
	consumeCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		// Spool message into backlog channel
		// This returns quickly, preventing Consume callback from blocking
		backlog <- msg
	})
	if err != nil {
		return err
	}

	r.consumeContexts = append(r.consumeContexts, consumeCtx)

	return nil
}

func (r *receiverImpl) handleLogs(msg message) {
	r.wg.Add(1)
	defer r.wg.Done()

	ctx := context.Background()
	msgT := MessageFrom[logspb.LogsData](msg)
	handlerErr := r.logsMessageHandler(ctx, msgT)
	r.ackOrNak(msg, handlerErr)
}

func (r *receiverImpl) handleTraces(msg message) {
	r.wg.Add(1)
	defer r.wg.Done()

	ctx := context.Background()
	msgT := MessageFrom[tracespb.TracesData](msg)
	handlerErr := r.tracesMessageHandler(ctx, msgT)
	r.ackOrNak(msg, handlerErr)
}

func (r *receiverImpl) handleMetrics(msg message) {
	r.wg.Add(1)
	defer r.wg.Done()

	ctx := context.Background()
	msgT := MessageFrom[metricspb.MetricsData](msg)
	handlerErr := r.metricsMessageHandler(ctx, msgT)
	r.ackOrNak(msg, handlerErr)
}

// ackOrNak acknowledges or negatively acknowledges a message based on error.
func (r *receiverImpl) ackOrNak(msg message, err error) {
	if err != nil {
		_ = msg.Nak()
	} else {
		_ = msg.Ack()
	}
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
	msgBacklogs := r.msgBacklogs
	r.msgBacklogs = nil
	r.mu.Unlock()

	// Unsubscribe core NATS subscriptions
	for _, sub := range subs {
		_ = sub.Unsubscribe()
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

	// Close backlog channels to allow worker goroutines to exit
	for _, backlog := range msgBacklogs {
		close(backlog)
	}

	return nil
}
