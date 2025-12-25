package otelnats

import (
	"context"
	"errors"
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

// LogsHandler is called when log data is received.
type LogsHandler func(context.Context, *logspb.LogsData) error

// TracesHandler is called when trace data is received.
type TracesHandler func(context.Context, *tracespb.TracesData) error

// MetricsHandler is called when metric data is received.
type MetricsHandler func(context.Context, *metricspb.MetricsData) error

// Receiver consumes OTLP telemetry from NATS subjects.
type Receiver struct {
	conn   *nats.Conn
	config *receiverConfig

	mu       sync.Mutex
	started  bool
	shutdown bool

	// Handlers
	logsHandler    LogsHandler
	tracesHandler  TracesHandler
	metricsHandler MetricsHandler

	// Channels (lazily created)
	logsCh    chan *logspb.LogsData
	tracesCh  chan *tracespb.TracesData
	metricsCh chan *metricspb.MetricsData

	// Core NATS subscriptions
	subs []*nats.Subscription

	// JetStream consumers and their contexts
	consumeContexts []jetstream.ConsumeContext

	// In-flight message tracking
	wg sync.WaitGroup
}

type receiverConfig struct {
	subjectPrefix string
	subjectSuffix string
	queueGroup    string
	chanBufSize   int

	// JetStream options
	jetstream      jetstream.JetStream
	stream         string
	consumerName   string
	consumer       jetstream.Consumer // pre-created consumer from WithConsumer
	ackWait        time.Duration
	fetchBatchSize int
	fetchTimeout   time.Duration
}

func defaultReceiverConfig() *receiverConfig {
	return &receiverConfig{
		subjectPrefix:  defaultSubjectPrefix,
		queueGroup:     "",
		chanBufSize:    100,
		ackWait:        30 * time.Second,
		fetchBatchSize: 10,
		fetchTimeout:   5 * time.Second,
	}
}

// ReceiverOption configures a Receiver.
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

// WithQueueGroup sets the queue group for load-balanced consumption.
// When multiple receivers use the same queue group, each message is
// delivered to only one receiver in the group.
func WithQueueGroup(group string) ReceiverOption {
	return func(c *receiverConfig) {
		c.queueGroup = group
	}
}

// WithChannelBufferSize sets the buffer size for channel-based APIs.
// The default is 100.
func WithChannelBufferSize(size int) ReceiverOption {
	return func(c *receiverConfig) {
		c.chanBufSize = size
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

// WithConsumerName sets the durable consumer name for JetStream subscriptions.
// When set, the consumer state is persisted, allowing receivers to resume
// from where they left off after restart.
// Only applies when JetStream is enabled via [WithReceiverJetStream].
func WithConsumerName(name string) ReceiverOption {
	return func(c *receiverConfig) {
		c.consumerName = name
	}
}

// WithConsumer binds the receiver to an existing JetStream consumer.
// The consumer must already exist (created via the native jetstream API).
// This option stores the consumer directly, avoiding lookups during subscription.
// Only applies when JetStream is enabled via [WithReceiverJetStream].
func WithConsumer(consumer jetstream.Consumer) ReceiverOption {
	return func(c *receiverConfig) {
		c.consumer = consumer
		c.consumerName = consumer.CachedInfo().Name
	}
}

// WithAckWait sets the acknowledgment timeout for JetStream messages.
// If a message is not acknowledged within this duration, it will be redelivered.
// The default is 30 seconds.
// Only applies when JetStream is enabled via [WithReceiverJetStream].
func WithAckWait(d time.Duration) ReceiverOption {
	return func(c *receiverConfig) {
		c.ackWait = d
	}
}

// WithFetchBatchSize sets the number of messages to fetch in each batch.
// The default is 10.
// Only applies when JetStream is enabled via [WithReceiverJetStream].
func WithFetchBatchSize(n int) ReceiverOption {
	return func(c *receiverConfig) {
		c.fetchBatchSize = n
	}
}

// WithFetchTimeout sets the maximum time to wait for messages when fetching.
// The default is 5 seconds.
// Only applies when JetStream is enabled via [WithReceiverJetStream].
func WithFetchTimeout(d time.Duration) ReceiverOption {
	return func(c *receiverConfig) {
		c.fetchTimeout = d
	}
}

// NewReceiver creates a new receiver that consumes from NATS.
func NewReceiver(nc *nats.Conn, opts ...ReceiverOption) (*Receiver, error) {
	if nc == nil {
		return nil, errNilConnection
	}

	cfg := defaultReceiverConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	return &Receiver{
		conn:   nc,
		config: cfg,
	}, nil
}

// OnLogs registers a handler for log data.
// The handler is called for each received log message.
// Must be called before Start.
func (r *Receiver) OnLogs(fn LogsHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logsHandler = fn
}

// OnTraces registers a handler for trace data.
// The handler is called for each received trace message.
// Must be called before Start.
func (r *Receiver) OnTraces(fn TracesHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tracesHandler = fn
}

// OnMetrics registers a handler for metric data.
// The handler is called for each received metric message.
// Must be called before Start.
func (r *Receiver) OnMetrics(fn MetricsHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metricsHandler = fn
}

// Logs returns a channel that receives log data.
// The channel is created on first call and closed on Shutdown.
// Must be called before Start.
func (r *Receiver) Logs() <-chan *logspb.LogsData {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.logsCh == nil {
		r.logsCh = make(chan *logspb.LogsData, r.config.chanBufSize)
	}
	return r.logsCh
}

// Traces returns a channel that receives trace data.
// The channel is created on first call and closed on Shutdown.
// Must be called before Start.
func (r *Receiver) Traces() <-chan *tracespb.TracesData {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tracesCh == nil {
		r.tracesCh = make(chan *tracespb.TracesData, r.config.chanBufSize)
	}
	return r.tracesCh
}

// Metrics returns a channel that receives metric data.
// The channel is created on first call and closed on Shutdown.
// Must be called before Start.
func (r *Receiver) Metrics() <-chan *metricspb.MetricsData {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.metricsCh == nil {
		r.metricsCh = make(chan *metricspb.MetricsData, r.config.chanBufSize)
	}
	return r.metricsCh
}

// Start begins receiving messages from NATS.
// It subscribes to subjects based on registered handlers and channels.
func (r *Receiver) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.shutdown {
		return errReceiverShutdown
	}
	if r.started {
		return nil
	}

	// Subscribe to logs if handler or channel is set
	if r.logsHandler != nil || r.logsCh != nil {
		if err := r.subscribe(ctx, SignalLogs, r.handleLogs); err != nil {
			return err
		}
	}

	// Subscribe to traces if handler or channel is set
	if r.tracesHandler != nil || r.tracesCh != nil {
		if err := r.subscribe(ctx, SignalTraces, r.handleTraces); err != nil {
			return err
		}
	}

	// Subscribe to metrics if handler or channel is set
	if r.metricsHandler != nil || r.metricsCh != nil {
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

// unmarshal decodes message data based on Content-Type header.
// Supports both protobuf (default) and JSON encoding.
// This is a thin wrapper around the exported Unmarshal function.
func unmarshal(msg message, v proto.Message) error {
	contentType := msg.Headers().Get(HeaderContentType)
	return Unmarshal(msg.Data(), contentType, v)
}

// msgHandler is a generic handler for messages
type msgHandler func(message)

func (r *Receiver) subscribe(ctx context.Context, signal string, handler msgHandler) error {
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

func (r *Receiver) subscribeJetStream(ctx context.Context, subject string, handler msgHandler) error {
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

	// Start consuming messages using the Consume API
	// This provides continuous message delivery with built-in lifecycle management
	consumeCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		handler(msg)
	})
	if err != nil {
		return err
	}

	r.consumeContexts = append(r.consumeContexts, consumeCtx)

	return nil
}

func (r *Receiver) handleLogs(msg message) {
	r.wg.Add(1)
	defer r.wg.Done()

	var data logspb.LogsData
	if err := unmarshal(msg, &data); err != nil {
		// Malformed message - ack to prevent redelivery of bad data
		_ = msg.Ack()
		return
	}

	ctx := context.Background()
	var handlerErr error

	// Call handler if set
	if r.logsHandler != nil {
		handlerErr = r.logsHandler(ctx, &data)
	}

	// Send to channel if set (non-blocking)
	if r.logsCh != nil {
		select {
		case r.logsCh <- &data:
		default:
			// Channel full, drop message
		}
	}

	// Ack/Nak for JetStream
	r.ackOrNak(msg, handlerErr)
}

func (r *Receiver) handleTraces(msg message) {
	r.wg.Add(1)
	defer r.wg.Done()

	var data tracespb.TracesData
	if err := unmarshal(msg, &data); err != nil {
		_ = msg.Ack()
		return
	}

	ctx := context.Background()
	var handlerErr error

	if r.tracesHandler != nil {
		handlerErr = r.tracesHandler(ctx, &data)
	}

	if r.tracesCh != nil {
		select {
		case r.tracesCh <- &data:
		default:
		}
	}

	r.ackOrNak(msg, handlerErr)
}

func (r *Receiver) handleMetrics(msg message) {
	r.wg.Add(1)
	defer r.wg.Done()

	var data metricspb.MetricsData
	if err := unmarshal(msg, &data); err != nil {
		_ = msg.Ack()
		return
	}

	ctx := context.Background()
	var handlerErr error

	if r.metricsHandler != nil {
		handlerErr = r.metricsHandler(ctx, &data)
	}

	if r.metricsCh != nil {
		select {
		case r.metricsCh <- &data:
		default:
		}
	}

	r.ackOrNak(msg, handlerErr)
}

// ackOrNak acknowledges or negatively acknowledges a message based on error.
func (r *Receiver) ackOrNak(msg message, err error) {
	if err != nil {
		_ = msg.Nak()
	} else {
		_ = msg.Ack()
	}
}

// Shutdown stops the receiver and waits for in-flight messages to complete.
func (r *Receiver) Shutdown(ctx context.Context) error {
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

	// Close channels
	r.mu.Lock()
	if r.logsCh != nil {
		close(r.logsCh)
	}
	if r.tracesCh != nil {
		close(r.tracesCh)
	}
	if r.metricsCh != nil {
		close(r.metricsCh)
	}
	r.mu.Unlock()

	return nil
}
