package otelnats

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracespb "go.opentelemetry.io/proto/otlp/trace/v1"
)

type Receiver interface {
	Start(ctx context.Context) error
	Shutdown(ctx context.Context) error
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
	buffer chan Message

	// In-flight message tracking
	wg sync.WaitGroup
}

// Start begins receiving messages from NATS.
// Creates subscriptions for logs, traces, and metrics subjects and routes messages to configured handlers.
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
	if err := r.setup(ctx); err != nil {
		return err
	}

	r.started = true
	return nil
}

func (r *receiverImpl) setup(ctx context.Context) error {
	// Create shared backlog channel
	r.buffer = make(chan Message, r.config.backlogSize)

	// Determine base context for message handlers
	// Use configured base context, or default to Background() if not set
	baseCtx := r.config.baseContext
	if baseCtx == nil {
		baseCtx = context.Background()
	}

	// Start worker goroutine to route all messages by header
	// Uses base context independent of Start() to avoid premature cancellation
	go func() {
		for msg := range r.buffer {
			r.routeMessage(baseCtx, msg)
		}
	}()

	if r.config.jetstream != nil {
		return r.setupJetStream(ctx)
	}
	return r.setupCore(ctx)
}

func (r *receiverImpl) setupCore(_ context.Context) error {
	natsMsgHandler := func(msg *nats.Msg) {
		// Spool message into buffer
		select {
		case r.buffer <- &messageCoreWrapper{msg: msg}:
		default:
			r.errorHandler(fmt.Errorf("%w: size=%d", ErrBufferOverflow, len(r.buffer)))
		}
	}

	for _, subject := range r.config.buildSubjects() {
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
	}

	// Flush to ensure subscription is registered with server
	return r.conn.Flush()
}

func (r *receiverImpl) setupJetStream(ctx context.Context) error {
	var consumer jetstream.Consumer

	// Use pre-created consumer if available
	if r.config.consumer != nil {
		consumer = r.config.consumer
	} else {
		stream, err := r.config.jetstream.Stream(ctx, r.config.stream)
		if err != nil {
			return err
		}

		// Queue group: create ephemeral consumer with DeliverGroup
		// Each receiver instance gets its own consumer for load balancing
		if r.config.queueGroup != "" {
			consumer, err = stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
				AckPolicy:      jetstream.AckExplicitPolicy,
				AckWait:        r.config.ackWait,
				FilterSubjects: r.config.buildSubjects(),
				DeliverGroup:   r.config.queueGroup,
			})
			if err != nil {
				return err
			}
		} else {
			// Durable consumer: try to get existing or create new
			consumer, err = stream.Consumer(ctx, r.config.buildConsumerName())
			if err != nil {
				if !errors.Is(err, jetstream.ErrConsumerNotFound) {
					return err
				}
				// Consumer doesn't exist, create it
				consumer, err = stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
					Durable:        r.config.buildConsumerName(),
					AckPolicy:      jetstream.AckExplicitPolicy,
					AckWait:        r.config.ackWait,
					FilterSubjects: r.config.buildSubjects(),
				})
				if err != nil {
					return err
				}
			}
		}
	}

	// Start consuming messages using the Consume API
	// Messages are spooled into backlog channel for async processing
	consumeCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		// Spool message into backlog channel
		// This returns quickly, preventing Consume callback from blocking
		select {
		case r.buffer <- msg:
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

// routeMessage routes a message to the appropriate handler based on the Otel-Signal header.
// For messages with unconfigured handlers, sends NAK to trigger redelivery.
// For messages with unknown/missing signal headers, terminates the message.
func (r *receiverImpl) routeMessage(ctx context.Context, msg Message) {
	signal := msg.Headers().Get(HeaderOtelSignal)

	switch signal {
	case SignalLogs:
		if r.logsMessageHandler == nil {
			r.handleMissingHandler(msg, signal)
			return
		}
		msgSignal := messageSignalFrom[logspb.LogsData](msg)
		r.handleLogs(ctx, msgSignal)

	case SignalTraces:
		if r.tracesMessageHandler == nil {
			r.handleMissingHandler(msg, signal)
			return
		}
		msgSignal := messageSignalFrom[tracespb.TracesData](msg)
		r.handleTraces(ctx, msgSignal)

	case SignalMetrics:
		if r.metricsMessageHandler == nil {
			r.handleMissingHandler(msg, signal)
			return
		}
		msgSignal := messageSignalFrom[metricspb.MetricsData](msg)
		r.handleMetrics(ctx, msgSignal)

	default:
		// Unknown or missing signal header
		r.handleUnknownSignal(msg, signal)
	}
}

// handleMissingHandler terminates the message and reports the missing handler error.
// Combines Term() error with ErrNoHandlerForSignal into a single error handler call.
func (r *receiverImpl) handleMissingHandler(msg Message, signal string) {
	termErr := msg.Term()
	handlerErr := fmt.Errorf("%w: %s", ErrNoHandlerForSignal, signal)

	if termErr != nil && !isAlreadyAckedError(termErr) {
		// Combine both errors
		r.errorHandler(fmt.Errorf("%w (term failed: %v)", handlerErr, termErr))
	} else {
		r.errorHandler(handlerErr)
	}
}

// handleUnknownSignal terminates messages with unknown/missing signal headers
// and reports the error to prevent silent failures.
func (r *receiverImpl) handleUnknownSignal(msg Message, signal string) {
	var termErr error
	if jsmsg, ok := msg.(interface{ Term() error }); ok {
		termErr = jsmsg.Term()
	}

	unknownErr := fmt.Errorf("%w: %q", ErrUnknownSignal, signal)
	if termErr != nil && !isAlreadyAckedError(termErr) {
		// Combine both errors
		r.errorHandler(fmt.Errorf("%w (term failed: %v)", unknownErr, termErr))
	} else {
		r.errorHandler(unknownErr)
	}
}

func (r *receiverImpl) handleLogs(ctx context.Context, msg MessageSignal[logspb.LogsData]) {
	r.wg.Add(1)
	defer r.wg.Done()

	err := r.logsMessageHandler(ctx, msg)
	if err != nil {
		r.errorHandler(err)
	}
	r.ackOrNak(msg, err)
}

func (r *receiverImpl) handleTraces(ctx context.Context, msg MessageSignal[tracespb.TracesData]) {
	r.wg.Add(1)
	defer r.wg.Done()

	err := r.tracesMessageHandler(ctx, msg)
	if err != nil {
		r.errorHandler(err)
	}
	r.ackOrNak(msg, err)
}

func (r *receiverImpl) handleMetrics(ctx context.Context, msg MessageSignal[metricspb.MetricsData]) {
	r.wg.Add(1)
	defer r.wg.Done()

	err := r.metricsMessageHandler(ctx, msg)
	if err != nil {
		r.errorHandler(err)
	}
	r.ackOrNak(msg, err)
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
	msgBacklog := r.buffer
	r.buffer = nil
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
