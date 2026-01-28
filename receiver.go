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
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracespb "go.opentelemetry.io/proto/otlp/trace/v1"
	"golang.org/x/time/rate"
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

	// Core NATS: message buffer and worker goroutine
	buffer     chan Message
	workerDone chan struct{}

	// JetStream: fetch goroutine control
	fetchCancel context.CancelFunc
	fetchDone   chan struct{}
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

	// Validate rate limiting configuration
	if r.config.rateLimit > 0 {
		if r.config.jetstream == nil {
			return errors.New("rate limiting requires JetStream (use WithReceiverJetStream)")
		}
		if r.config.rateBurst <= 0 {
			return errors.New("rate burst must be > 0")
		}
		// Default batchSize to burst if not explicitly set
		if r.config.rateBatchSize == 0 {
			r.config.rateBatchSize = r.config.rateBurst
		}
		if r.config.rateBurst < r.config.rateBatchSize {
			return errors.New("rate burst must be >= batch size")
		}
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
	// JetStream uses Fetch API - no buffer needed
	if r.config.jetstream != nil {
		return r.setupJetStream(ctx)
	}

	// Core NATS: create buffer channel and worker goroutine
	r.buffer = make(chan Message, r.config.backlogSize)

	// Determine base context for message handlers
	baseCtx := r.config.baseContext
	if baseCtx == nil {
		baseCtx = context.Background()
	}

	// Start worker goroutine to route all messages by header.
	// buf and workerDone are captured locally to avoid data races with Shutdown.
	buf := r.buffer
	r.workerDone = make(chan struct{})
	workerDone := r.workerDone
	go func() {
		defer close(workerDone)
		for msg := range buf {
			r.routeMessage(baseCtx, msg)
		}
	}()

	return r.setupCore(ctx)
}

func (r *receiverImpl) setupCore(_ context.Context) error {
	buf := r.buffer // capture locally to avoid data race with Shutdown setting r.buffer = nil
	natsMsgHandler := func(msg *nats.Msg) {
		// Spool message into buffer
		select {
		case buf <- &messageCoreWrapper{msg: msg}:
		default:
			r.errorHandler(fmt.Errorf("%w: size=%d", ErrBufferOverflow, len(buf)))
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

		// Try to get existing durable consumer, or create if it doesn't exist
		// NOTE: We only support pull consumers (via Consume/Fetch API), not push consumers.
		// For load balancing across multiple receiver instances, use the same
		// consumer name (WithReceiverConsumerName) on all instances.
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

	return r.setupJetStreamFetch(consumer)
}

// setupJetStreamFetch sets up pull-based consumption using the Fetch API.
// When rate limiting is enabled, tokens are acquired before each fetch.
// Messages are processed directly without buffering.
func (r *receiverImpl) setupJetStreamFetch(consumer jetstream.Consumer) error {
	// Create rate limiter only if rate limiting is enabled
	var limiter *rate.Limiter
	if r.config.rateLimit > 0 {
		limiter = rate.NewLimiter(rate.Limit(r.config.rateLimit), r.config.rateBurst)
	}

	baseCtx := r.config.baseContext
	if baseCtx == nil {
		baseCtx = context.Background()
	}

	var fetchCtx context.Context
	fetchCtx, r.fetchCancel = context.WithCancel(context.Background())
	r.fetchDone = make(chan struct{})

	batchSize := r.config.rateBatchSize
	if batchSize == 0 {
		batchSize = defaultFetchBatchSize
	}
	fetchTimeout := r.calculateFetchTimeout(batchSize)

	go func() {
		defer close(r.fetchDone)
		for {
			// Acquire tokens BEFORE fetching (only if rate limiting enabled)
			if limiter != nil {
				if err := limiter.WaitN(fetchCtx, batchSize); err != nil {
					return // context cancelled
				}
			}

			batch, err := consumer.Fetch(batchSize, jetstream.FetchMaxWait(fetchTimeout))
			if err != nil {
				select {
				case <-fetchCtx.Done():
					return
				default:
					// ErrNoMessages is normal when no messages available - not an error
					if !errors.Is(err, jetstream.ErrNoMessages) {
						r.errorHandler(err)
						time.Sleep(fetchRetryDelay)
					}
					continue
				}
			}

			// Process messages - check for shutdown while waiting
			msgs := batch.Messages()
		msgLoop:
			for {
				select {
				case <-fetchCtx.Done():
					// Drain and NAK any remaining messages
					for msg := range msgs {
						if err := msg.Nak(); err != nil {
							r.errorHandler(err)
						}
					}
					return
				case msg, ok := <-msgs:
					if !ok {
						break msgLoop // batch complete
					}
					r.routeMessage(baseCtx, msg)
				}
			}
		}
	}()

	return nil
}

// calculateFetchTimeout calculates the timeout for Fetch() calls.
// When rate limiting is enabled, allows enough time to consume the batch at the configured rate.
// Always capped at ackWait to prevent messages from timing out.
func (r *receiverImpl) calculateFetchTimeout(batchSize int) time.Duration {
	var timeout time.Duration

	if r.config.rateLimit > 0 {
		// Time to consume batch at configured rate, plus buffer for network latency
		timeout = time.Duration(float64(batchSize)/r.config.rateLimit*float64(time.Second)) + minFetchTimeout
	} else {
		// No rate limit - use default fetch timeout for responsive shutdown
		timeout = defaultFetchTimeout
	}

	// Cap at ackWait to prevent messages from timing out
	if timeout > r.config.ackWait {
		timeout = r.config.ackWait
	}

	// Ensure minimum timeout for network operations
	if timeout < minFetchTimeout {
		timeout = minFetchTimeout
	}

	return timeout
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
	err := r.logsMessageHandler(ctx, msg)
	if err != nil {
		r.errorHandler(err)
	}
	r.ackOrNak(msg, err)
}

func (r *receiverImpl) handleTraces(ctx context.Context, msg MessageSignal[tracespb.TracesData]) {
	err := r.tracesMessageHandler(ctx, msg)
	if err != nil {
		r.errorHandler(err)
	}
	r.ackOrNak(msg, err)
}

func (r *receiverImpl) handleMetrics(ctx context.Context, msg MessageSignal[metricspb.MetricsData]) {
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
	msgBacklog := r.buffer
	r.buffer = nil
	workerDone := r.workerDone
	r.workerDone = nil
	fetchCancel := r.fetchCancel
	r.fetchCancel = nil
	fetchDone := r.fetchDone
	r.fetchDone = nil
	r.mu.Unlock()

	// Unsubscribe core NATS subscriptions
	for _, sub := range subs {
		if err := sub.Unsubscribe(); err != nil {
			r.errorHandler(err)
		}
	}

	// Stop JetStream fetch goroutine
	if fetchCancel != nil {
		fetchCancel()
	}

	// Wait for fetch goroutine to exit
	if fetchDone != nil {
		select {
		case <-fetchDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Close the buffer channel so the worker goroutine drains remaining
	// messages and exits. This must happen after Drain to avoid sending
	// on a closed channel in the consume callback.
	if msgBacklog != nil {
		close(msgBacklog)
	}

	// Wait for the worker goroutine to finish processing all buffered
	// messages. Since all handlers run synchronously in the worker,
	// workerDone closing guarantees all messages have been fully processed.
	if workerDone != nil {
		select {
		case <-workerDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}
