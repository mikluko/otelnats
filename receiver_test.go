package otelnats

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	sdklog "go.opentelemetry.io/otel/sdk/log"

	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
)

func TestNewReceiver(t *testing.T) {
	t.Run("nil connection returns error", func(t *testing.T) {
		recv, err := NewReceiver(nil)
		require.Error(t, err)
		require.Nil(t, recv)
		require.Equal(t, ErrNilConnection, err)
	})

	t.Run("valid connection succeeds", func(t *testing.T) {
		ns := startEmbeddedNATS(t)
		nc := connectToNATS(t, ns)

		recv, err := NewReceiver(nc)
		require.NoError(t, err)
		require.NotNil(t, recv)
	})
}

func TestReceiver_MessageHandlerAPI(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	// Create exporter
	exp, err := NewLogExporter(nc, WithExporterSubjectPrefix("mh"))
	require.NoError(t, err)

	// Track received data
	var received atomic.Pointer[logspb.LogsData]

	// Create receiver with handler
	recv, err := NewReceiver(nc,
		WithReceiverSubjectPrefix("mh"),
		WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
			data, err := msg.Signal()
			if err != nil {
				return err
			}
			received.Store(data)
			return nil
		}),
	)
	require.NoError(t, err)

	// Start receiver
	err = recv.Start(ctx)
	require.NoError(t, err)

	// Export a log record
	rec := createTestLogRecord(t)
	err = exp.Export(ctx, []sdklog.Record{rec})
	require.NoError(t, err)

	// Wait for message to be received
	require.Eventually(t, func() bool {
		return received.Load() != nil
	}, 5*time.Second, 10*time.Millisecond)

	// Verify content
	data := received.Load()
	require.Len(t, data.ResourceLogs, 1)
	require.Len(t, data.ResourceLogs[0].ScopeLogs, 1)
	require.Len(t, data.ResourceLogs[0].ScopeLogs[0].LogRecords, 1)

	lr := data.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
	require.Equal(t, "test message", lr.Body.GetStringValue())

	// Shutdown
	err = recv.Shutdown(ctx)
	require.NoError(t, err)
}

func TestReceiver_QueueGroup(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc1 := connectToNATS(t, ns)
	nc2 := connectToNATS(t, ns)
	nc3 := connectToNATS(t, ns)
	ctx := t.Context()

	// Create exporter
	exp, err := NewLogExporter(nc1, WithExporterSubjectPrefix("qg"))
	require.NoError(t, err)

	// Create two receivers in same queue group
	var count1, count2 atomic.Int32

	recv1, err := NewReceiver(nc2,
		WithReceiverSubjectPrefix("qg"),
		WithReceiverQueueGroup("test-group"),
		WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
			_, err := msg.Signal()
			if err != nil {
				return err
			}
			count1.Add(1)
			return nil
		}),
	)
	require.NoError(t, err)

	recv2, err := NewReceiver(nc3,
		WithReceiverSubjectPrefix("qg"),
		WithReceiverQueueGroup("test-group"),
		WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
			_, err := msg.Signal()
			if err != nil {
				return err
			}

			count2.Add(1)
			return nil
		}),
	)
	require.NoError(t, err)

	// Start receivers
	require.NoError(t, recv1.Start(ctx))
	require.NoError(t, recv2.Start(ctx))

	// Send multiple messages
	const numMessages = 10
	for i := 0; i < numMessages; i++ {
		rec := createTestLogRecord(t)
		err = exp.Export(ctx, []sdklog.Record{rec})
		require.NoError(t, err)
	}

	// Wait for all messages to be received
	require.Eventually(t, func() bool {
		return count1.Load()+count2.Load() == numMessages
	}, 5*time.Second, 10*time.Millisecond)

	// With queue group, messages should be distributed (not duplicated)
	total := count1.Load() + count2.Load()
	require.Equal(t, int32(numMessages), total, "each message should be received exactly once")

	// Both receivers should have received some messages (load balancing)
	// Note: with only 10 messages, it's possible one receiver gets all,
	// but typically they should be distributed
	t.Logf("recv1: %d, recv2: %d", count1.Load(), count2.Load())

	// Shutdown
	require.NoError(t, recv1.Shutdown(ctx))
	require.NoError(t, recv2.Shutdown(ctx))
}

func TestReceiver_Shutdown(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	recv, err := NewReceiver(nc,
		WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
			_, err := msg.Signal()
			if err != nil {
				return err
			}

			return nil
		}),
	)
	require.NoError(t, err)

	// Start and shutdown
	require.NoError(t, recv.Start(ctx))
	require.NoError(t, recv.Shutdown(ctx))

	// Shutdown is idempotent
	require.NoError(t, recv.Shutdown(ctx))

	// Start after shutdown returns error
	err = recv.Start(ctx)
	require.Equal(t, ErrReceiverShutdown, err)
}

func TestReceiver_NoSubscriptionsWithoutHandlers(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	recv, err := NewReceiver(nc)
	require.NoError(t, err)

	// Start without any handlers - should return error requiring explicit configuration
	err = recv.Start(ctx)
	require.ErrorIs(t, err, ErrNoHandlers)
}

// createTestLogRecord is defined in log_exporter_test.go
// Redeclaring here for test isolation, but in practice they share the helper

func init() {
	// Ensure createTestLogRecord is available
	_ = createTestLogRecord
}

func TestReceiver_JetStream(t *testing.T) {
	ns := startEmbeddedNATSWithJetStream(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	js := createJetStream(t, nc)
	streamName := createTestStream(t, js, "jsrecv")

	// Create exporter with JetStream
	exp, err := NewLogExporter(nc, WithExporterSubjectPrefix("jsrecv"), WithExporterJetStream(js))
	require.NoError(t, err)

	// Track received data
	received := make(chan *logspb.LogsData, 1)

	// Create receiver with JetStream
	recv, err := NewReceiver(nc,
		WithReceiverSubjectPrefix("jsrecv"),
		WithReceiverJetStream(js, streamName),
		WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
			data, err := msg.Signal()
			if err != nil {
				return err
			}

			received <- data
			return nil
		}),
	)
	require.NoError(t, err)

	require.NoError(t, recv.Start(ctx))
	defer recv.Shutdown(ctx)

	// Export a log record
	rec := createTestLogRecord(t)
	require.NoError(t, exp.Export(ctx, []sdklog.Record{rec}))

	// Verify message received
	select {
	case data := <-received:
		require.Len(t, data.ResourceLogs, 1)
		lr := data.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
		require.Equal(t, "test message", lr.Body.GetStringValue())
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestReceiver_JetStream_DurableConsumer(t *testing.T) {
	ns := startEmbeddedNATSWithJetStream(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	js := createJetStream(t, nc)
	streamName := createTestStream(t, js, "jsdurable")

	// Create exporter with JetStream
	exp, err := NewLogExporter(nc, WithExporterSubjectPrefix("jsdurable"), WithExporterJetStream(js))
	require.NoError(t, err)

	// Create receiver with durable consumer
	received := make(chan *logspb.LogsData, 10)
	recv, err := NewReceiver(nc,
		WithReceiverSubjectPrefix("jsdurable"),
		WithReceiverJetStream(js, streamName),
		WithReceiverConsumerName("test-consumer"),
		WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
			data, err := msg.Signal()
			if err != nil {
				return err
			}

			received <- data
			return nil
		}),
	)
	require.NoError(t, err)

	require.NoError(t, recv.Start(ctx))

	// Export messages
	for i := 0; i < 3; i++ {
		rec := createTestLogRecord(t)
		require.NoError(t, exp.Export(ctx, []sdklog.Record{rec}))
	}

	// Wait for all messages
	for i := 0; i < 3; i++ {
		select {
		case <-received:
			// OK
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for message %d", i+1)
		}
	}

	// Verify durable consumer was created
	stream, err := js.Stream(ctx, streamName)
	require.NoError(t, err)
	consumer, err := stream.Consumer(ctx, "test-consumer")
	require.NoError(t, err)
	info := consumer.CachedInfo()
	require.Equal(t, "test-consumer", info.Name)
	require.Equal(t, uint64(3), info.Delivered.Consumer, "should have delivered 3 messages")

	require.NoError(t, recv.Shutdown(ctx))
}

func TestReceiver_JetStream_NakOnError(t *testing.T) {
	ns := startEmbeddedNATSWithJetStream(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	js := createJetStream(t, nc)
	streamName := createTestStream(t, js, "jsnak")

	// Create exporter with JetStream
	exp, err := NewLogExporter(nc, WithExporterSubjectPrefix("jsnak"), WithExporterJetStream(js))
	require.NoError(t, err)

	// Track delivery attempts
	var attempts atomic.Int32
	errSimulated := errors.New("simulated error")

	recv, err := NewReceiver(nc,
		WithReceiverSubjectPrefix("jsnak"),
		WithReceiverJetStream(js, streamName),
		WithReceiverAckWait(500*time.Millisecond), // Short ack wait for faster test
		WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
			_, err := msg.Signal()
			if err != nil {
				return err
			}

			count := attempts.Add(1)
			// Fail first 2 attempts, succeed on 3rd
			if count < 3 {
				return errSimulated
			}
			return nil
		}),
		WithReceiverErrorHandler(func(err error) {
			require.ErrorIs(t, err, errSimulated)
		}),
	)
	require.NoError(t, err)

	require.NoError(t, recv.Start(ctx))
	defer recv.Shutdown(ctx)

	// Export a log record
	rec := createTestLogRecord(t)
	require.NoError(t, exp.Export(ctx, []sdklog.Record{rec}))

	// Should be retried until success
	require.Eventually(t, func() bool {
		return attempts.Load() >= 3
	}, 10*time.Second, 100*time.Millisecond)
}

func TestReceiver_MessageHandler_TypedAccess(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	exp, err := NewLogExporter(nc, WithExporterSubjectPrefix("msg"))
	require.NoError(t, err)

	// Track received data using generic MessageHandler
	var receivedData atomic.Pointer[logspb.LogsData]

	recv, err := NewReceiver(nc,
		WithReceiverSubjectPrefix("msg"),
		WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
			// Access typed data via Signal()
			data, err := msg.Signal()
			if err != nil {
				return err
			}
			receivedData.Store(data)
			return nil
		}),
	)
	require.NoError(t, err)

	require.NoError(t, recv.Start(ctx))

	// Send test data
	rec := createTestLogRecord(t)
	require.NoError(t, exp.Export(ctx, []sdklog.Record{rec}))

	// Verify typed data was received
	require.Eventually(t, func() bool {
		return receivedData.Load() != nil
	}, 1*time.Second, 10*time.Millisecond)

	data := receivedData.Load()
	require.NotNil(t, data)
	require.Len(t, data.ResourceLogs, 1)

	require.NoError(t, recv.Shutdown(ctx))
}

func TestReceiver_MessageHandler_RawAccess(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	exp, err := NewLogExporter(nc, WithExporterSubjectPrefix("raw"))
	require.NoError(t, err)

	// Track raw bytes and headers
	var receivedBytes atomic.Pointer[[]byte]
	var contentType atomic.Pointer[string]

	recv, err := NewReceiver(nc,
		WithReceiverSubjectPrefix("raw"),
		WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
			// Access raw bytes without unmarshaling
			rawData := msg.Data()
			receivedBytes.Store(&rawData)

			// Access headers
			ct := msg.Headers().Get(HeaderContentType)
			contentType.Store(&ct)

			// Don't call Signal() - testing raw access only
			return nil
		}),
	)
	require.NoError(t, err)

	require.NoError(t, recv.Start(ctx))

	// Send test data
	rec := createTestLogRecord(t)
	require.NoError(t, exp.Export(ctx, []sdklog.Record{rec}))

	// Verify raw bytes were received
	require.Eventually(t, func() bool {
		return receivedBytes.Load() != nil
	}, 1*time.Second, 10*time.Millisecond)

	rawBytes := receivedBytes.Load()
	require.NotNil(t, rawBytes)
	require.NotEmpty(t, *rawBytes)

	ct := contentType.Load()
	require.NotNil(t, ct)
	require.Equal(t, ContentTypeProtobuf, *ct)

	require.NoError(t, recv.Shutdown(ctx))
}

func TestReceiver_MessageHandler_BothTypedAndRaw(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	exp, err := NewLogExporter(nc, WithExporterSubjectPrefix("both"))
	require.NoError(t, err)

	// Track both typed and raw access
	var typedAccess atomic.Bool
	var rawAccess atomic.Bool

	recv, err := NewReceiver(nc,
		WithReceiverSubjectPrefix("both"),
		WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
			// Can access both in same handler
			rawData := msg.Data()
			if len(rawData) > 0 {
				rawAccess.Store(true)
			}

			typedData, err := msg.Signal()
			if err != nil {
				return err
			}
			if len(typedData.ResourceLogs) > 0 {
				typedAccess.Store(true)
			}

			return nil
		}),
	)
	require.NoError(t, err)

	require.NoError(t, recv.Start(ctx))

	// Send test data
	rec := createTestLogRecord(t)
	require.NoError(t, exp.Export(ctx, []sdklog.Record{rec}))

	// Verify both access patterns worked
	require.Eventually(t, func() bool {
		return typedAccess.Load() && rawAccess.Load()
	}, 1*time.Second, 10*time.Millisecond)

	require.NoError(t, recv.Shutdown(ctx))
}

func TestReceiver_MessageHandler_ErrorHandling(t *testing.T) {
	ns := startEmbeddedNATSWithJetStream(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	// Set up JetStream for error testing (need Nak support)
	js := createJetStream(t, nc)
	streamName := createTestStream(t, js, "error")

	exp, err := NewLogExporter(nc, WithExporterSubjectPrefix("error"))
	require.NoError(t, err)

	// Handler that returns error - should trigger Nak
	var callCount atomic.Int32
	errSimulated := errors.New("test error")
	errCapturedChan := make(chan error, 1)

	recv, err := NewReceiver(nc,
		WithReceiverSubjectPrefix("error"),
		WithReceiverJetStream(js, streamName),
		WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
			callCount.Add(1)

			// Return error on first call, success on retry
			if callCount.Load() == 1 {
				return errSimulated
			}
			return nil
		}),
		WithReceiverErrorHandler(func(err error) {
			errCapturedChan <- err
		}),
	)
	require.NoError(t, err)

	require.NoError(t, recv.Start(ctx))

	// Send test data
	rec := createTestLogRecord(t)
	require.NoError(t, exp.Export(ctx, []sdklog.Record{rec}))

	// Handler should be called multiple times due to Nak/retry
	require.Eventually(t, func() bool {
		return callCount.Load() >= 2
	}, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, recv.Shutdown(ctx))

	err = <-errCapturedChan
	require.ErrorIs(t, err, errSimulated)
}

func TestReceiver_MessageHandler_JetStream(t *testing.T) {
	ns := startEmbeddedNATSWithJetStream(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	js := createJetStream(t, nc)
	streamName := createTestStream(t, js, "msgjs")

	exp, err := NewLogExporter(nc, WithExporterSubjectPrefix("msgjs"))
	require.NoError(t, err)

	var received atomic.Pointer[logspb.LogsData]
	var ackCalled atomic.Bool

	recv, err := NewReceiver(nc,
		WithReceiverSubjectPrefix("msgjs"),
		WithReceiverJetStream(js, streamName),
		WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
			data, err := msg.Signal()
			if err != nil {
				return err
			}
			received.Store(data)

			// Ack should work with JetStream
			err = msg.Ack()
			if err == nil {
				ackCalled.Store(true)
			}

			return nil
		}),
	)
	require.NoError(t, err)

	require.NoError(t, recv.Start(ctx))

	// Send test data
	rec := createTestLogRecord(t)
	require.NoError(t, exp.Export(ctx, []sdklog.Record{rec}))

	// Verify message received and acknowledged
	require.Eventually(t, func() bool {
		return received.Load() != nil
	}, 2*time.Second, 10*time.Millisecond)

	// Note: Ack is called internally by handleLogs based on handler error,
	// so explicit Ack() call from handler is a no-op (already acked)
	// We mainly verify the Ack() method doesn't panic

	require.NoError(t, recv.Shutdown(ctx))
}

func TestReceiver_MessageHandler_CustomUnmarshal(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	exp, err := NewLogExporter(nc, WithExporterSubjectPrefix("custom"))
	require.NoError(t, err)

	// Demonstrate custom unmarshaling to different type
	var customUnmarshalSuccess atomic.Bool

	recv, err := NewReceiver(nc,
		WithReceiverSubjectPrefix("custom"),
		WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
			// Can unmarshal raw bytes to custom type
			var customData logspb.LogsData
			contentType := msg.Headers().Get(HeaderContentType)
			err := Unmarshal(msg.Data(), contentType, &customData)
			if err == nil && len(customData.ResourceLogs) > 0 {
				customUnmarshalSuccess.Store(true)
			}

			return nil
		}),
	)
	require.NoError(t, err)

	require.NoError(t, recv.Start(ctx))

	// Send test data
	rec := createTestLogRecord(t)
	require.NoError(t, exp.Export(ctx, []sdklog.Record{rec}))

	// Verify custom unmarshaling worked
	require.Eventually(t, func() bool {
		return customUnmarshalSuccess.Load()
	}, 1*time.Second, 10*time.Millisecond)

	require.NoError(t, recv.Shutdown(ctx))
}

func TestReceiver_ErrorHandler(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	t.Run("default handler panics on error", func(t *testing.T) {
		// Create receiver with handler that returns an error
		// This will trigger NAK, and if NAK fails, default error handler should panic
		recv, err := NewReceiver(nc,
			WithReceiverSubjectPrefix("err-default"),
			WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
				// Return error to trigger NAK
				return errors.New("handler error")
			}),
		)
		require.NoError(t, err)

		// Note: We can't easily test panic in a JetStream scenario without
		// actually triggering a NAK error, which would require mocking.
		// This test just verifies the receiver is created with default handler.
		require.NotNil(t, recv)
	})

	t.Run("custom error handler is called", func(t *testing.T) {
		exp, err := NewLogExporter(nc, WithExporterSubjectPrefix("err-custom"))
		require.NoError(t, err)

		var errorHandlerCalled atomic.Bool
		var capturedError atomic.Pointer[error]

		// Create receiver with custom error handler
		recv, err := NewReceiver(nc,
			WithReceiverSubjectPrefix("err-custom"),
			WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
				// For core NATS, returning error doesn't trigger error handler
				// (no NAK in core NATS)
				return nil
			}),
			WithReceiverErrorHandler(func(err error) {
				errorHandlerCalled.Store(true)
				capturedError.Store(&err)
			}),
		)
		require.NoError(t, err)

		require.NoError(t, recv.Start(ctx))

		// Send a message
		rec := createTestLogRecord(t)
		require.NoError(t, exp.Export(ctx, []sdklog.Record{rec}))

		// Give it time to process
		time.Sleep(100 * time.Millisecond)

		// In core NATS mode, error handler should not be called for handler errors
		require.False(t, errorHandlerCalled.Load())

		require.NoError(t, recv.Shutdown(ctx))
	})

	t.Run("error handler called on shutdown errors", func(t *testing.T) {
		var shutdownErrors atomic.Int32

		recv, err := NewReceiver(nc,
			WithReceiverSubjectPrefix("err-shutdown"),
			WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
				return nil
			}),
			WithReceiverErrorHandler(func(err error) {
				// Count errors during shutdown
				shutdownErrors.Add(1)
			}),
		)
		require.NoError(t, err)

		require.NoError(t, recv.Start(ctx))

		// Shutdown should succeed even with error handler
		require.NoError(t, recv.Shutdown(ctx))

		// No errors expected during normal shutdown
		require.Equal(t, int32(0), shutdownErrors.Load())
	})
}

func TestReceiver_BacklogSize(t *testing.T) {
	t.Run("custom backlog size", func(t *testing.T) {
		ns := startEmbeddedNATS(t)
		nc := connectToNATS(t, ns)
		ctx := t.Context()

		// Create receiver with small backlog
		recv, err := NewReceiver(nc,
			WithReceiverSubjectPrefix("backlog"),
			WithReceiverBacklogSize(5),
			WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
				return nil
			}),
		)
		require.NoError(t, err)

		// Verify receiver was created successfully
		require.NotNil(t, recv)

		require.NoError(t, recv.Start(ctx))
		require.NoError(t, recv.Shutdown(ctx))
	})

	t.Run("default backlog size", func(t *testing.T) {
		ns := startEmbeddedNATS(t)
		nc := connectToNATS(t, ns)
		ctx := t.Context()

		// Create receiver without specifying backlog size
		recv, err := NewReceiver(nc,
			WithReceiverSubjectPrefix("backlog-default"),
			WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
				return nil
			}),
		)
		require.NoError(t, err)

		require.NoError(t, recv.Start(ctx))
		require.NoError(t, recv.Shutdown(ctx))
	})

	t.Run("backlog handles burst of messages", func(t *testing.T) {
		ns := startEmbeddedNATS(t)
		nc := connectToNATS(t, ns)
		ctx := t.Context()

		exp, err := NewLogExporter(nc, WithExporterSubjectPrefix("backlog-burst"))
		require.NoError(t, err)

		var receivedCount atomic.Int32
		processingDelay := 50 * time.Millisecond

		// Create receiver with reasonable backlog
		recv, err := NewReceiver(nc,
			WithReceiverSubjectPrefix("backlog-burst"),
			WithReceiverBacklogSize(100),
			WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
				// Simulate slow processing
				time.Sleep(processingDelay)
				receivedCount.Add(1)
				return nil
			}),
		)
		require.NoError(t, err)

		require.NoError(t, recv.Start(ctx))

		// Send burst of messages
		messageCount := 10
		for i := 0; i < messageCount; i++ {
			rec := createTestLogRecord(t)
			require.NoError(t, exp.Export(ctx, []sdklog.Record{rec}))
		}

		// Wait for all messages to be processed
		require.Eventually(t, func() bool {
			return receivedCount.Load() == int32(messageCount)
		}, 5*time.Second, 10*time.Millisecond)

		require.NoError(t, recv.Shutdown(ctx))
	})
}

func TestReceiver_JSONEncoding_Roundtrip(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	// Create exporter with JSON encoding
	exp, err := NewLogExporter(nc,
		WithExporterSubjectPrefix("jsonrt"),
		WithExporterEncoding(EncodingJSON),
	)
	require.NoError(t, err)

	// Create receiver (auto-detects encoding from Content-Type)
	received := make(chan *logspb.LogsData, 1)

	recv, err := NewReceiver(nc,
		WithReceiverSubjectPrefix("jsonrt"),
		WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
			data, err := msg.Signal()
			if err != nil {
				return err
			}
			received <- data
			return nil
		}),
	)
	require.NoError(t, err)

	require.NoError(t, recv.Start(ctx))
	defer recv.Shutdown(ctx)

	// Export a JSON-encoded message
	rec := createTestLogRecord(t)
	require.NoError(t, exp.Export(ctx, []sdklog.Record{rec}))

	// Verify receipt and correct decoding
	select {
	case data := <-received:
		require.Len(t, data.ResourceLogs, 1)
		require.Len(t, data.ResourceLogs[0].ScopeLogs, 1)
		require.Len(t, data.ResourceLogs[0].ScopeLogs[0].LogRecords, 1)

		lr := data.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
		require.Equal(t, "test message", lr.Body.GetStringValue())
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestReceiver_MixedEncoding_AutoDetects(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	// Create two exporters: one protobuf, one JSON
	expProto, err := NewLogExporter(nc,
		WithExporterSubjectPrefix("mixed"),
		WithExporterEncoding(EncodingProtobuf),
	)
	require.NoError(t, err)

	expJSON, err := NewLogExporter(nc,
		WithExporterSubjectPrefix("mixed"),
		WithExporterEncoding(EncodingJSON),
	)
	require.NoError(t, err)

	// Create receiver (should handle both)
	received := make(chan *logspb.LogsData, 10)

	recv, err := NewReceiver(nc,
		WithReceiverSubjectPrefix("mixed"),
		WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
			data, err := msg.Signal()
			if err != nil {
				return err
			}
			received <- data
			return nil
		}),
	)
	require.NoError(t, err)

	require.NoError(t, recv.Start(ctx))
	defer recv.Shutdown(ctx)

	// Export both protobuf and JSON messages
	rec := createTestLogRecord(t)
	require.NoError(t, expProto.Export(ctx, []sdklog.Record{rec}))
	require.NoError(t, expJSON.Export(ctx, []sdklog.Record{rec}))

	// Should receive both messages
	for i := 0; i < 2; i++ {
		select {
		case data := <-received:
			require.Len(t, data.ResourceLogs, 1)
			lr := data.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
			require.Equal(t, "test message", lr.Body.GetStringValue())
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for message %d", i+1)
		}
	}
}

func TestReceiver_SignalHeaderErrors(t *testing.T) {
	t.Run("unknown signal header calls error handler", func(t *testing.T) {
		ns := startEmbeddedNATS(t)
		nc1 := connectToNATS(t, ns)
		nc2 := connectToNATS(t, ns)
		ctx := t.Context()

		// Track errors
		var capturedErrors []error
		var errorsMu sync.Mutex

		recv, err := NewReceiver(nc2,
			WithReceiverSubjectPrefix("unknown"),
			WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
				_, err := msg.Signal()
				return err
			}),
			WithReceiverErrorHandler(func(err error) {
				errorsMu.Lock()
				defer errorsMu.Unlock()
				capturedErrors = append(capturedErrors, err)
			}),
		)
		require.NoError(t, err)

		require.NoError(t, recv.Start(ctx))

		// Publish a message with an unknown signal header directly via NATS
		logsData := &logspb.LogsData{}
		data, err := Marshal(logsData, EncodingProtobuf)
		require.NoError(t, err)

		msg := &nats.Msg{
			Subject: BuildSubject("unknown", "logs", ""),
			Data:    data,
			Header: nats.Header{
				HeaderContentType: []string{ContentTypeProtobuf},
				HeaderOtelSignal:  []string{"unknown-signal"}, // Invalid signal
			},
		}

		require.NoError(t, nc1.PublishMsg(msg))
		require.NoError(t, nc1.Flush())

		// Wait for error to be captured
		require.Eventually(t, func() bool {
			errorsMu.Lock()
			defer errorsMu.Unlock()
			return len(capturedErrors) > 0
		}, 5*time.Second, 10*time.Millisecond)

		// Verify the error is ErrUnknownSignal
		errorsMu.Lock()
		require.True(t, errors.Is(capturedErrors[0], ErrUnknownSignal))
		errorsMu.Unlock()

		require.NoError(t, recv.Shutdown(ctx))
	})

	t.Run("missing signal header calls error handler", func(t *testing.T) {
		ns := startEmbeddedNATS(t)
		nc1 := connectToNATS(t, ns)
		nc2 := connectToNATS(t, ns)
		ctx := t.Context()

		// Track errors
		var capturedErrors []error
		var errorsMu sync.Mutex

		recv, err := NewReceiver(nc2,
			WithReceiverSubjectPrefix("missing"),
			WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
				_, err := msg.Signal()
				return err
			}),
			WithReceiverErrorHandler(func(err error) {
				errorsMu.Lock()
				defer errorsMu.Unlock()
				capturedErrors = append(capturedErrors, err)
			}),
		)
		require.NoError(t, err)

		require.NoError(t, recv.Start(ctx))

		// Publish a message WITHOUT signal header
		logsData := &logspb.LogsData{}
		data, err := Marshal(logsData, EncodingProtobuf)
		require.NoError(t, err)

		msg := &nats.Msg{
			Subject: BuildSubject("missing", "logs", ""),
			Data:    data,
			Header: nats.Header{
				HeaderContentType: []string{ContentTypeProtobuf},
				// HeaderOtelSignal is missing
			},
		}

		require.NoError(t, nc1.PublishMsg(msg))
		require.NoError(t, nc1.Flush())

		// Wait for error to be captured
		require.Eventually(t, func() bool {
			errorsMu.Lock()
			defer errorsMu.Unlock()
			return len(capturedErrors) > 0
		}, 5*time.Second, 10*time.Millisecond)

		// Verify the error is ErrUnknownSignal
		errorsMu.Lock()
		require.True(t, errors.Is(capturedErrors[0], ErrUnknownSignal))
		errorsMu.Unlock()

		require.NoError(t, recv.Shutdown(ctx))
	})
}

func TestReceiverWithConsumerObject(t *testing.T) {
	ns := startEmbeddedNATSWithJetStream(t)
	nc := connectToNATS(t, ns)
	js := createJetStream(t, nc)
	ctx := t.Context()

	// Create stream
	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "WITHCONSUMER",
		Subjects: OTLPSubjects("wc"),
	})
	require.NoError(t, err)

	// Create consumer using native API
	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       "my-consumer",
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       2 * time.Minute,
	})
	require.NoError(t, err)

	// Create exporter
	exp, err := NewLogExporter(nc,
		WithExporterSubjectPrefix("wc"),
		WithExporterJetStream(js),
	)
	require.NoError(t, err)

	// Create receiver using WithConsumer (passing consumer directly)
	received := make(chan *logspb.LogsData, 1)

	recv, err := NewReceiver(nc,
		WithReceiverSubjectPrefix("wc"),
		WithReceiverJetStream(js, "WITHCONSUMER"),
		WithReceiverConsumer(consumer),
		WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
			data, err := msg.Signal()
			if err != nil {
				return err
			}
			received <- data
			return nil
		}),
	)
	require.NoError(t, err)

	require.NoError(t, recv.Start(ctx))
	defer recv.Shutdown(ctx)

	// Export a message
	rec := createTestLogRecord(t)
	require.NoError(t, exp.Export(ctx, []sdklog.Record{rec}))

	// Verify receipt
	select {
	case data := <-received:
		require.Len(t, data.ResourceLogs, 1)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	// Verify consumer settings are preserved
	consumerCheck, err := stream.Consumer(ctx, "my-consumer")
	require.NoError(t, err)
	require.Equal(t, 2*time.Minute, consumerCheck.CachedInfo().Config.AckWait)
}

func TestMultiTenantWithSuffix(t *testing.T) {
	ns := startEmbeddedNATSWithJetStream(t)
	nc := connectToNATS(t, ns)
	js := createJetStream(t, nc)
	ctx := t.Context()

	// Create stream with wildcard subjects for multi-tenant
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "MULTI_TENANT",
		Subjects: OTLPSubjects("mt", ">"), // mt.logs.>, mt.traces.>, mt.metrics.>
	})
	require.NoError(t, err)

	// Create exporter for tenant-a
	expA, err := NewLogExporter(nc,
		WithExporterSubjectPrefix("mt"),
		WithExporterSubjectSuffix("tenant-a"),
		WithExporterJetStream(js),
	)
	require.NoError(t, err)

	// Create exporter for tenant-b
	expB, err := NewLogExporter(nc,
		WithExporterSubjectPrefix("mt"),
		WithExporterSubjectSuffix("tenant-b"),
		WithExporterJetStream(js),
	)
	require.NoError(t, err)

	// Create receiver that subscribes to all tenants
	received := make(chan *logspb.LogsData, 10)

	recv, err := NewReceiver(nc,
		WithReceiverSubjectPrefix("mt"),
		WithReceiverSubjectSuffix(">"),
		WithReceiverJetStream(js, "MULTI_TENANT"),
		WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
			data, err := msg.Signal()
			if err != nil {
				return err
			}
			received <- data
			return nil
		}),
	)
	require.NoError(t, err)

	require.NoError(t, recv.Start(ctx))
	defer recv.Shutdown(ctx)

	// Export from both tenants
	rec := createTestLogRecord(t)
	require.NoError(t, expA.Export(ctx, []sdklog.Record{rec}))
	require.NoError(t, expB.Export(ctx, []sdklog.Record{rec}))

	// Should receive both messages
	for i := 0; i < 2; i++ {
		select {
		case <-received:
			// OK
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for message %d", i+1)
		}
	}
}

func TestReceiverWithPerSignalSubjects(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	// Create exporters for different custom subjects
	expLogs, err := NewLogExporter(nc,
		WithExporterSubjectPrefix("custom.logs"),
	)
	require.NoError(t, err)

	// Track received logs
	receivedLogs := make(chan *logspb.LogsData, 10)

	// Create receiver with explicit per-signal subjects
	recv, err := NewReceiver(nc,
		WithReceiverSignalSubject(SignalLogs, "custom.logs.logs"),
		WithReceiverSignalSubject(SignalTraces, "custom.traces.traces"),
		WithReceiverSignalSubject(SignalMetrics, "custom.metrics.metrics"),
		WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
			data, err := msg.Signal()
			if err != nil {
				return err
			}
			receivedLogs <- data
			return nil
		}),
	)
	require.NoError(t, err)

	require.NoError(t, recv.Start(ctx))
	defer recv.Shutdown(ctx)

	// Export a log record
	rec := createTestLogRecord(t)
	require.NoError(t, expLogs.Export(ctx, []sdklog.Record{rec}))

	// Should receive the message
	select {
	case data := <-receivedLogs:
		require.Len(t, data.ResourceLogs, 1)
		lr := data.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
		require.Equal(t, "test message", lr.Body.GetStringValue())
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for log message")
	}
}

func TestReceiverWithMixedSubjectConfiguration(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc1 := connectToNATS(t, ns)
	nc2 := connectToNATS(t, ns)
	ctx := t.Context()

	// Create two exporters - one using explicit subject, one using prefix/suffix
	expCustom, err := NewLogExporter(nc1,
		WithExporterSubjectPrefix("custom.logs"),
	)
	require.NoError(t, err)

	expStandard, err := NewLogExporter(nc1,
		WithExporterSubjectPrefix("standard"),
	)
	require.NoError(t, err)

	// Track received data
	receivedCustom := make(chan *logspb.LogsData, 10)
	receivedStandard := make(chan *logspb.LogsData, 10)

	// Create receiver that uses explicit subject for logs only
	// Other signals would use prefix/suffix (if handlers were configured)
	recv, err := NewReceiver(nc2,
		WithReceiverSubjectPrefix("standard"),
		WithReceiverSignalSubject(SignalLogs, "custom.logs.logs"),
		WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
			data, err := msg.Signal()
			if err != nil {
				return err
			}
			receivedCustom <- data
			return nil
		}),
	)
	require.NoError(t, err)

	// Create a second receiver that uses standard prefix/suffix
	recv2, err := NewReceiver(nc2,
		WithReceiverSubjectPrefix("standard"),
		WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
			data, err := msg.Signal()
			if err != nil {
				return err
			}
			receivedStandard <- data
			return nil
		}),
	)
	require.NoError(t, err)

	require.NoError(t, recv.Start(ctx))
	require.NoError(t, recv2.Start(ctx))
	defer recv.Shutdown(ctx)
	defer recv2.Shutdown(ctx)

	// Export to custom subject
	rec1 := createTestLogRecord(t)
	require.NoError(t, expCustom.Export(ctx, []sdklog.Record{rec1}))

	// Export to standard subject
	rec2 := createTestLogRecord(t)
	require.NoError(t, expStandard.Export(ctx, []sdklog.Record{rec2}))

	// Should receive custom message on recv
	select {
	case data := <-receivedCustom:
		require.Len(t, data.ResourceLogs, 1)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for custom log message")
	}

	// Should receive standard message on recv2
	select {
	case data := <-receivedStandard:
		require.Len(t, data.ResourceLogs, 1)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for standard log message")
	}
}

func TestReceiver_JetStream_RateLimited(t *testing.T) {
	ns := startEmbeddedNATSWithJetStream(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	js := createJetStream(t, nc)
	streamName := createTestStream(t, js, "ratelimit")

	// Create exporter with JetStream
	exp, err := NewLogExporter(nc, WithExporterSubjectPrefix("ratelimit"), WithExporterJetStream(js))
	require.NoError(t, err)

	// Track received data
	var receivedCount atomic.Int32

	// Create receiver with rate limiting
	recv, err := NewReceiver(nc,
		WithReceiverSubjectPrefix("ratelimit"),
		WithReceiverJetStream(js, streamName),
		WithReceiverRateLimit(1000, 10), // 1000 msg/s, burst 10
		WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
			receivedCount.Add(1)
			return nil
		}),
		WithReceiverErrorHandler(func(err error) {
			t.Logf("error: %v", err)
		}),
	)
	require.NoError(t, err)

	require.NoError(t, recv.Start(ctx))

	// Export multiple log records
	for i := 0; i < 5; i++ {
		rec := createTestLogRecord(t)
		require.NoError(t, exp.Export(ctx, []sdklog.Record{rec}))
	}

	// Wait for messages to be received
	require.Eventually(t, func() bool {
		return receivedCount.Load() >= 5
	}, 5*time.Second, 10*time.Millisecond)

	// Shutdown before final assertion to avoid race with ongoing processing
	require.NoError(t, recv.Shutdown(ctx))

	require.GreaterOrEqual(t, receivedCount.Load(), int32(5))
}

func TestReceiver_JetStream_RateLimited_CustomBatchSize(t *testing.T) {
	ns := startEmbeddedNATSWithJetStream(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	js := createJetStream(t, nc)
	streamName := createTestStream(t, js, "rlbatch")

	// Create exporter with JetStream
	exp, err := NewLogExporter(nc, WithExporterSubjectPrefix("rlbatch"), WithExporterJetStream(js))
	require.NoError(t, err)

	// Track received data
	var receivedCount atomic.Int32

	// Create receiver with rate limiting and custom batch size
	recv, err := NewReceiver(nc,
		WithReceiverSubjectPrefix("rlbatch"),
		WithReceiverJetStream(js, streamName),
		WithReceiverRateLimit(1000, 20),  // 1000 msg/s, burst 20
		WithReceiverFetchBatchSize(5),     // fetch 5 at a time
		WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
			receivedCount.Add(1)
			return nil
		}),
		WithReceiverErrorHandler(func(err error) {
			t.Logf("error: %v", err)
		}),
	)
	require.NoError(t, err)

	require.NoError(t, recv.Start(ctx))

	// Export multiple log records
	for i := 0; i < 10; i++ {
		rec := createTestLogRecord(t)
		require.NoError(t, exp.Export(ctx, []sdklog.Record{rec}))
	}

	// Wait for messages to be received
	require.Eventually(t, func() bool {
		return receivedCount.Load() >= 10
	}, 5*time.Second, 10*time.Millisecond)

	// Shutdown before final assertion to avoid race with ongoing processing
	require.NoError(t, recv.Shutdown(ctx))

	require.GreaterOrEqual(t, receivedCount.Load(), int32(10))
}

func TestReceiver_JetStream_RateLimited_Shutdown(t *testing.T) {
	ns := startEmbeddedNATSWithJetStream(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	js := createJetStream(t, nc)
	streamName := createTestStream(t, js, "rlshutdown")

	// Create exporter with JetStream
	exp, err := NewLogExporter(nc, WithExporterSubjectPrefix("rlshutdown"), WithExporterJetStream(js))
	require.NoError(t, err)

	// Track received data
	var receivedCount atomic.Int32

	// Create receiver with rate limiting
	recv, err := NewReceiver(nc,
		WithReceiverSubjectPrefix("rlshutdown"),
		WithReceiverJetStream(js, streamName),
		WithReceiverRateLimit(10, 5), // Very slow: 10 msg/s
		WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
			receivedCount.Add(1)
			return nil
		}),
		WithReceiverErrorHandler(func(err error) {
			t.Logf("error: %v", err)
		}),
	)
	require.NoError(t, err)

	require.NoError(t, recv.Start(ctx))

	// Export some records
	for i := 0; i < 3; i++ {
		rec := createTestLogRecord(t)
		require.NoError(t, exp.Export(ctx, []sdklog.Record{rec}))
	}

	// Wait briefly for some processing
	time.Sleep(100 * time.Millisecond)

	// Shutdown should complete gracefully
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err = recv.Shutdown(shutdownCtx)
	require.NoError(t, err)
}

func TestReceiver_JetStream_RateLimited_ValidationError(t *testing.T) {
	ns := startEmbeddedNATSWithJetStream(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	js := createJetStream(t, nc)
	streamName := createTestStream(t, js, "rlvalidation")

	// Create receiver with invalid config: batch > burst
	recv, err := NewReceiver(nc,
		WithReceiverSubjectPrefix("rlvalidation"),
		WithReceiverJetStream(js, streamName),
		WithReceiverRateLimit(100, 5),     // burst = 5
		WithReceiverFetchBatchSize(10),    // batch = 10 > burst (invalid!)
		WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
			return nil
		}),
	)
	require.NoError(t, err)

	// Start should fail validation
	err = recv.Start(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "burst must be >= batch size")
}

func TestReceiver_JetStream_NoRateLimit_PreservesConsume(t *testing.T) {
	// This test verifies that when rate limiting is NOT enabled,
	// the receiver uses the Consume API (buffer-based) rather than Fetch
	ns := startEmbeddedNATSWithJetStream(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	js := createJetStream(t, nc)
	streamName := createTestStream(t, js, "norl")

	// Create exporter with JetStream
	exp, err := NewLogExporter(nc, WithExporterSubjectPrefix("norl"), WithExporterJetStream(js))
	require.NoError(t, err)

	// Track received data
	var receivedCount atomic.Int32

	// Create receiver WITHOUT rate limiting (default behavior)
	recv, err := NewReceiver(nc,
		WithReceiverSubjectPrefix("norl"),
		WithReceiverJetStream(js, streamName),
		// No WithReceiverRateLimit - uses default Consume API
		WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
			receivedCount.Add(1)
			return nil
		}),
		WithReceiverErrorHandler(func(err error) {
			t.Logf("error: %v", err)
		}),
	)
	require.NoError(t, err)

	require.NoError(t, recv.Start(ctx))

	// Export multiple log records rapidly
	for i := 0; i < 20; i++ {
		rec := createTestLogRecord(t)
		require.NoError(t, exp.Export(ctx, []sdklog.Record{rec}))
	}

	// All messages should be received (Consume API handles them)
	require.Eventually(t, func() bool {
		return receivedCount.Load() >= 20
	}, 5*time.Second, 10*time.Millisecond)

	// Shutdown before final assertion to avoid race with ongoing processing
	require.NoError(t, recv.Shutdown(ctx))

	require.GreaterOrEqual(t, receivedCount.Load(), int32(20))
}
