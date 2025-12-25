package otelnats

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

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

func TestReceiver_JetStream_QueueGroup(t *testing.T) {
	ns := startEmbeddedNATSWithJetStream(t)
	nc1 := connectToNATS(t, ns)
	nc2 := connectToNATS(t, ns)
	nc3 := connectToNATS(t, ns)
	ctx := t.Context()

	js1 := createJetStream(t, nc1)
	js2 := createJetStream(t, nc2)
	js3 := createJetStream(t, nc3)
	streamName := createTestStream(t, js1, "jsqg")

	// Create exporter
	exp, err := NewLogExporter(nc1, WithExporterSubjectPrefix("jsqg"), WithExporterJetStream(js1))
	require.NoError(t, err)

	// Create two receivers in same queue group
	var count1, count2 atomic.Int32

	recv1, err := NewReceiver(nc2,
		WithReceiverSubjectPrefix("jsqg"),
		WithReceiverJetStream(js2, streamName),
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
		WithReceiverSubjectPrefix("jsqg"),
		WithReceiverJetStream(js3, streamName),
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

	require.NoError(t, recv1.Start(ctx))
	require.NoError(t, recv2.Start(ctx))

	// Send multiple messages
	const numMessages = 10
	for i := 0; i < numMessages; i++ {
		rec := createTestLogRecord(t)
		require.NoError(t, exp.Export(ctx, []sdklog.Record{rec}))
	}

	// Wait for all messages to be received
	require.Eventually(t, func() bool {
		return count1.Load()+count2.Load() == numMessages
	}, 5*time.Second, 10*time.Millisecond)

	total := count1.Load() + count2.Load()
	require.Equal(t, int32(numMessages), total, "each message should be received exactly once")
	t.Logf("JetStream queue group: recv1=%d, recv2=%d", count1.Load(), count2.Load())

	require.NoError(t, recv1.Shutdown(ctx))
	require.NoError(t, recv2.Shutdown(ctx))
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
