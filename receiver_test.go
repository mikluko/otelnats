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
		require.Equal(t, errNilConnection, err)
	})

	t.Run("valid connection succeeds", func(t *testing.T) {
		ns := startEmbeddedNATS(t)
		nc := connectToNATS(t, ns)

		recv, err := NewReceiver(nc)
		require.NoError(t, err)
		require.NotNil(t, recv)
	})
}

func TestReceiver_CallbackAPI(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	// Create exporter and receiver
	exp, err := NewLogExporter(nc, WithSubjectPrefix("cb"))
	require.NoError(t, err)

	recv, err := NewReceiver(nc, WithReceiverSubjectPrefix("cb"))
	require.NoError(t, err)

	// Track received data
	var received atomic.Pointer[logspb.LogsData]
	recv.OnLogs(func(ctx context.Context, data *logspb.LogsData) error {
		received.Store(data)
		return nil
	})

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

func TestReceiver_ChannelAPI(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	// Create exporter and receiver
	exp, err := NewLogExporter(nc, WithSubjectPrefix("ch"))
	require.NoError(t, err)

	recv, err := NewReceiver(nc, WithReceiverSubjectPrefix("ch"))
	require.NoError(t, err)

	// Get channel before start
	logsCh := recv.Logs()
	require.NotNil(t, logsCh)

	// Start receiver
	err = recv.Start(ctx)
	require.NoError(t, err)

	// Export a log record
	rec := createTestLogRecord(t)
	err = exp.Export(ctx, []sdklog.Record{rec})
	require.NoError(t, err)

	// Receive from channel
	select {
	case data := <-logsCh:
		require.Len(t, data.ResourceLogs, 1)
		lr := data.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
		require.Equal(t, "test message", lr.Body.GetStringValue())
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	// Shutdown
	err = recv.Shutdown(ctx)
	require.NoError(t, err)

	// Channel should be closed
	_, ok := <-logsCh
	require.False(t, ok, "channel should be closed after shutdown")
}

func TestReceiver_QueueGroup(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc1 := connectToNATS(t, ns)
	nc2 := connectToNATS(t, ns)
	nc3 := connectToNATS(t, ns)
	ctx := t.Context()

	// Create exporter
	exp, err := NewLogExporter(nc1, WithSubjectPrefix("qg"))
	require.NoError(t, err)

	// Create two receivers in same queue group
	var count1, count2 atomic.Int32

	recv1, err := NewReceiver(nc2,
		WithReceiverSubjectPrefix("qg"),
		WithQueueGroup("test-group"),
	)
	require.NoError(t, err)
	recv1.OnLogs(func(ctx context.Context, data *logspb.LogsData) error {
		count1.Add(1)
		return nil
	})

	recv2, err := NewReceiver(nc3,
		WithReceiverSubjectPrefix("qg"),
		WithQueueGroup("test-group"),
	)
	require.NoError(t, err)
	recv2.OnLogs(func(ctx context.Context, data *logspb.LogsData) error {
		count2.Add(1)
		return nil
	})

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

	recv, err := NewReceiver(nc)
	require.NoError(t, err)

	recv.OnLogs(func(ctx context.Context, data *logspb.LogsData) error {
		return nil
	})

	// Start and shutdown
	require.NoError(t, recv.Start(ctx))
	require.NoError(t, recv.Shutdown(ctx))

	// Shutdown is idempotent
	require.NoError(t, recv.Shutdown(ctx))

	// Start after shutdown returns error
	err = recv.Start(ctx)
	require.Equal(t, errReceiverShutdown, err)
}

func TestReceiver_BothCallbackAndChannel(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	exp, err := NewLogExporter(nc, WithSubjectPrefix("both"))
	require.NoError(t, err)

	recv, err := NewReceiver(nc, WithReceiverSubjectPrefix("both"))
	require.NoError(t, err)

	// Set both callback and channel
	var callbackReceived atomic.Bool
	recv.OnLogs(func(ctx context.Context, data *logspb.LogsData) error {
		callbackReceived.Store(true)
		return nil
	})
	logsCh := recv.Logs()

	require.NoError(t, recv.Start(ctx))

	// Export
	rec := createTestLogRecord(t)
	require.NoError(t, exp.Export(ctx, []sdklog.Record{rec}))

	// Both should receive the message
	require.Eventually(t, func() bool {
		return callbackReceived.Load()
	}, 5*time.Second, 10*time.Millisecond)

	select {
	case <-logsCh:
		// OK
	case <-time.After(5 * time.Second):
		t.Fatal("channel did not receive message")
	}

	require.NoError(t, recv.Shutdown(ctx))
}

func TestReceiver_NoSubscriptionsWithoutHandlers(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	recv, err := NewReceiver(nc)
	require.NoError(t, err)

	// Start without any handlers - should succeed but not subscribe to anything
	require.NoError(t, recv.Start(ctx))

	// Verify no subscriptions were created
	require.Empty(t, recv.subs)

	require.NoError(t, recv.Shutdown(ctx))
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
	exp, err := NewLogExporter(nc, WithSubjectPrefix("jsrecv"), WithJetStream(js))
	require.NoError(t, err)

	// Create receiver with JetStream
	recv, err := NewReceiver(nc,
		WithReceiverSubjectPrefix("jsrecv"),
		WithReceiverJetStream(js, streamName),
	)
	require.NoError(t, err)

	// Track received data
	received := make(chan *logspb.LogsData, 1)
	recv.OnLogs(func(ctx context.Context, data *logspb.LogsData) error {
		received <- data
		return nil
	})

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
	exp, err := NewLogExporter(nc, WithSubjectPrefix("jsdurable"), WithJetStream(js))
	require.NoError(t, err)

	// Create receiver with durable consumer
	received := make(chan *logspb.LogsData, 10)
	recv, err := NewReceiver(nc,
		WithReceiverSubjectPrefix("jsdurable"),
		WithReceiverJetStream(js, streamName),
		WithConsumerName("test-consumer"),
	)
	require.NoError(t, err)
	recv.OnLogs(func(ctx context.Context, data *logspb.LogsData) error {
		received <- data
		return nil
	})

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
	exp, err := NewLogExporter(nc, WithSubjectPrefix("jsnak"), WithJetStream(js))
	require.NoError(t, err)

	// Track delivery attempts
	var attempts atomic.Int32
	errSimulated := errors.New("simulated error")

	recv, err := NewReceiver(nc,
		WithReceiverSubjectPrefix("jsnak"),
		WithReceiverJetStream(js, streamName),
		WithAckWait(500*time.Millisecond), // Short ack wait for faster test
	)
	require.NoError(t, err)

	recv.OnLogs(func(ctx context.Context, data *logspb.LogsData) error {
		count := attempts.Add(1)
		// Fail first 2 attempts, succeed on 3rd
		if count < 3 {
			return errSimulated
		}
		return nil
	})

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
	exp, err := NewLogExporter(nc1, WithSubjectPrefix("jsqg"), WithJetStream(js1))
	require.NoError(t, err)

	// Create two receivers in same queue group
	var count1, count2 atomic.Int32

	recv1, err := NewReceiver(nc2,
		WithReceiverSubjectPrefix("jsqg"),
		WithReceiverJetStream(js2, streamName),
		WithQueueGroup("test-group"),
	)
	require.NoError(t, err)
	recv1.OnLogs(func(ctx context.Context, data *logspb.LogsData) error {
		count1.Add(1)
		return nil
	})

	recv2, err := NewReceiver(nc3,
		WithReceiverSubjectPrefix("jsqg"),
		WithReceiverJetStream(js3, streamName),
		WithQueueGroup("test-group"),
	)
	require.NoError(t, err)
	recv2.OnLogs(func(ctx context.Context, data *logspb.LogsData) error {
		count2.Add(1)
		return nil
	})

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
