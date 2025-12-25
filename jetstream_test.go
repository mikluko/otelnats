package otelnats

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
)

func TestOTLPSubjects(t *testing.T) {
	t.Run("default prefix", func(t *testing.T) {
		subjects := OTLPSubjects("otel")
		require.ElementsMatch(t, []string{"otel.logs", "otel.traces", "otel.metrics"}, subjects)
	})

	t.Run("custom prefix", func(t *testing.T) {
		subjects := OTLPSubjects("myapp")
		require.ElementsMatch(t, []string{"myapp.logs", "myapp.traces", "myapp.metrics"}, subjects)
	})

	t.Run("with suffix", func(t *testing.T) {
		subjects := OTLPSubjects("otel", ">")
		require.ElementsMatch(t, []string{"otel.logs.>", "otel.traces.>", "otel.metrics.>"}, subjects)
	})

	t.Run("with tenant suffix", func(t *testing.T) {
		subjects := OTLPSubjects("otel", "tenant-a")
		require.ElementsMatch(t, []string{"otel.logs.tenant-a", "otel.traces.tenant-a", "otel.metrics.tenant-a"}, subjects)
	})
}

func TestJetStreamIntegration(t *testing.T) {
	ns := startEmbeddedNATSWithJetStream(t)
	nc := connectToNATS(t, ns)
	js := createJetStream(t, nc)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second*2)
	defer cancel()

	// Create stream using native jetstream API with OTLPSubjects helper
	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "INTEGRATION_TEST",
		Subjects: OTLPSubjects("integ"),
	})
	require.NoError(t, err)
	require.Equal(t, "INTEGRATION_TEST", stream.CachedInfo().Config.Name)

	// Create exporter and receiver
	exp, err := NewLogExporter(nc,
		WithExporterSubjectPrefix("integ"),
		WithExporterJetStream(js),
	)
	require.NoError(t, err)

	received := make(chan struct{}, 1)

	recv, err := NewReceiver(nc,
		WithReceiverSubjectPrefix("integ"),
		WithReceiverJetStream(js, "INTEGRATION_TEST"),
		WithReceiverConsumerName("test-processor"),
		WithReceiverLogsHandler(func(ctx context.Context, msg MessageSignal[logspb.LogsData]) error {
			_, err := msg.Signal()
			if err != nil {
				return err
			}
			received <- struct{}{}
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
	case <-received:
		// OK
	case <-ctx.Done():
		t.Fatal("timeout waiting for message")
	}

	// Verify the consumer was created by the receiver
	consumer, err := stream.Consumer(ctx, "test-processor")
	require.NoError(t, err)
	require.Equal(t, "test-processor", consumer.CachedInfo().Config.Durable)
}

func TestJetStreamWithPreCreatedConsumer(t *testing.T) {
	ns := startEmbeddedNATSWithJetStream(t)
	nc := connectToNATS(t, ns)
	js := createJetStream(t, nc)
	ctx := t.Context()

	// Create stream
	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "PRECREATED",
		Subjects: OTLPSubjects("pre"),
	})
	require.NoError(t, err)

	// Pre-create consumer with specific settings using native API
	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       "pre-created-consumer",
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       1 * time.Minute,
		MaxDeliver:    3,
	})
	require.NoError(t, err)
	require.Equal(t, "pre-created-consumer", consumer.CachedInfo().Config.Durable)
	require.Equal(t, 1*time.Minute, consumer.CachedInfo().Config.AckWait)

	// Create exporter
	exp, err := NewLogExporter(nc,
		WithExporterSubjectPrefix("pre"),
		WithExporterJetStream(js),
	)
	require.NoError(t, err)

	// Create receiver that binds to pre-created consumer
	received := make(chan *logspb.LogsData, 1)

	recv, err := NewReceiver(nc,
		WithReceiverSubjectPrefix("pre"),
		WithReceiverJetStream(js, "PRECREATED"),
		WithReceiverConsumerName("pre-created-consumer"),
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
	consumerCheck, err := stream.Consumer(ctx, "pre-created-consumer")
	require.NoError(t, err)
	info := consumerCheck.CachedInfo()
	require.Equal(t, 1*time.Minute, info.Config.AckWait)
	require.Equal(t, 3, info.Config.MaxDeliver)
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
