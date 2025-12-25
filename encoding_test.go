package otelnats

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/protobuf/encoding/protojson"

	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
)

func TestJSONEncoding_LogExporter(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	t.Run("exports with JSON encoding and correct content-type", func(t *testing.T) {
		exp, err := NewLogExporter(nc,
			WithExporterSubjectPrefix("json"),
			WithExporterEncoding(EncodingJSON),
		)
		require.NoError(t, err)

		sub, err := nc.SubscribeSync("json.logs")
		require.NoError(t, err)
		defer sub.Unsubscribe()

		rec := createTestLogRecord(t)
		err = exp.Export(ctx, []sdklog.Record{rec})
		require.NoError(t, err)

		msg := requireMessage(t, sub, 5*time.Second)

		// Check Content-Type header is JSON
		require.Equal(t, ContentTypeJSON, msg.Header.Get(HeaderContentType))
		require.Equal(t, SignalLogs, msg.Header.Get(HeaderOtelSignal))

		// Verify payload is valid JSON (not protobuf)
		var logsData logspb.LogsData
		err = protojson.Unmarshal(msg.Data, &logsData)
		require.NoError(t, err)

		require.Len(t, logsData.ResourceLogs, 1)
		require.Len(t, logsData.ResourceLogs[0].ScopeLogs, 1)
		require.Len(t, logsData.ResourceLogs[0].ScopeLogs[0].LogRecords, 1)

		lr := logsData.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
		require.Equal(t, "test message", lr.Body.GetStringValue())
	})
}

func TestJSONEncoding_Roundtrip(t *testing.T) {
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

func TestJSONEncoding_SpanExporter(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	exp, err := NewSpanExporter(nc,
		WithExporterSubjectPrefix("jsontrace"),
		WithExporterEncoding(EncodingJSON),
	)
	require.NoError(t, err)

	sub, err := nc.SubscribeSync("jsontrace.traces")
	require.NoError(t, err)
	defer sub.Unsubscribe()

	span := createTestSpan(t)
	err = exp.ExportSpans(ctx, []sdktrace.ReadOnlySpan{span})
	require.NoError(t, err)

	msg := requireMessage(t, sub, 5*time.Second)

	// Check Content-Type header is JSON
	require.Equal(t, ContentTypeJSON, msg.Header.Get(HeaderContentType))
	require.Equal(t, SignalTraces, msg.Header.Get(HeaderOtelSignal))
}

func TestJSONEncoding_MetricExporter(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	exp, err := NewMetricExporter(nc,
		WithExporterSubjectPrefix("jsonmetric"),
		WithExporterEncoding(EncodingJSON),
	)
	require.NoError(t, err)

	sub, err := nc.SubscribeSync("jsonmetric.metrics")
	require.NoError(t, err)
	defer sub.Unsubscribe()

	rm := createTestResourceMetrics(t)
	err = exp.Export(ctx, rm)
	require.NoError(t, err)

	msg := requireMessage(t, sub, 5*time.Second)

	// Check Content-Type header is JSON
	require.Equal(t, ContentTypeJSON, msg.Header.Get(HeaderContentType))
	require.Equal(t, SignalMetrics, msg.Header.Get(HeaderOtelSignal))
}

func TestMixedEncoding_ReceiverAutoDetects(t *testing.T) {
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
