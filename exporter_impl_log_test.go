package otelnats

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
)

func TestNewLogExporter(t *testing.T) {
	t.Run("nil connection returns error", func(t *testing.T) {
		exp, err := NewLogExporter(nil)
		require.Error(t, err)
		require.Nil(t, exp)
		require.Equal(t, ErrNilConnection, err)
	})

	t.Run("valid connection succeeds", func(t *testing.T) {
		ns := startEmbeddedNATS(t)
		nc := connectToNATS(t, ns)

		exp, err := NewLogExporter(nc)
		require.NoError(t, err)
		require.NotNil(t, exp)
	})
}

func TestLogExporter_Export(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	t.Run("empty records does nothing", func(t *testing.T) {
		exp, err := NewLogExporter(nc)
		require.NoError(t, err)

		err = exp.Export(ctx, nil)
		require.NoError(t, err)

		err = exp.Export(ctx, []sdklog.Record{})
		require.NoError(t, err)
	})

	t.Run("exports records with correct subject and headers", func(t *testing.T) {
		exp, err := NewLogExporter(nc, WithExporterSubjectPrefix("test"))
		require.NoError(t, err)

		// Subscribe to receive the message
		sub, err := nc.SubscribeSync("test.logs")
		require.NoError(t, err)
		defer sub.Unsubscribe()

		// Create a test record
		rec := createTestLogRecord(t)

		// Export
		err = exp.Export(ctx, []sdklog.Record{rec})
		require.NoError(t, err)

		// Verify message received
		msg := requireMessage(t, sub, 5*time.Second)

		// Check headers
		require.Equal(t, ContentTypeProtobuf, msg.Header.Get(HeaderContentType))
		require.Equal(t, SignalLogs, msg.Header.Get(HeaderOtelSignal))

		// Verify payload is valid protobuf
		var logsData logspb.LogsData
		err = proto.Unmarshal(msg.Data, &logsData)
		require.NoError(t, err)

		// Verify structure
		require.Len(t, logsData.ResourceLogs, 1)
		require.Len(t, logsData.ResourceLogs[0].ScopeLogs, 1)
		require.Len(t, logsData.ResourceLogs[0].ScopeLogs[0].LogRecords, 1)

		// Verify log record content
		lr := logsData.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
		require.Equal(t, "test message", lr.Body.GetStringValue())
		require.Equal(t, logspb.SeverityNumber_SEVERITY_NUMBER_INFO, lr.SeverityNumber)
	})

	t.Run("custom headers are included", func(t *testing.T) {
		exp, err := NewLogExporter(nc,
			WithExporterSubjectPrefix("headers"),
			WithExporterHeaders(func(ctx context.Context) nats.Header {
				return nats.Header{"X-Custom": []string{"value"}}
			}),
		)
		require.NoError(t, err)

		sub, err := nc.SubscribeSync("headers.logs")
		require.NoError(t, err)
		defer sub.Unsubscribe()

		rec := createTestLogRecord(t)
		err = exp.Export(ctx, []sdklog.Record{rec})
		require.NoError(t, err)

		msg := requireMessage(t, sub, 5*time.Second)

		// Built-in headers should be present
		require.Equal(t, ContentTypeProtobuf, msg.Header.Get(HeaderContentType))
		require.Equal(t, SignalLogs, msg.Header.Get(HeaderOtelSignal))

		// Custom header should be present
		require.Equal(t, "value", msg.Header.Get("X-Custom"))
	})
}

func TestLogExporter_Shutdown(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	exp, err := NewLogExporter(nc)
	require.NoError(t, err)

	// Shutdown should succeed
	err = exp.Shutdown(ctx)
	require.NoError(t, err)

	// Export after shutdown should return nil (not error)
	rec := createTestLogRecord(t)
	err = exp.Export(ctx, []sdklog.Record{rec})
	require.NoError(t, err)
}

func TestLogExporter_ForceFlush(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	exp, err := NewLogExporter(nc)
	require.NoError(t, err)

	// ForceFlush should succeed
	err = exp.ForceFlush(ctx)
	require.NoError(t, err)

	// ForceFlush after shutdown should succeed
	err = exp.Shutdown(ctx)
	require.NoError(t, err)

	err = exp.ForceFlush(ctx)
	require.NoError(t, err)
}

func TestLogExporter_RecordGrouping(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	exp, err := NewLogExporter(nc, WithExporterSubjectPrefix("group"))
	require.NoError(t, err)

	sub, err := nc.SubscribeSync("group.logs")
	require.NoError(t, err)
	defer sub.Unsubscribe()

	// Create multiple records - they should be grouped by resource/scope
	rec1 := createTestLogRecord(t)
	rec2 := createTestLogRecord(t)

	err = exp.Export(ctx, []sdklog.Record{rec1, rec2})
	require.NoError(t, err)

	msg := requireMessage(t, sub, 5*time.Second)

	var logsData logspb.LogsData
	err = proto.Unmarshal(msg.Data, &logsData)
	require.NoError(t, err)

	// Should have one ResourceLogs with one ScopeLogs containing both records
	require.Len(t, logsData.ResourceLogs, 1)
	require.Len(t, logsData.ResourceLogs[0].ScopeLogs, 1)
	require.Len(t, logsData.ResourceLogs[0].ScopeLogs[0].LogRecords, 2)
}

// createTestLogRecord creates a log record for testing.
func createTestLogRecord(t *testing.T) sdklog.Record {
	t.Helper()

	var rec sdklog.Record
	rec.SetTimestamp(time.Now())
	rec.SetObservedTimestamp(time.Now())
	rec.SetSeverity(log.SeverityInfo)
	rec.SetSeverityText("INFO")
	rec.SetBody(log.StringValue("test message"))
	rec.SetAttributes(
		log.String("key1", "value1"),
		log.Int("key2", 42),
	)

	// Set trace context
	traceID, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	spanID, _ := trace.SpanIDFromHex("0102030405060708")
	rec.SetTraceID(traceID)
	rec.SetSpanID(spanID)

	return rec
}

// Compile-time check that logExporterImpl implements sdklog.Exporter
var _ sdklog.Exporter = (*logExporterImpl)(nil)
