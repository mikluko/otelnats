package otelnats

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
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

// TestLogExporter_EventName pins that a record's event name reaches the wire.
// The event name is what distinguishes a structured event from an ordinary log
// line, so a consumer routing on it sees every record as unnamed without this.
func TestLogExporter_EventName(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	exp, err := NewLogExporter(nc, WithExporterSubjectPrefix("evname"))
	require.NoError(t, err)

	sub, err := nc.SubscribeSync("evname.logs")
	require.NoError(t, err)
	defer sub.Unsubscribe()

	named := createTestLogRecord(t)
	named.SetEventName("acme.thing.happened")
	unnamed := createTestLogRecord(t)

	err = exp.Export(ctx, []sdklog.Record{named, unnamed})
	require.NoError(t, err)

	msg := requireMessage(t, sub, 5*time.Second)

	var logsData logspb.LogsData
	err = proto.Unmarshal(msg.Data, &logsData)
	require.NoError(t, err)

	records := logsData.ResourceLogs[0].ScopeLogs[0].LogRecords
	require.Len(t, records, 2)
	require.Equal(t, "acme.thing.happened", records[0].EventName)
	require.Empty(t, records[1].EventName)
}

func TestAttributeValueToProto(t *testing.T) {
	str := func(s string) *commonpb.AnyValue {
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: s}}
	}
	i64 := func(n int64) *commonpb.AnyValue {
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: n}}
	}

	tests := []struct {
		name string
		in   attribute.Value
		want *commonpb.AnyValue
	}{
		{
			name: "empty",
			in:   attribute.Value{},
			want: &commonpb.AnyValue{},
		},
		{
			name: "bytes",
			in:   attribute.ByteSliceValue([]byte{0x01, 0x02}),
			want: &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: []byte{0x01, 0x02}}},
		},
		{
			name: "heterogeneous slice",
			in:   attribute.SliceValue(attribute.StringValue("a"), attribute.Int64Value(1)),
			want: &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{
				Values: []*commonpb.AnyValue{str("a"), i64(1)},
			}}},
		},
		{
			name: "nested map",
			in: attribute.MapValue(
				attribute.String("k", "v"),
				attribute.KeyValue{Key: "inner", Value: attribute.MapValue(attribute.Int64("n", 2))},
			),
			want: &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{
				Values: []*commonpb.KeyValue{
					{Key: "inner", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{
						Values: []*commonpb.KeyValue{{Key: "n", Value: i64(2)}},
					}}}},
					{Key: "k", Value: str("v")},
				},
			}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := attributeValueToProto(tt.in)
			require.True(t, proto.Equal(tt.want, got), "want %v, got %v", tt.want, got)
		})
	}
}

// createTestLogRecord creates a log record for testing.
func createTestLogRecord(t *testing.T) sdklog.Record {
	t.Helper()

	var rec sdklog.Record
	rec.SetTimestamp(time.Now())
	rec.SetObservedTimestamp(time.Now())
	rec.SetSeverity(log.SeverityInfo)
	rec.SetSeverityText("INFO")
	rec.SetBody(attribute.StringValue("test message"))
	rec.SetAttributes(
		attribute.String("key1", "value1"),
		attribute.Int("key2", 42),
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

func TestLogExporter_JSONEncoding(t *testing.T) {
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
