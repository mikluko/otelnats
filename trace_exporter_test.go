package otelnats

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func TestNewTraceExporter(t *testing.T) {
	t.Run("nil connection returns error", func(t *testing.T) {
		exp, err := NewTraceExporter(nil)
		require.Error(t, err)
		require.Nil(t, exp)
		require.Equal(t, errNilConnection, err)
	})

	t.Run("valid connection succeeds", func(t *testing.T) {
		ns := startEmbeddedNATS(t)
		nc := connectToNATS(t, ns)

		exp, err := NewTraceExporter(nc)
		require.NoError(t, err)
		require.NotNil(t, exp)
	})
}

func TestTraceExporter_ExportSpans(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	t.Run("empty spans does nothing", func(t *testing.T) {
		exp, err := NewTraceExporter(nc)
		require.NoError(t, err)

		err = exp.ExportSpans(ctx, nil)
		require.NoError(t, err)

		err = exp.ExportSpans(ctx, []sdktrace.ReadOnlySpan{})
		require.NoError(t, err)
	})

	t.Run("exports spans with correct subject and headers", func(t *testing.T) {
		exp, err := NewTraceExporter(nc, WithSubjectPrefix("test"))
		require.NoError(t, err)

		// Subscribe to receive the message
		sub, err := nc.SubscribeSync("test.traces")
		require.NoError(t, err)
		defer sub.Unsubscribe()

		// Create a test span
		span := createTestSpan(t)

		// Export
		err = exp.ExportSpans(ctx, []sdktrace.ReadOnlySpan{span})
		require.NoError(t, err)

		// Verify message received
		msg := requireMessage(t, sub, 5*time.Second)

		// Check headers
		require.Equal(t, ContentTypeProtobuf, msg.Header.Get(HeaderContentType))
		require.Equal(t, SignalTraces, msg.Header.Get(HeaderOtelSignal))

		// Verify payload is valid protobuf
		var tracesData tracepb.TracesData
		err = proto.Unmarshal(msg.Data, &tracesData)
		require.NoError(t, err)

		// Verify structure
		require.Len(t, tracesData.ResourceSpans, 1)
		require.Len(t, tracesData.ResourceSpans[0].ScopeSpans, 1)
		require.Len(t, tracesData.ResourceSpans[0].ScopeSpans[0].Spans, 1)

		// Verify span content
		s := tracesData.ResourceSpans[0].ScopeSpans[0].Spans[0]
		require.Equal(t, "test-span", s.Name)
		require.Equal(t, tracepb.Span_SPAN_KIND_INTERNAL, s.Kind)
	})
}

func TestTraceExporter_Shutdown(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	exp, err := NewTraceExporter(nc)
	require.NoError(t, err)

	// Shutdown should succeed
	err = exp.Shutdown(ctx)
	require.NoError(t, err)

	// ExportSpans after shutdown should return nil (not error)
	span := createTestSpan(t)
	err = exp.ExportSpans(ctx, []sdktrace.ReadOnlySpan{span})
	require.NoError(t, err)
}

func TestTraceExporter_SpanGrouping(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	exp, err := NewTraceExporter(nc, WithSubjectPrefix("group"))
	require.NoError(t, err)

	sub, err := nc.SubscribeSync("group.traces")
	require.NoError(t, err)
	defer sub.Unsubscribe()

	// Create multiple spans
	span1 := createTestSpan(t)
	span2 := createTestSpan(t)

	err = exp.ExportSpans(ctx, []sdktrace.ReadOnlySpan{span1, span2})
	require.NoError(t, err)

	msg := requireMessage(t, sub, 5*time.Second)

	var tracesData tracepb.TracesData
	err = proto.Unmarshal(msg.Data, &tracesData)
	require.NoError(t, err)

	// Should have one ResourceSpans with one ScopeSpans containing both spans
	require.Len(t, tracesData.ResourceSpans, 1)
	require.Len(t, tracesData.ResourceSpans[0].ScopeSpans, 1)
	require.Len(t, tracesData.ResourceSpans[0].ScopeSpans[0].Spans, 2)
}

func TestTraceExporter_Roundtrip(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	// Create exporter and receiver
	exp, err := NewTraceExporter(nc, WithSubjectPrefix("rt"))
	require.NoError(t, err)

	recv, err := NewReceiver(nc, WithReceiverSubjectPrefix("rt"))
	require.NoError(t, err)

	// Track received data
	received := make(chan *tracepb.TracesData, 1)
	recv.OnTraces(func(ctx context.Context, data *tracepb.TracesData) error {
		received <- data
		return nil
	})

	require.NoError(t, recv.Start(ctx))
	defer recv.Shutdown(ctx)

	// Export a span
	span := createTestSpan(t)
	require.NoError(t, exp.ExportSpans(ctx, []sdktrace.ReadOnlySpan{span}))

	// Verify roundtrip
	select {
	case data := <-received:
		require.Len(t, data.ResourceSpans, 1)
		s := data.ResourceSpans[0].ScopeSpans[0].Spans[0]
		require.Equal(t, "test-span", s.Name)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

// createTestSpan creates a span for testing using tracetest.SpanStub.
func createTestSpan(t *testing.T) sdktrace.ReadOnlySpan {
	t.Helper()

	traceID, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	spanID, _ := trace.SpanIDFromHex("0102030405060708")

	stub := tracetest.SpanStub{
		Name:      "test-span",
		SpanKind:  trace.SpanKindInternal,
		StartTime: time.Now().Add(-time.Second),
		EndTime:   time.Now(),
		SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: traceID,
			SpanID:  spanID,
		}),
		Attributes: []attribute.KeyValue{
			attribute.String("key1", "value1"),
			attribute.Int64("key2", 42),
		},
		Status: sdktrace.Status{
			Code:        codes.Ok,
			Description: "success",
		},
	}

	return stub.Snapshot()
}

// Compile-time check that TraceExporter implements sdktrace.SpanExporter
var _ sdktrace.SpanExporter = (*TraceExporter)(nil)
