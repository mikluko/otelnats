package otelnats

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"google.golang.org/protobuf/proto"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
)

func TestProtocolConstants(t *testing.T) {
	t.Run("header keys", func(t *testing.T) {
		require.Equal(t, "Content-Type", HeaderContentType)
		require.Equal(t, "Otel-Signal", HeaderOtelSignal)
	})

	t.Run("content types", func(t *testing.T) {
		require.Equal(t, "application/x-protobuf", ContentTypeProtobuf)
		require.Equal(t, "application/json", ContentTypeJSON)
	})

	t.Run("signal types", func(t *testing.T) {
		require.Equal(t, "traces", SignalTraces)
		require.Equal(t, "metrics", SignalMetrics)
		require.Equal(t, "logs", SignalLogs)
	})
}

func TestBuildSubject(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		signal string
		suffix string
		want   string
	}{
		{
			name:   "no suffix",
			prefix: "otel",
			signal: "traces",
			suffix: "",
			want:   "otel.traces",
		},
		{
			name:   "with tenant suffix",
			prefix: "otel",
			signal: "logs",
			suffix: "tenant-a",
			want:   "otel.logs.tenant-a",
		},
		{
			name:   "with wildcard suffix",
			prefix: "myapp",
			signal: "metrics",
			suffix: ">",
			want:   "myapp.metrics.>",
		},
		{
			name:   "custom prefix no suffix",
			prefix: "custom",
			signal: "traces",
			suffix: "",
			want:   "custom.traces",
		},
		{
			name:   "empty prefix",
			prefix: "",
			signal: "logs",
			suffix: "",
			want:   ".logs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildSubject(tt.prefix, tt.signal, tt.suffix)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestContentType(t *testing.T) {
	tests := []struct {
		name     string
		encoding Encoding
		want     string
	}{
		{
			name:     "protobuf encoding",
			encoding: EncodingProtobuf,
			want:     ContentTypeProtobuf,
		},
		{
			name:     "json encoding",
			encoding: EncodingJSON,
			want:     ContentTypeJSON,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ContentType(tt.encoding)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestMarshal(t *testing.T) {
	// Create test protobuf message
	msg := &logspb.LogsData{
		ResourceLogs: []*logspb.ResourceLogs{
			{
				ScopeLogs: []*logspb.ScopeLogs{
					{
						LogRecords: []*logspb.LogRecord{
							{
								Body: &commonpb.AnyValue{
									Value: &commonpb.AnyValue_StringValue{
										StringValue: "test",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	t.Run("protobuf encoding", func(t *testing.T) {
		data, err := Marshal(msg, EncodingProtobuf)
		require.NoError(t, err)
		require.NotEmpty(t, data)

		// Verify it's valid protobuf
		var decoded logspb.LogsData
		err = proto.Unmarshal(data, &decoded)
		require.NoError(t, err)
		require.Equal(t, "test", decoded.ResourceLogs[0].ScopeLogs[0].LogRecords[0].Body.GetStringValue())
	})

	t.Run("json encoding", func(t *testing.T) {
		data, err := Marshal(msg, EncodingJSON)
		require.NoError(t, err)
		require.NotEmpty(t, data)

		// Verify it's valid JSON (contains expected fields)
		require.Contains(t, string(data), "resourceLogs")
		require.Contains(t, string(data), "test")
	})
}

func TestUnmarshal(t *testing.T) {
	// Create test protobuf message
	original := &logspb.LogsData{
		ResourceLogs: []*logspb.ResourceLogs{
			{
				ScopeLogs: []*logspb.ScopeLogs{
					{
						LogRecords: []*logspb.LogRecord{
							{
								Body: &commonpb.AnyValue{
									Value: &commonpb.AnyValue_StringValue{
										StringValue: "test",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	t.Run("protobuf content type", func(t *testing.T) {
		data, _ := Marshal(original, EncodingProtobuf)

		var decoded logspb.LogsData
		err := Unmarshal(data, ContentTypeProtobuf, &decoded)
		require.NoError(t, err)
		require.Equal(t, "test", decoded.ResourceLogs[0].ScopeLogs[0].LogRecords[0].Body.GetStringValue())
	})

	t.Run("json content type", func(t *testing.T) {
		data, _ := Marshal(original, EncodingJSON)

		var decoded logspb.LogsData
		err := Unmarshal(data, ContentTypeJSON, &decoded)
		require.NoError(t, err)
		require.Equal(t, "test", decoded.ResourceLogs[0].ScopeLogs[0].LogRecords[0].Body.GetStringValue())
	})

	t.Run("empty content type defaults to protobuf", func(t *testing.T) {
		data, _ := Marshal(original, EncodingProtobuf)

		var decoded logspb.LogsData
		err := Unmarshal(data, "", &decoded)
		require.NoError(t, err)
		require.Equal(t, "test", decoded.ResourceLogs[0].ScopeLogs[0].LogRecords[0].Body.GetStringValue())
	})

	t.Run("unknown content type defaults to protobuf", func(t *testing.T) {
		data, _ := Marshal(original, EncodingProtobuf)

		var decoded logspb.LogsData
		err := Unmarshal(data, "application/octet-stream", &decoded)
		require.NoError(t, err)
		require.Equal(t, "test", decoded.ResourceLogs[0].ScopeLogs[0].LogRecords[0].Body.GetStringValue())
	})
}

func TestBuildHeaders(t *testing.T) {
	ctx := context.Background()

	t.Run("protobuf encoding", func(t *testing.T) {
		headers := BuildHeaders(ctx, SignalTraces, EncodingProtobuf, nil)

		require.Equal(t, ContentTypeProtobuf, headers.Get(HeaderContentType))
		require.Equal(t, SignalTraces, headers.Get(HeaderOtelSignal))
	})

	t.Run("json encoding", func(t *testing.T) {
		headers := BuildHeaders(ctx, SignalLogs, EncodingJSON, nil)

		require.Equal(t, ContentTypeJSON, headers.Get(HeaderContentType))
		require.Equal(t, SignalLogs, headers.Get(HeaderOtelSignal))
	})

	t.Run("with custom headers", func(t *testing.T) {
		customHeaders := func(ctx context.Context) nats.Header {
			return nats.Header{
				"X-Tenant-ID": []string{"tenant-a"},
				"X-Region":    []string{"us-east-1"},
			}
		}

		headers := BuildHeaders(ctx, SignalMetrics, EncodingProtobuf, customHeaders)

		// Built-in headers
		require.Equal(t, ContentTypeProtobuf, headers.Get(HeaderContentType))
		require.Equal(t, SignalMetrics, headers.Get(HeaderOtelSignal))

		// Custom headers
		require.Equal(t, "tenant-a", headers.Get("X-Tenant-ID"))
		require.Equal(t, "us-east-1", headers.Get("X-Region"))
	})

	t.Run("nil custom headers", func(t *testing.T) {
		headers := BuildHeaders(ctx, SignalTraces, EncodingProtobuf, nil)

		require.Equal(t, ContentTypeProtobuf, headers.Get(HeaderContentType))
		require.Equal(t, SignalTraces, headers.Get(HeaderOtelSignal))
		require.Len(t, headers, 2)
	})
}

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
