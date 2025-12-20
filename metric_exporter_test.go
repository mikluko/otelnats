package otelnats

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	"google.golang.org/protobuf/proto"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

func TestNewMetricExporter(t *testing.T) {
	t.Run("nil connection returns error", func(t *testing.T) {
		exp, err := NewMetricExporter(nil)
		require.Error(t, err)
		require.Nil(t, exp)
		require.Equal(t, errNilConnection, err)
	})

	t.Run("valid connection succeeds", func(t *testing.T) {
		ns := startEmbeddedNATS(t)
		nc := connectToNATS(t, ns)

		exp, err := NewMetricExporter(nc)
		require.NoError(t, err)
		require.NotNil(t, exp)
	})
}

func TestMetricExporter_Export(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	t.Run("nil metrics does nothing", func(t *testing.T) {
		exp, err := NewMetricExporter(nc)
		require.NoError(t, err)

		err = exp.Export(ctx, nil)
		require.NoError(t, err)
	})

	t.Run("exports metrics with correct subject and headers", func(t *testing.T) {
		exp, err := NewMetricExporter(nc, WithSubjectPrefix("test"))
		require.NoError(t, err)

		// Subscribe to receive the message
		sub, err := nc.SubscribeSync("test.metrics")
		require.NoError(t, err)
		defer sub.Unsubscribe()

		// Create test metrics
		rm := createTestResourceMetrics(t)

		// Export
		err = exp.Export(ctx, rm)
		require.NoError(t, err)

		// Verify message received
		msg := requireMessage(t, sub, 5*time.Second)

		// Check headers
		require.Equal(t, contentTypeProtobuf, msg.Header.Get(headerContentType))
		require.Equal(t, signalMetrics, msg.Header.Get(headerOtelSignal))

		// Verify payload is valid protobuf
		var metricsData metricspb.MetricsData
		err = proto.Unmarshal(msg.Data, &metricsData)
		require.NoError(t, err)

		// Verify structure
		require.Len(t, metricsData.ResourceMetrics, 1)
		require.Len(t, metricsData.ResourceMetrics[0].ScopeMetrics, 1)
		require.Len(t, metricsData.ResourceMetrics[0].ScopeMetrics[0].Metrics, 1)

		// Verify metric content
		m := metricsData.ResourceMetrics[0].ScopeMetrics[0].Metrics[0]
		require.Equal(t, "test.counter", m.Name)
		require.Equal(t, "A test counter", m.Description)
	})
}

func TestMetricExporter_Temporality(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)

	exp, err := NewMetricExporter(nc)
	require.NoError(t, err)

	// Default should be cumulative
	require.Equal(t, metricdata.CumulativeTemporality, exp.Temporality(metric.InstrumentKindCounter))
	require.Equal(t, metricdata.CumulativeTemporality, exp.Temporality(metric.InstrumentKindHistogram))
}

func TestMetricExporter_Aggregation(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)

	exp, err := NewMetricExporter(nc)
	require.NoError(t, err)

	// Should return default aggregation
	agg := exp.Aggregation(metric.InstrumentKindCounter)
	require.NotNil(t, agg)
}

func TestMetricExporter_Shutdown(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	exp, err := NewMetricExporter(nc)
	require.NoError(t, err)

	// Shutdown should succeed
	err = exp.Shutdown(ctx)
	require.NoError(t, err)

	// Export after shutdown should return nil (not error)
	rm := createTestResourceMetrics(t)
	err = exp.Export(ctx, rm)
	require.NoError(t, err)
}

func TestMetricExporter_ForceFlush(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	exp, err := NewMetricExporter(nc)
	require.NoError(t, err)

	err = exp.ForceFlush(ctx)
	require.NoError(t, err)
}

func TestMetricExporter_Roundtrip(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc := connectToNATS(t, ns)
	ctx := t.Context()

	// Create exporter and receiver
	exp, err := NewMetricExporter(nc, WithSubjectPrefix("rt"))
	require.NoError(t, err)

	recv, err := NewReceiver(nc, WithReceiverSubjectPrefix("rt"))
	require.NoError(t, err)

	// Track received data
	received := make(chan *metricspb.MetricsData, 1)
	recv.OnMetrics(func(ctx context.Context, data *metricspb.MetricsData) error {
		received <- data
		return nil
	})

	require.NoError(t, recv.Start(ctx))
	defer recv.Shutdown(ctx)

	// Export metrics
	rm := createTestResourceMetrics(t)
	require.NoError(t, exp.Export(ctx, rm))

	// Verify roundtrip
	select {
	case data := <-received:
		require.Len(t, data.ResourceMetrics, 1)
		m := data.ResourceMetrics[0].ScopeMetrics[0].Metrics[0]
		require.Equal(t, "test.counter", m.Name)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

// createTestResourceMetrics creates a ResourceMetrics for testing.
func createTestResourceMetrics(t *testing.T) *metricdata.ResourceMetrics {
	t.Helper()

	now := time.Now()

	return &metricdata.ResourceMetrics{
		Resource: resource.NewSchemaless(
			attribute.String("service.name", "test-service"),
		),
		ScopeMetrics: []metricdata.ScopeMetrics{
			{
				Scope: instrumentation.Scope{
					Name:    "test-scope",
					Version: "1.0.0",
				},
				Metrics: []metricdata.Metrics{
					{
						Name:        "test.counter",
						Description: "A test counter",
						Unit:        "1",
						Data: metricdata.Sum[int64]{
							DataPoints: []metricdata.DataPoint[int64]{
								{
									Attributes: attribute.NewSet(
										attribute.String("key1", "value1"),
									),
									StartTime: now.Add(-time.Minute),
									Time:      now,
									Value:     42,
								},
							},
							Temporality: metricdata.CumulativeTemporality,
							IsMonotonic: true,
						},
					},
				},
			},
		},
	}
}

// Compile-time check that MetricExporter implements metric.Exporter
var _ metric.Exporter = (*MetricExporter)(nil)
