# otelnats

OpenTelemetry NATS transport for Go — exporters and receivers for traces, metrics, and logs.

## Overview

`otelnats` provides bidirectional OTLP transport over NATS:

- **Exporters**: Publish telemetry from OTel SDK to NATS subjects
- **Receivers**: Consume telemetry from NATS for processing/forwarding

### Use Cases

- **Unified telemetry pipeline**: Export all signals to NATS for centralized processing
- **Microservices**: Route telemetry through existing NATS infrastructure
- **Fan-out**: Multiple consumers (analytics, alerting, archival) subscribe to same subjects
- **Debugging**: Inspect telemetry flowing through NATS with `nats sub 'otel.>'`

## Installation

```bash
go get github.com/mikluko/otelnats
```

## Quick Start

### Exporter

```go
package main

import (
	"context"

	"github.com/mikluko/otelnats"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/trace"
)

func main() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	// Log exporter
	logExp, _ := otelnats.NewLogExporter(nc)
	logProvider := log.NewLoggerProvider(
		log.WithProcessor(log.NewBatchProcessor(logExp)),
	)
	defer logProvider.Shutdown(context.Background())

	// Trace exporter
	traceExp, _ := otelnats.NewTraceExporter(nc)
	traceProvider := trace.NewTracerProvider(
		trace.WithBatcher(traceExp),
	)
	defer traceProvider.Shutdown(context.Background())

	// Metric exporter
	metricExp, _ := otelnats.NewMetricExporter(nc)
	metricReader := metric.NewPeriodicReader(metricExp)
	meterProvider := metric.NewMeterProvider(
		metric.WithReader(metricReader),
	)
	defer meterProvider.Shutdown(context.Background())
}
```

### Receiver

```go
package main

import (
	"context"
	"fmt"

	"github.com/mikluko/otelnats"
	"github.com/nats-io/nats.go"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
)

func main() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	recv, _ := otelnats.NewReceiver(nc)

	// Callback API
	recv.OnLogs(func(ctx context.Context, data *logspb.LogsData) error {
		for _, rl := range data.ResourceLogs {
			for _, sl := range rl.ScopeLogs {
				for _, lr := range sl.LogRecords {
					fmt.Printf("[%s] %s\n", lr.SeverityText, lr.Body.GetStringValue())
				}
			}
		}
		return nil
	})

	// Or channel API
	tracesCh := recv.Traces()
	go func() {
		for data := range tracesCh {
			fmt.Printf("Received %d spans\n", countSpans(data))
		}
	}()

	recv.Start(context.Background())
	defer recv.Shutdown(context.Background())

	// Block forever
	select {}
}
```

## Configuration

### Exporter Options

```go
// Custom subject prefix (default: "otel")
otelnats.NewLogExporter(nc, otelnats.WithSubjectPrefix("myapp"))
// Publishes to: myapp.logs, myapp.traces, myapp.metrics

// Subject suffix for multi-tenant routing
otelnats.NewLogExporter(nc, otelnats.WithSubjectSuffix("tenant-a"))
// Publishes to: otel.logs.tenant-a, otel.traces.tenant-a, otel.metrics.tenant-a

// Custom timeout (default: 5s)
otelnats.NewLogExporter(nc, otelnats.WithTimeout(10*time.Second))

// JetStream publishing (at-least-once delivery)
js, _ := jetstream.New(nc)
otelnats.NewLogExporter(nc, otelnats.WithJetStream(js))

// Custom headers per message
otelnats.NewLogExporter(nc, otelnats.WithHeaders(func (ctx context.Context) nats.Header {
return nats.Header{"X-Tenant-ID": []string{tenantFromContext(ctx)}}
}))
```

### Receiver Options

```go
// Custom subject prefix
otelnats.NewReceiver(nc, otelnats.WithReceiverSubjectPrefix("myapp"))

// Subject suffix for filtering or wildcard subscriptions
otelnats.NewReceiver(nc, otelnats.WithReceiverSubjectSuffix("tenant-a"))
// Subscribes to: otel.logs.tenant-a, otel.traces.tenant-a, otel.metrics.tenant-a

// Wildcard suffix to receive from all tenants
otelnats.NewReceiver(nc, otelnats.WithReceiverSubjectSuffix(">"))
// Subscribes to: otel.logs.>, otel.traces.>, otel.metrics.>

// Queue group for load balancing
otelnats.NewReceiver(nc, otelnats.WithQueueGroup("processors"))

// Channel buffer size (default: 100)
otelnats.NewReceiver(nc, otelnats.WithChannelBufferSize(1000))

// JetStream consumption (at-least-once delivery with pull consumers)
js, _ := jetstream.New(nc)
otelnats.NewReceiver(nc,
    otelnats.WithReceiverJetStream(js, "TELEMETRY"),  // stream name
    otelnats.WithConsumerName("processor-1"),         // durable consumer
    otelnats.WithAckWait(30*time.Second),             // ack timeout (default: 30s)
    otelnats.WithFetchBatchSize(100),                 // messages per fetch (default: 10)
    otelnats.WithFetchTimeout(10*time.Second),        // fetch wait time (default: 5s)
)
```

### JetStream Acknowledgment

When using JetStream, the receiver automatically handles message acknowledgment:

- **Handler returns `nil`**: Message is acknowledged (won't be redelivered)
- **Handler returns error**: Message is NAK'd (triggers immediate redelivery)

### JetStream Setup

Use `OTLPSubjects` helper with the native jetstream API to create streams:

```go
js, _ := jetstream.New(nc)

// Create stream for OTLP telemetry
stream, _ := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
    Name:     "TELEMETRY",
    Subjects: otelnats.OTLPSubjects("otel"),  // returns: otel.logs, otel.traces, otel.metrics
    Storage:  jetstream.FileStorage,
    MaxAge:   24 * time.Hour,
})

// For multi-tenant, use wildcard suffix
stream, _ := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
    Name:     "TELEMETRY",
    Subjects: otelnats.OTLPSubjects("otel", ">"),  // returns: otel.logs.>, otel.traces.>, otel.metrics.>
    Storage:  jetstream.FileStorage,
})

// Create consumer
consumer, _ := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
    Durable:       "analytics",
    DeliverPolicy: jetstream.DeliverAllPolicy,
    FilterSubject: "otel.logs",
})

// Bind consumer to receiver
recv, _ := otelnats.NewReceiver(nc,
    otelnats.WithReceiverJetStream(js, "TELEMETRY"),
    otelnats.WithConsumer(consumer),
)
```

## Subject Naming

Default subjects (configurable via `WithSubjectPrefix` and `WithSubjectSuffix`):

| Signal  | Subject (no suffix) | Subject (with suffix) |
|---------|---------------------|----------------------|
| Logs    | `otel.logs`         | `otel.logs.{suffix}` |
| Traces  | `otel.traces`       | `otel.traces.{suffix}` |
| Metrics | `otel.metrics`      | `otel.metrics.{suffix}` |

## Message Format

| Field                 | Value                                   |
|-----------------------|-----------------------------------------|
| Subject               | `{prefix}.{signal}` or `{prefix}.{signal}.{suffix}` |
| Payload               | Protobuf-serialized OTLP data |
| Header `Content-Type` | `application/x-protobuf`      |
| Header `Otel-Signal`  | `traces` / `metrics` / `logs` |

## API Reference

### Exporters

All exporters implement their respective OTel SDK interfaces:

| Type             | Implements                                        | Constructor                        |
|------------------|---------------------------------------------------|------------------------------------|
| `LogExporter`    | `go.opentelemetry.io/otel/sdk/log.Exporter`       | `NewLogExporter(nc, ...Option)`    |
| `TraceExporter`  | `go.opentelemetry.io/otel/sdk/trace.SpanExporter` | `NewTraceExporter(nc, ...Option)`  |
| `MetricExporter` | `go.opentelemetry.io/otel/sdk/metric.Exporter`    | `NewMetricExporter(nc, ...Option)` |

### Receiver

```go
type Receiver struct { ... }

// Constructor
func NewReceiver(nc *nats.Conn, opts ...ReceiverOption) (*Receiver, error)

// Callback registration (call before Start)
func (r *Receiver) OnLogs(fn func (context.Context, *logspb.LogsData) error)
func (r *Receiver) OnTraces(fn func (context.Context, *tracespb.TracesData) error)
func (r *Receiver) OnMetrics(fn func (context.Context, *metricspb.MetricsData) error)

// Channel API (call before Start)
func (r *Receiver) Logs() <-chan *logspb.LogsData
func (r *Receiver) Traces() <-chan *tracespb.TracesData
func (r *Receiver) Metrics() <-chan *metricspb.MetricsData

// Lifecycle
func (r *Receiver) Start(ctx context.Context) error
func (r *Receiver) Shutdown(ctx context.Context) error
```

## Roadmap

- [x] JetStream consumer support for receivers (durable subscriptions, ack/nak)
- [x] Stream and consumer creation helpers
- [ ] Compression support
- [ ] JSON encoding option

## Related Projects

- [otelnats-collector](https://github.com/mikluko/otelnats-collector) — OTel Collector with NATS exporter/receiver components
- [opentelemetry-collector-contrib#39540](https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/39540) — Proposal for native NATS exporter/receiver in OTel Collector Contrib

## License

[MIT](./LICENSE)
