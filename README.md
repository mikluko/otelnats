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

	recv, _ := otelnats.NewReceiver(nc,
		otelnats.WithReceiverLogsHandler(func(ctx context.Context, msg otelnats.Message[logspb.LogsData]) error {
			data, err := msg.Item()
			if err != nil {
				return err
			}
			for _, rl := range data.ResourceLogs {
				for _, sl := range rl.ScopeLogs {
					for _, lr := range sl.LogRecords {
						fmt.Printf("[%s] %s\n", lr.SeverityText, lr.Body.GetStringValue())
					}
				}
			}
			return nil
		}),
	)

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

// JSON encoding (default: protobuf)
otelnats.NewLogExporter(nc, otelnats.WithEncoding(otelnats.EncodingJSON))

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
otelnats.NewReceiver(nc, otelnats.WithReceiverQueueGroup("processors"))

// JetStream consumption (at-least-once delivery)
js, _ := jetstream.New(nc)
otelnats.NewReceiver(nc,
    otelnats.WithReceiverJetStream(js, "TELEMETRY"),  // stream name
    otelnats.WithReceiverConsumerName("processor-1"), // durable consumer
    otelnats.WithReceiverAckWait(30*time.Second),     // ack timeout (default: 30s)
)
```

### Advanced Message Handling

For performance-critical or custom processing scenarios, use the generic `MessageHandler` API which provides access to both typed data and raw message bytes:

```go
recv, _ := otelnats.NewReceiver(nc,
    otelnats.WithReceiverLogsHandler(func(ctx context.Context, msg otelnats.Message[logspb.LogsData]) error {
        // Option 1: Access typed data (lazy unmarshaling)
        data, err := msg.Item()
        if err != nil {
            return err
        }
        for _, rl := range data.ResourceLogs {
            // Process logs...
        }

        // Option 2: Access raw bytes without unmarshaling
        rawBytes := msg.Data()

        // Access headers
        contentType := msg.Headers().Get(otelnats.HeaderContentType)

        // JetStream: explicit acknowledgment (optional, automatic by default)
        return msg.Ack()  // or msg.Nak() on error
    }),
)
```

**Use Cases:**

**1. Performance optimization** — Avoid double unmarshaling:
```go
recv, _ := otelnats.NewReceiver(nc,
    otelnats.WithReceiverLogsHandler(func(ctx context.Context, msg otelnats.Message[logspb.LogsData]) error {
        // Access raw bytes to pass to downstream processor without unmarshaling
        return forwardToProcessor(msg.Data(), msg.Headers())
    }),
)
```

**2. Custom unmarshaling** — Unmarshal to different types:
```go
recv, _ := otelnats.NewReceiver(nc,
    otelnats.WithReceiverLogsHandler(func(ctx context.Context, msg otelnats.Message[logspb.LogsData]) error {
        // Unmarshal to custom type for collector implementations
        var customData MyCustomLogsFormat
        contentType := msg.Headers().Get(otelnats.HeaderContentType)
        if err := otelnats.Unmarshal(msg.Data(), contentType, &customData); err != nil {
            return err
        }
        return processCustomFormat(customData)
    }),
)
```

**3. Conditional processing** — Inspect headers before unmarshaling:
```go
recv, _ := otelnats.NewReceiver(nc,
    otelnats.WithReceiverLogsHandler(func(ctx context.Context, msg otelnats.Message[logspb.LogsData]) error {
        // Check headers first
        tenant := msg.Headers().Get("X-Tenant-ID")
        if !shouldProcess(tenant) {
            return nil  // Skip processing
        }

        // Only unmarshal when needed
        data, err := msg.Item()
        if err != nil {
            return err
        }
        return processTenantLogs(tenant, data)
    }),
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
    otelnats.WithReceiverConsumer(consumer),
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
| Payload               | Serialized OTLP data (protobuf or JSON) |
| Header `Content-Type` | `application/x-protobuf` or `application/json` |
| Header `Otel-Signal`  | `traces` / `metrics` / `logs` |

Receivers auto-detect encoding from the `Content-Type` header.

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
type Receiver interface {
    Start(ctx context.Context) error
    Shutdown(ctx context.Context) error
}

// Constructor
func NewReceiver(nc *nats.Conn, opts ...ReceiverOption) (Receiver, error)

// Message interface
type Message[T any] interface {
    Item() (*T, error)    // Access typed data (lazy unmarshal)
    Data() []byte         // Access raw message bytes
    Headers() nats.Header // Access message headers
    Ack() error          // Acknowledge (JetStream only)
    Nak() error          // Negative acknowledge (JetStream only)
}

// Message handler type
type MessageHandler[T any] func(ctx context.Context, msg Message[T]) error

// Handler options (passed to NewReceiver)
func WithReceiverLogsHandler(fn MessageHandler[logspb.LogsData]) ReceiverOption
func WithReceiverTracesHandler(fn MessageHandler[tracespb.TracesData]) ReceiverOption
func WithReceiverMetricsHandler(fn MessageHandler[metricspb.MetricsData]) ReceiverOption
```

### Protocol Primitives

For building protocol-compatible implementations (custom exporters, receivers, gateways, testing tools), `otelnats` exports low-level protocol primitives:

```go
// Protocol constants
const (
    HeaderContentType = "Content-Type"
    HeaderOtelSignal  = "Otel-Signal"

    ContentTypeProtobuf = "application/x-protobuf"
    ContentTypeJSON     = "application/json"

    SignalTraces  = "traces"
    SignalMetrics = "metrics"
    SignalLogs    = "logs"
)

// Encoding type
type Encoding int

const (
    EncodingProtobuf Encoding = iota  // Default: binary protobuf
    EncodingJSON                      // JSON encoding
)

// Subject construction
func BuildSubject(prefix, signal, suffix string) string

// Header construction
func BuildHeaders(
    ctx context.Context,
    signal string,
    encoding Encoding,
    customHeaders func(context.Context) nats.Header,
) nats.Header

// Encoding helpers
func ContentType(enc Encoding) string
func Marshal(m proto.Message, enc Encoding) ([]byte, error)
func Unmarshal(data []byte, contentType string, m proto.Message) error
```

**Example**: Custom exporter using protocol primitives

```go
package main

import (
    "context"

    "github.com/mikluko/otelnats"
    "github.com/nats-io/nats.go"
    logspb "go.opentelemetry.io/proto/otlp/logs/v1"
)

func main() {
    nc, _ := nats.Connect(nats.DefaultURL)
    defer nc.Close()

    ctx := context.Background()

    // Build OTLP message
    logsData := &logspb.LogsData{ /* ... */ }

    // Serialize with protocol primitives
    data, _ := otelnats.Marshal(logsData, otelnats.EncodingProtobuf)

    // Build message
    msg := &nats.Msg{
        Subject: otelnats.BuildSubject("otel", otelnats.SignalLogs, "tenant-a"),
        Data:    data,
        Header:  otelnats.BuildHeaders(ctx, otelnats.SignalLogs, otelnats.EncodingProtobuf, nil),
    }

    // Publish using NATS directly
    nc.PublishMsg(msg)
    nc.FlushWithContext(ctx)
}
```

## Roadmap

- [x] JetStream consumer support for receivers (durable subscriptions, ack/nak)
- [x] Stream and consumer creation helpers
- [x] JSON encoding option
- [ ] Compression support

## Related Projects

- [otelnats-collector](https://github.com/mikluko/otelnats-collector) — OTel Collector with NATS exporter/receiver components
- [opentelemetry-collector-contrib#39540](https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/39540) — Proposal for native NATS exporter/receiver in OTel Collector Contrib

## License

[MIT](./LICENSE)
