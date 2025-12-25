// Package otelnats provides OpenTelemetry exporters and receivers for NATS.
//
// This package enables bidirectional OTLP transport over NATS with support for
// both core NATS (at-most-once) and JetStream (at-least-once) delivery:
//   - Exporters: Publish telemetry from OTel SDK to NATS subjects
//   - Receivers: Consume telemetry from NATS for processing/forwarding
//
// # Exporters
//
// Exporters implement the standard OTel SDK interfaces:
//   - [NewLogExporter] returns [go.opentelemetry.io/otel/sdk/log.Exporter]
//   - [NewTraceExporter] returns [go.opentelemetry.io/otel/sdk/trace.SpanExporter]
//   - [NewMetricExporter] returns [go.opentelemetry.io/otel/sdk/metric.Exporter]
//
// Exporters support both core NATS and JetStream publishing. Use [WithExporterJetStream]
// for reliable delivery with persistence and at-least-once semantics.
//
// Encoding can be configured with [WithExporterEncoding]:
//   - [EncodingProtobuf] (default): Binary protobuf format
//   - [EncodingJSON]: JSON format using protojson
//
// # Receivers
//
// Receivers consume OTLP telemetry from NATS subjects and route messages to handlers
// based on the Otel-Signal header. Each signal type requires an explicit handler:
//   - [WithReceiverLogsHandler]
//   - [WithReceiverTracesHandler]
//   - [WithReceiverMetricsHandler]
//
// Receivers use header-based routing, allowing a single durable consumer to handle
// multiple signal types. This prevents the load-balancing issues that occur when
// using separate consumers for each signal with the same consumer name.
//
// JetStream support provides durable consumers with at-least-once delivery:
//   - [WithReceiverJetStream]: Enable JetStream consumption
//   - [WithReceiverConsumerName]: Set durable consumer name
//   - [WithReceiverConsumer]: Bind to pre-created consumer
//   - [WithReceiverAckWait]: Set acknowledgment timeout
//   - [WithReceiverBacklogSize]: Set message buffer size
//
// Error handling for async errors (Nak, Ack, Term failures):
//   - Default: Panics to prevent silent failures
//   - [WithReceiverErrorHandler]: Custom error handling (e.g., logging)
//
// # Subject Naming
//
// Default subjects (configurable via [WithExporterSubjectPrefix] and [WithReceiverSubjectPrefix]):
//   - otel.traces
//   - otel.metrics
//   - otel.logs
//
// Multi-tenant support via subject suffixes ([WithExporterSubjectSuffix], [WithReceiverSubjectSuffix]):
//   - otel.logs.tenant-a
//   - otel.logs.tenant-b
//
// Wildcard subscriptions for multi-tenant receivers:
//   - otel.logs.> (matches all tenants)
//
// # Message Format
//
// Messages use OTLP protocol buffers or JSON with standard headers:
//   - Content-Type: application/x-protobuf OR application/json
//   - Otel-Signal: traces | metrics | logs
//
// The Otel-Signal header is used by receivers to route messages to the appropriate
// handler, decoupling message routing from NATS subject topology.
//
// # Protocol Primitives
//
// The package exports protocol functions and constants for building custom
// implementations without depending on the full exporters/receivers:
//   - [BuildSubject]: Construct OTLP subject names
//   - [BuildHeaders]: Construct NATS headers for OTLP messages
//   - [Marshal]: Serialize protobuf messages with encoding support
//   - [Unmarshal]: Deserialize protobuf messages with auto-detection
//   - [ContentType]: Get MIME type for encoding
//   - Constants: [HeaderContentType], [HeaderOtelSignal], [SignalLogs], etc.
//
// # Example: Basic Exporter
//
//	nc, _ := nats.Connect(nats.DefaultURL)
//
//	// Create log exporter
//	logExp, _ := otelnats.NewLogExporter(nc,
//	    otelnats.WithExporterSubjectPrefix("myapp"))
//
//	// Use with OTel SDK
//	lp := log.NewLoggerProvider(
//	    log.WithProcessor(log.NewBatchProcessor(logExp)))
//
// # Example: JetStream Exporter with JSON
//
//	nc, _ := nats.Connect(nats.DefaultURL)
//	js, _ := jetstream.New(nc)
//
//	exp, _ := otelnats.NewLogExporter(nc,
//	    otelnats.WithExporterJetStream(js),
//	    otelnats.WithExporterEncoding(otelnats.EncodingJSON))
//
// # Example: JetStream Receiver with Error Handling
//
//	nc, _ := nats.Connect(nats.DefaultURL)
//	js, _ := jetstream.New(nc)
//
//	recv, _ := otelnats.NewReceiver(nc,
//	    otelnats.WithReceiverJetStream(js, "otlp-stream"),
//	    otelnats.WithReceiverConsumerName("my-consumer"),
//	    otelnats.WithReceiverLogsHandler(func(ctx context.Context, msg otelnats.Message[logspb.LogsData]) error {
//	        data, err := msg.Item()
//	        if err != nil {
//	            return err
//	        }
//	        // Process logs...
//	        return nil
//	    }),
//	    otelnats.WithReceiverErrorHandler(func(err error) {
//	        log.Printf("receiver error: %v", err)
//	    }))
//
//	recv.Start(context.Background())
//
// # Example: Multi-Signal Receiver with Queue Group
//
//	recv, _ := otelnats.NewReceiver(nc,
//	    otelnats.WithReceiverQueueGroup("processors"),
//	    otelnats.WithReceiverLogsHandler(handleLogs),
//	    otelnats.WithReceiverTracesHandler(handleTraces),
//	    otelnats.WithReceiverMetricsHandler(handleMetrics))
package otelnats
