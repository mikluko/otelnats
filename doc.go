// Package otelnats provides OpenTelemetry exporters and receivers for NATS.
//
// This package enables bidirectional OTLP transport over NATS:
//   - Exporters: Publish telemetry from OTel SDK to NATS subjects
//   - Receivers: Consume telemetry from NATS for processing/forwarding
//
// # Exporters
//
// Exporters implement the standard OTel SDK interfaces:
//   - [LogExporter] implements [go.opentelemetry.io/otel/sdk/log.Exporter]
//   - TraceExporter implements [go.opentelemetry.io/otel/sdk/trace.SpanExporter]
//   - MetricExporter implements [go.opentelemetry.io/otel/sdk/metric.Exporter]
//
// # Subject Naming
//
// Default subjects (configurable via [WithSubjectPrefix]):
//   - otel.traces
//   - otel.metrics
//   - otel.logs
//
// # Message Format
//
// All messages use protobuf-serialized OTLP data with headers:
//   - Content-Type: application/x-protobuf
//   - Otel-Signal: traces | metrics | logs
//
// # Example Usage
//
//	nc, _ := nats.Connect(nats.DefaultURL)
//
//	// Create log exporter
//	logExp, _ := otelnats.NewLogExporter(nc,
//	    otelnats.WithSubjectPrefix("myapp"))
//
//	// Use with OTel SDK
//	lp := log.NewLoggerProvider(
//	    log.WithProcessor(log.NewBatchProcessor(logExp)))
package otelnats
