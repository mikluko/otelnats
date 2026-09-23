# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/2.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.11.0] - 2026-09-23

### Changed

- The module requires Go 1.26, OpenTelemetry Go SDK v1.46.0 and the log SDK v0.22.0.
- An empty log body or attribute value is exported as an unset OTLP value instead of an empty string.

## [0.10.0] - 2026-08-14

### Added

- `WithExporterTemporality` sets the aggregation temporality of the metric exporter.

### Fixed

- The log exporter carries the record's `EventName`.
- The metric exporter carries exemplars.

## [0.9.0] - 2026-01-30

### Changed

- The module requires Go 1.24.

## [0.8.0] - 2026-01-28

### Added

- `WithReceiverRateLimit` and `WithReceiverFetchBatchSize` rate-limit the JetStream receiver.

## [0.7.1] - 2026-01-27

### Fixed

- `Receiver.Shutdown` no longer panics on `WaitGroup` reuse.

## [0.7.0] - 2025-12-26

### Added

- `WithReceiverSignalSubject` sets the subject a receiver subscribes to for one signal.

## [0.6.0] - 2025-12-26

### Added

- `Message`, `MessageSignal` and `MessageHandler`.
- `WithReceiverErrorHandler`, `WithReceiverBaseContext` and `WithReceiverBacklogSize`.
- Exported `Err*` error values.

### Changed

- **Breaking:** receiver handlers are set with `WithReceiverLogsHandler`, `WithReceiverMetricsHandler` and `WithReceiverTracesHandler`.
- **Breaking:** `NewReceiver` returns a `Receiver` interface; exporter constructors return the SDK exporter interfaces.
- **Breaking:** `NewTraceExporter` is renamed `NewSpanExporter`.
- **Breaking:** options are prefixed `WithExporter` or `WithReceiver`.
- The receiver routes messages by the `Otel-Signal` header.

### Removed

- **Breaking:** `Receiver.On*` callbacks, the `Receiver.Logs`, `Metrics` and `Traces` channels, and `WithChannelBufferSize`.
- **Breaking:** `WithTimeout`, `WithFetchBatchSize` and `WithFetchTimeout`.

## [0.5.0] - 2025-12-25

### Removed

- **Breaking:** `PublishMessage`.

## [0.4.0] - 2025-12-25

### Added

- Protocol primitives: `BuildSubject`, `BuildHeaders`, `ContentType`, `Marshal`, `Unmarshal`, `PublishMessage`, and the header, content-type and signal constants.

### Changed

- The JetStream receiver shuts down without waiting for a fetch timeout.

## [0.3.0] - 2025-12-20

### Added

- `WithEncoding` selects protobuf or JSON serialization.

## [0.2.0] - 2025-12-20

### Added

- `WithSubjectSuffix` and `WithReceiverSubjectSuffix` append a suffix to signal subjects.
- `OTLPSubjects` takes an optional suffix.

## [0.1.0] - 2025-12-20

### Added

- Log, metric and trace exporters that publish OTLP to NATS subjects, over Core NATS or JetStream.
- A receiver that consumes OTLP from NATS subjects, over Core NATS or JetStream.

[Unreleased]: https://github.com/mikluko/otelnats/compare/v0.11.0...HEAD
[0.11.0]: https://github.com/mikluko/otelnats/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/mikluko/otelnats/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/mikluko/otelnats/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/mikluko/otelnats/compare/v0.7.1...v0.8.0
[0.7.1]: https://github.com/mikluko/otelnats/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/mikluko/otelnats/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/mikluko/otelnats/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/mikluko/otelnats/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/mikluko/otelnats/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/mikluko/otelnats/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/mikluko/otelnats/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/mikluko/otelnats/releases/tag/v0.1.0
