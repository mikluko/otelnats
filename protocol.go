package otelnats

import (
	"context"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Protocol constants for OTLP over NATS.
// These are exported to enable protocol-compatible implementations
// without depending on the full SDK exporters/receivers.

// Encoding specifies the serialization format for OTLP messages.
type Encoding int

const (
	// EncodingProtobuf uses Protocol Buffers serialization (default).
	// Content-Type: application/x-protobuf
	EncodingProtobuf Encoding = iota

	// EncodingJSON uses JSON serialization.
	// Content-Type: application/json
	EncodingJSON
)

// Header keys used in OTLP NATS messages.
const (
	HeaderContentType = "Content-Type"
	HeaderOtelSignal  = "Otel-Signal"
)

// Content-Type header values for different encodings.
const (
	ContentTypeProtobuf = "application/x-protobuf"
	ContentTypeJSON     = "application/json"
)

// Signal types for OTLP telemetry.
const (
	SignalTraces  = "traces"
	SignalMetrics = "metrics"
	SignalLogs    = "logs"
)

// BuildSubject constructs an OTLP subject following the otelnats convention.
//
// The subject format is:
//   - Without suffix: "{prefix}.{signal}"
//   - With suffix: "{prefix}.{signal}.{suffix}"
//
// Examples:
//
//	BuildSubject("otel", "traces", "") → "otel.traces"
//	BuildSubject("otel", "logs", "tenant-a") → "otel.logs.tenant-a"
//	BuildSubject("myapp", "metrics", "") → "myapp.metrics"
func BuildSubject(prefix, signal, suffix string) string {
	s := prefix + "." + signal
	if suffix != "" {
		s += "." + suffix
	}
	return s
}

// ContentType returns the MIME type for the given encoding.
//
// Returns:
//   - "application/x-protobuf" for EncodingProtobuf
//   - "application/json" for EncodingJSON
func ContentType(enc Encoding) string {
	if enc == EncodingJSON {
		return ContentTypeJSON
	}
	return ContentTypeProtobuf
}

// Marshal serializes a protobuf message using the specified encoding.
//
// Supports:
//   - EncodingProtobuf: Binary protobuf format
//   - EncodingJSON: JSON format using protojson
//
// This is the canonical encoding method for OTLP messages in otelnats.
func Marshal(m proto.Message, enc Encoding) ([]byte, error) {
	if enc == EncodingJSON {
		return protojson.Marshal(m)
	}
	return proto.Marshal(m)
}

// Unmarshal deserializes data into a protobuf message.
//
// Encoding is auto-detected from the contentType parameter:
//   - "application/json" → JSON decoding via protojson
//   - "application/x-protobuf" or empty → Binary protobuf decoding
//
// Defaults to protobuf for backward compatibility when contentType is empty.
func Unmarshal(data []byte, contentType string, m proto.Message) error {
	if contentType == ContentTypeJSON {
		return protojson.Unmarshal(data, m)
	}
	// Default to protobuf for backward compatibility
	return proto.Unmarshal(data, m)
}

// BuildHeaders constructs NATS headers for OTLP messages.
//
// Always includes:
//   - Content-Type header (based on encoding)
//   - Otel-Signal header (traces/metrics/logs)
//
// The customHeaders function is optional and can add additional headers.
// Built-in headers (Content-Type, Otel-Signal) cannot be overridden.
//
// Example:
//
//	headers := BuildHeaders(ctx, SignalTraces, EncodingProtobuf, func(ctx context.Context) nats.Header {
//	    return nats.Header{"X-Tenant-ID": []string{"tenant-a"}}
//	})
func BuildHeaders(
	ctx context.Context,
	signal string,
	encoding Encoding,
	customHeaders func(context.Context) nats.Header,
) nats.Header {
	h := nats.Header{}
	h.Set(HeaderContentType, ContentType(encoding))
	h.Set(HeaderOtelSignal, signal)

	if customHeaders != nil {
		for k, v := range customHeaders(ctx) {
			for _, vv := range v {
				h.Add(k, vv)
			}
		}
	}

	return h
}

// OTLPSubjects returns NATS subjects for all three OTLP signals with the given prefix.
// The returned subjects are: {prefix}.logs, {prefix}.traces, {prefix}.metrics.
//
// An optional suffix can be provided to support hierarchical subjects.
// For example, OTLPSubjects("otel", ">") returns subjects with wildcard matching:
// otel.logs.>, otel.traces.>, otel.metrics.>
//
// Use this when creating a JetStream stream to capture OTLP telemetry:
//
//	js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
//	    Name:     "OTEL",
//	    Subjects: otelnats.OTLPSubjects("otel"),
//	    Storage:  jetstream.FileStorage,
//	})
func OTLPSubjects(prefix string, suffix ...string) []string {
	sfx := ""
	if len(suffix) > 0 && suffix[0] != "" {
		sfx = "." + suffix[0]
	}
	return []string{
		prefix + "." + SignalLogs + sfx,
		prefix + "." + SignalTraces + sfx,
		prefix + "." + SignalMetrics + sfx,
	}
}
