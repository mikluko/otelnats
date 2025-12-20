package otelnats

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

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

const (
	defaultSubjectPrefix = "otel"
	defaultTimeout       = 5 * time.Second

	signalTraces  = "traces"
	signalMetrics = "metrics"
	signalLogs    = "logs"

	headerContentType = "Content-Type"
	headerOtelSignal  = "Otel-Signal"

	contentTypeProtobuf = "application/x-protobuf"
	contentTypeJSON     = "application/json"
)

// config holds shared configuration for exporters and receivers.
type config struct {
	subjectPrefix string
	subjectSuffix string
	timeout       time.Duration
	encoding      Encoding
	jetstream     jetstream.JetStream
	headers       func(context.Context) nats.Header
}

func defaultConfig() *config {
	return &config{
		subjectPrefix: defaultSubjectPrefix,
		timeout:       defaultTimeout,
	}
}

func (c *config) subject(signal string) string {
	s := c.subjectPrefix + "." + signal
	if c.subjectSuffix != "" {
		s += "." + c.subjectSuffix
	}
	return s
}

func (c *config) contentType() string {
	if c.encoding == EncodingJSON {
		return contentTypeJSON
	}
	return contentTypeProtobuf
}

func (c *config) marshal(m proto.Message) ([]byte, error) {
	if c.encoding == EncodingJSON {
		return protojson.Marshal(m)
	}
	return proto.Marshal(m)
}

func (c *config) buildHeaders(ctx context.Context, signal string) nats.Header {
	h := nats.Header{}
	h.Set(headerContentType, c.contentType())
	h.Set(headerOtelSignal, signal)

	if c.headers != nil {
		for k, v := range c.headers(ctx) {
			for _, vv := range v {
				h.Add(k, vv)
			}
		}
	}

	return h
}
