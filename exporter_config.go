package otelnats

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	defaultSubjectPrefix = "otel"
)

// config holds shared configuration for exporters and receivers.
type config struct {
	subjectPrefix string
	subjectSuffix string
	conn          *nats.Conn
	jetstream     jetstream.JetStream
	encoding      Encoding
	headers       func(context.Context) nats.Header
}

func defaultConfig(nc *nats.Conn) *config {
	return &config{
		conn:          nc,
		subjectPrefix: defaultSubjectPrefix,
	}
}

func (c *config) subject(signal string) string {
	return BuildSubject(c.subjectPrefix, signal, c.subjectSuffix)
}

func (c *config) contentType() string {
	return ContentType(c.encoding)
}

func (c *config) buildHeaders(ctx context.Context, signal string) nats.Header {
	return BuildHeaders(ctx, signal, c.encoding, c.headers)
}

func (c *config) marshaler() marshaler {
	switch c.encoding {
	case EncodingJSON:
		return marshalerJSON{}
	case EncodingProtobuf:
		return marshalerProtobuf{}
	default:
		panic("unknown encoding")
	}
}

func (c *config) publisher() publisher {
	if c.jetstream != nil {
		return &publisherJetStream{c.jetstream}
	}
	return &publisherCore{c.conn}
}
