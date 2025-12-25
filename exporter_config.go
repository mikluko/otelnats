package otelnats

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

const (
	defaultSubjectPrefix = "otel"
	defaultTimeout       = 5 * time.Second
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
	return BuildSubject(c.subjectPrefix, signal, c.subjectSuffix)
}

func (c *config) contentType() string {
	return ContentType(c.encoding)
}

func (c *config) marshal(m proto.Message) ([]byte, error) {
	return Marshal(m, c.encoding)
}

func (c *config) buildHeaders(ctx context.Context, signal string) nats.Header {
	return BuildHeaders(ctx, signal, c.encoding, c.headers)
}
