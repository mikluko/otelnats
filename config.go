package otelnats

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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
)

// config holds shared configuration for exporters and receivers.
type config struct {
	subjectPrefix string
	subjectSuffix string
	timeout       time.Duration
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

func (c *config) buildHeaders(ctx context.Context, signal string) nats.Header {
	h := nats.Header{}
	h.Set(headerContentType, contentTypeProtobuf)
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
