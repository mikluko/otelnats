package otelnats

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const defaultPublishTimeout = 5 * time.Second

type publisher interface {
	publish(ctx context.Context, msg *nats.Msg) error
}

type publisherCore struct {
	nc *nats.Conn
}

func (e *publisherCore) publish(ctx context.Context, msg *nats.Msg) error {
	if err := e.nc.PublishMsg(msg); err != nil {
		return err
	}

	// Add default timeout if context has no deadline
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultPublishTimeout)
		defer cancel()
	}

	return e.nc.FlushWithContext(ctx)
}

type publisherJetStream struct {
	js jetstream.JetStream
}

func (e *publisherJetStream) publish(ctx context.Context, msg *nats.Msg) error {
	// Add default timeout if context has no deadline
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultPublishTimeout)
		defer cancel()
	}

	_, err := e.js.PublishMsg(ctx, msg)
	return err
}
