package otelnats

import (
	"context"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

const defaultFlushTimeout = 5 * time.Second

type lifecycle struct {
	nc       *nats.Conn
	mu       sync.Mutex
	shutdown bool
}

func (e *lifecycle) isShutdown() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.shutdown
}

// ForceFlush flushes any buffered metric data.
func (e *lifecycle) ForceFlush(ctx context.Context) error {
	e.mu.Lock()
	if e.shutdown {
		e.mu.Unlock()
		return nil
	}
	e.mu.Unlock()

	// Add default timeout if context has no deadline
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultFlushTimeout)
		defer cancel()
	}

	return e.nc.FlushWithContext(ctx)
}

// Shutdown shuts down the exporter.
//
// After Shutdown is called, Export will return immediately without error.
func (e *lifecycle) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	e.shutdown = true
	e.mu.Unlock()
	return nil
}
