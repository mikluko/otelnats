package otelnats

import (
	"context"
	"fmt"
	"sync"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

type MessageCore interface {
	Subject() string
	Data() []byte
	Headers() nats.Header
}

type MessageFlow interface {
	Ack() error
	Nak() error
	Term() error
}

type Message interface {
	MessageCore
	MessageFlow
}

type MessageSignal[T any] interface {
	Message
	// Signal must return ErrUnmarshal if the message data is invalid
	Signal() (*T, error)
}

type MessageHandler[T any] func(ctx context.Context, msg MessageSignal[T]) error

// messageCoreWrapper wraps a nats.Msg and provides generic typed access with lazy unmarshaling
type messageCoreWrapper struct {
	msg *nats.Msg
}

func (m *messageCoreWrapper) Subject() string {
	return m.msg.Subject
}

func (m *messageCoreWrapper) Data() []byte {
	return m.msg.Data
}

func (m *messageCoreWrapper) Headers() nats.Header {
	return m.msg.Header
}

func (m *messageCoreWrapper) Ack() error {
	return nil
}

func (m *messageCoreWrapper) Nak() error {
	return nil
}

func (m *messageCoreWrapper) Term() error {
	return nil
}

func messageSignalFrom[T any](msg Message) MessageSignal[T] {
	return &messageSignalImpl[T]{Message: msg}
}

// messageSignalImpl wraps a message and provides generic typed access with lazy unmarshaling
type messageSignalImpl[T any] struct {
	Message
	item    T
	itemErr error
	once    sync.Once
}

func (m *messageSignalImpl[T]) Signal() (*T, error) {
	m.once.Do(func() {
		contentType := m.Headers().Get(HeaderContentType)
		// T must be a proto.Message, so we need to convert it
		// This works because logspb.LogsData, tracespb.TracesData, metricspb.MetricsData all implement proto.Message
		if protoMsg, ok := any(&m.item).(proto.Message); ok {
			m.itemErr = Unmarshal(m.Data(), contentType, protoMsg)
			if m.itemErr != nil {
				m.itemErr = fmt.Errorf("%w: %v", ErrUnmarshal, m.itemErr)
			}
		} else {
			panic("invalid type T for MessageFrom, this is always a programming error")
		}
	})
	return &m.item, m.itemErr
}
