package otelnats

import "errors"

var (
	ErrNilConnection    = errors.New("otelnats: nil NATS connection")
	ErrReceiverShutdown = errors.New("otelnats: receiver already shut down")
	ErrUnmarshal        = errors.New("otelnats: failed to unmarshal data")
	ErrNoHandlers       = errors.New("otelnats: no message handlers configured")
)
