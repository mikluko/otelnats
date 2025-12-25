package otelnats

import "errors"

var (
	ErrNilConnection    = errors.New("otelnats: nil NATS connection")
	ErrReceiverShutdown = errors.New("otelnats: receiver already shut down")
	ErrUnmarshall       = errors.New("otelnats: failed to unmarshal data")
)
