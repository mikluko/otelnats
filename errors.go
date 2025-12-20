package otelnats

import "errors"

var (
	errNilConnection    = errors.New("otelnats: nil NATS connection")
	errReceiverShutdown = errors.New("otelnats: receiver already shut down")
)
