package otelnats

import "time"

const (
	defaultSubjectPrefix = "otel"
	defaultSubjectSuffix = ""
	defaultQueueGroup    = ""
	defaultEncoding      = EncodingProtobuf

	defaultAckWait        = 30 * time.Second
	defaultBacklogSize    = 100
	defaultFetchBatchSize = 100
	defaultFetchTimeout   = 5 * time.Second
)
