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
	fetchRetryDelay       = 100 * time.Millisecond // delay after fetch errors before retry
	minFetchTimeout       = time.Second            // minimum timeout for fetch operations
)
