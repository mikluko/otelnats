package otelnats

const (
	defaultSubjectPrefix = "otel"
	defaultSubjectSuffix = ""
	defaultQueueGroup    = ""
	defaultEncoding      = EncodingProtobuf

	defaultAckWait     = 30 * 1000
	defaultBacklogSize = 100
)
