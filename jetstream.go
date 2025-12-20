package otelnats

// OTLPSubjects returns NATS subjects for all three OTLP signals with the given prefix.
// The returned subjects are: {prefix}.logs, {prefix}.traces, {prefix}.metrics.
//
// Use this when creating a JetStream stream to capture OTLP telemetry:
//
//	js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
//	    Name:     "OTEL",
//	    Subjects: otelnats.OTLPSubjects("otel"),
//	    Storage:  jetstream.FileStorage,
//	})
func OTLPSubjects(prefix string) []string {
	return []string{
		prefix + "." + signalLogs,
		prefix + "." + signalTraces,
		prefix + "." + signalMetrics,
	}
}
