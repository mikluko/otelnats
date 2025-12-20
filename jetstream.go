package otelnats

// OTLPSubjects returns NATS subjects for all three OTLP signals with the given prefix.
// The returned subjects are: {prefix}.logs, {prefix}.traces, {prefix}.metrics.
//
// An optional suffix can be provided to support hierarchical subjects.
// For example, OTLPSubjects("otel", ">") returns subjects with wildcard matching:
// otel.logs.>, otel.traces.>, otel.metrics.>
//
// Use this when creating a JetStream stream to capture OTLP telemetry:
//
//	js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
//	    Name:     "OTEL",
//	    Subjects: otelnats.OTLPSubjects("otel"),
//	    Storage:  jetstream.FileStorage,
//	})
func OTLPSubjects(prefix string, suffix ...string) []string {
	sfx := ""
	if len(suffix) > 0 && suffix[0] != "" {
		sfx = "." + suffix[0]
	}
	return []string{
		prefix + "." + signalLogs + sfx,
		prefix + "." + signalTraces + sfx,
		prefix + "." + signalMetrics + sfx,
	}
}
