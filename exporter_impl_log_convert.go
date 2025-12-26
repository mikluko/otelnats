package otelnats

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// recordsToLogsData converts SDK log records to OTLP LogsData proto.
//
// Records are grouped by resource and instrumentation scope to minimize
// the size of the serialized data.
func recordsToLogsData(records []sdklog.Record) *logspb.LogsData {
	// Group records by resource and scope
	type scopeKey struct {
		name    string
		version string
	}

	resourceMap := make(map[string]*logspb.ResourceLogs)
	scopeMap := make(map[string]map[scopeKey]*logspb.ScopeLogs)

	for _, rec := range records {
		// Get resource key
		res := rec.Resource()
		resKey := resourceKey(res)

		// Get or create ResourceLogs
		rl, ok := resourceMap[resKey]
		if !ok {
			rl = &logspb.ResourceLogs{
				Resource: resourceToProto(res),
			}
			resourceMap[resKey] = rl
			scopeMap[resKey] = make(map[scopeKey]*logspb.ScopeLogs)
		}

		// Get scope key
		scope := rec.InstrumentationScope()
		sk := scopeKey{name: scope.Name, version: scope.Version}

		// Get or create ScopeLogs
		sl, ok := scopeMap[resKey][sk]
		if !ok {
			sl = &logspb.ScopeLogs{
				Scope: scopeToProto(scope),
			}
			scopeMap[resKey][sk] = sl
			rl.ScopeLogs = append(rl.ScopeLogs, sl)
		}

		// Add log record
		sl.LogRecords = append(sl.LogRecords, recordToProto(rec))
	}

	// Collect all ResourceLogs
	result := &logspb.LogsData{}
	for _, rl := range resourceMap {
		result.ResourceLogs = append(result.ResourceLogs, rl)
	}

	return result
}

func resourceKey(res *resource.Resource) string {
	if res == nil {
		return ""
	}
	// Use resource attributes as key
	var key string
	for _, kv := range res.Attributes() {
		key += string(kv.Key) + "=" + kv.Value.AsString() + ";"
	}
	return key
}

func resourceToProto(res *resource.Resource) *resourcepb.Resource {
	r := &resourcepb.Resource{}
	if res == nil {
		return r
	}
	for _, kv := range res.Attributes() {
		r.Attributes = append(r.Attributes, &commonpb.KeyValue{
			Key:   string(kv.Key),
			Value: attributeValueToProto(kv.Value),
		})
	}
	return r
}

func scopeToProto(scope instrumentation.Scope) *commonpb.InstrumentationScope {
	return &commonpb.InstrumentationScope{
		Name:    scope.Name,
		Version: scope.Version,
	}
}

func recordToProto(rec sdklog.Record) *logspb.LogRecord {
	lr := &logspb.LogRecord{
		TimeUnixNano:         uint64(rec.Timestamp().UnixNano()),
		ObservedTimeUnixNano: uint64(rec.ObservedTimestamp().UnixNano()),
		SeverityNumber:       logspb.SeverityNumber(rec.Severity()),
		SeverityText:         rec.SeverityText(),
		Body:                 logValueToProto(rec.Body()),
	}

	// Add trace context if present
	traceID := rec.TraceID()
	if traceID.IsValid() {
		lr.TraceId = traceID[:]
	}
	spanID := rec.SpanID()
	if spanID.IsValid() {
		lr.SpanId = spanID[:]
	}

	// Add attributes
	rec.WalkAttributes(func(kv log.KeyValue) bool {
		lr.Attributes = append(lr.Attributes, &commonpb.KeyValue{
			Key:   kv.Key,
			Value: logValueToProto(kv.Value),
		})
		return true
	})

	return lr
}

// attributeValueToProto converts an attribute.Value to proto AnyValue.
func attributeValueToProto(v attribute.Value) *commonpb.AnyValue {
	switch v.Type() {
	case attribute.BOOL:
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_BoolValue{BoolValue: v.AsBool()},
		}
	case attribute.FLOAT64:
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_DoubleValue{DoubleValue: v.AsFloat64()},
		}
	case attribute.INT64:
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_IntValue{IntValue: v.AsInt64()},
		}
	case attribute.STRING:
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{StringValue: v.AsString()},
		}
	case attribute.BOOLSLICE:
		vals := v.AsBoolSlice()
		arr := &commonpb.ArrayValue{Values: make([]*commonpb.AnyValue, len(vals))}
		for i, val := range vals {
			arr.Values[i] = &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: val}}
		}
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: arr}}
	case attribute.INT64SLICE:
		vals := v.AsInt64Slice()
		arr := &commonpb.ArrayValue{Values: make([]*commonpb.AnyValue, len(vals))}
		for i, val := range vals {
			arr.Values[i] = &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: val}}
		}
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: arr}}
	case attribute.FLOAT64SLICE:
		vals := v.AsFloat64Slice()
		arr := &commonpb.ArrayValue{Values: make([]*commonpb.AnyValue, len(vals))}
		for i, val := range vals {
			arr.Values[i] = &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: val}}
		}
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: arr}}
	case attribute.STRINGSLICE:
		vals := v.AsStringSlice()
		arr := &commonpb.ArrayValue{Values: make([]*commonpb.AnyValue, len(vals))}
		for i, val := range vals {
			arr.Values[i] = &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: val}}
		}
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: arr}}
	default:
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{StringValue: v.AsString()},
		}
	}
}

// logValueToProto converts a log.Value to proto AnyValue.
func logValueToProto(v log.Value) *commonpb.AnyValue {
	switch v.Kind() {
	case log.KindBool:
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_BoolValue{BoolValue: v.AsBool()},
		}
	case log.KindFloat64:
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_DoubleValue{DoubleValue: v.AsFloat64()},
		}
	case log.KindInt64:
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_IntValue{IntValue: v.AsInt64()},
		}
	case log.KindString:
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{StringValue: v.AsString()},
		}
	case log.KindBytes:
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_BytesValue{BytesValue: v.AsBytes()},
		}
	case log.KindSlice:
		items := v.AsSlice()
		arr := &commonpb.ArrayValue{
			Values: make([]*commonpb.AnyValue, len(items)),
		}
		for i, item := range items {
			arr.Values[i] = logValueToProto(item)
		}
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_ArrayValue{ArrayValue: arr},
		}
	case log.KindMap:
		items := v.AsMap()
		kvl := &commonpb.KeyValueList{
			Values: make([]*commonpb.KeyValue, len(items)),
		}
		for i, kv := range items {
			kvl.Values[i] = &commonpb.KeyValue{
				Key:   kv.Key,
				Value: logValueToProto(kv.Value),
			}
		}
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_KvlistValue{KvlistValue: kvl},
		}
	default:
		// KindEmpty or unknown
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{StringValue: v.AsString()},
		}
	}
}
