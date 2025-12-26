package otelnats

import (
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// spansToTracesData converts SDK spans to OTLP TracesData proto.
//
// Spans are grouped by resource and instrumentation scope to minimize
// the size of the serialized data.
func spansToTracesData(spans []sdktrace.ReadOnlySpan) *tracepb.TracesData {
	type scopeKey struct {
		name    string
		version string
	}

	resourceMap := make(map[string]*tracepb.ResourceSpans)
	scopeMap := make(map[string]map[scopeKey]*tracepb.ScopeSpans)

	for _, span := range spans {
		res := span.Resource()
		resKey := traceResourceKey(res)

		rs, ok := resourceMap[resKey]
		if !ok {
			rs = &tracepb.ResourceSpans{
				Resource: traceResourceToProto(res),
			}
			resourceMap[resKey] = rs
			scopeMap[resKey] = make(map[scopeKey]*tracepb.ScopeSpans)
		}

		scope := span.InstrumentationScope()
		sk := scopeKey{name: scope.Name, version: scope.Version}

		ss, ok := scopeMap[resKey][sk]
		if !ok {
			ss = &tracepb.ScopeSpans{
				Scope: traceScopeToProto(scope),
			}
			scopeMap[resKey][sk] = ss
			rs.ScopeSpans = append(rs.ScopeSpans, ss)
		}

		ss.Spans = append(ss.Spans, spanToProto(span))
	}

	result := &tracepb.TracesData{}
	for _, rs := range resourceMap {
		result.ResourceSpans = append(result.ResourceSpans, rs)
	}

	return result
}

func traceResourceKey(res *resource.Resource) string {
	if res == nil {
		return ""
	}
	var key string
	for _, kv := range res.Attributes() {
		key += string(kv.Key) + "=" + kv.Value.AsString() + ";"
	}
	return key
}

func traceResourceToProto(res *resource.Resource) *resourcepb.Resource {
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

func traceScopeToProto(scope instrumentation.Scope) *commonpb.InstrumentationScope {
	return &commonpb.InstrumentationScope{
		Name:    scope.Name,
		Version: scope.Version,
	}
}

func spanToProto(span sdktrace.ReadOnlySpan) *tracepb.Span {
	sc := span.SpanContext()
	parent := span.Parent()

	traceID := sc.TraceID()
	spanID := sc.SpanID()

	s := &tracepb.Span{
		TraceId:                traceID[:],
		SpanId:                 spanID[:],
		TraceState:             sc.TraceState().String(),
		Name:                   span.Name(),
		Kind:                   spanKindToProto(span.SpanKind()),
		StartTimeUnixNano:      uint64(span.StartTime().UnixNano()),
		EndTimeUnixNano:        uint64(span.EndTime().UnixNano()),
		DroppedAttributesCount: uint32(span.DroppedAttributes()),
		DroppedEventsCount:     uint32(span.DroppedEvents()),
		DroppedLinksCount:      uint32(span.DroppedLinks()),
		Status:                 statusToProto(span.Status()),
	}

	if parent.IsValid() {
		parentSpanID := parent.SpanID()
		s.ParentSpanId = parentSpanID[:]
	}

	// Convert attributes
	for _, kv := range span.Attributes() {
		s.Attributes = append(s.Attributes, &commonpb.KeyValue{
			Key:   string(kv.Key),
			Value: attributeValueToProto(kv.Value),
		})
	}

	// Convert events
	for _, event := range span.Events() {
		s.Events = append(s.Events, eventToProto(event))
	}

	// Convert links
	for _, link := range span.Links() {
		s.Links = append(s.Links, linkToProto(link))
	}

	return s
}

func spanKindToProto(kind trace.SpanKind) tracepb.Span_SpanKind {
	switch kind {
	case trace.SpanKindInternal:
		return tracepb.Span_SPAN_KIND_INTERNAL
	case trace.SpanKindServer:
		return tracepb.Span_SPAN_KIND_SERVER
	case trace.SpanKindClient:
		return tracepb.Span_SPAN_KIND_CLIENT
	case trace.SpanKindProducer:
		return tracepb.Span_SPAN_KIND_PRODUCER
	case trace.SpanKindConsumer:
		return tracepb.Span_SPAN_KIND_CONSUMER
	default:
		return tracepb.Span_SPAN_KIND_UNSPECIFIED
	}
}

func statusToProto(status sdktrace.Status) *tracepb.Status {
	s := &tracepb.Status{
		Message: status.Description,
	}
	switch status.Code {
	case codes.Ok:
		s.Code = tracepb.Status_STATUS_CODE_OK
	case codes.Error:
		s.Code = tracepb.Status_STATUS_CODE_ERROR
	default:
		s.Code = tracepb.Status_STATUS_CODE_UNSET
	}
	return s
}

func eventToProto(event sdktrace.Event) *tracepb.Span_Event {
	e := &tracepb.Span_Event{
		TimeUnixNano:           uint64(event.Time.UnixNano()),
		Name:                   event.Name,
		DroppedAttributesCount: uint32(event.DroppedAttributeCount),
	}
	for _, kv := range event.Attributes {
		e.Attributes = append(e.Attributes, &commonpb.KeyValue{
			Key:   string(kv.Key),
			Value: attributeValueToProto(kv.Value),
		})
	}
	return e
}

func linkToProto(link sdktrace.Link) *tracepb.Span_Link {
	traceID := link.SpanContext.TraceID()
	spanID := link.SpanContext.SpanID()

	l := &tracepb.Span_Link{
		TraceId:                traceID[:],
		SpanId:                 spanID[:],
		TraceState:             link.SpanContext.TraceState().String(),
		DroppedAttributesCount: uint32(link.DroppedAttributeCount),
	}
	for _, kv := range link.Attributes {
		l.Attributes = append(l.Attributes, &commonpb.KeyValue{
			Key:   string(kv.Key),
			Value: attributeValueToProto(kv.Value),
		})
	}
	return l
}
