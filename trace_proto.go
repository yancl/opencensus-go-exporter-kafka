package kafka

import (
	tracepb "github.com/census-instrumentation/opencensus-proto/gen-go/trace/v1"
	timestamppb "github.com/golang/protobuf/ptypes/timestamp"
	wrapperspb "github.com/golang/protobuf/ptypes/wrappers"
	"go.opencensus.io/trace"
	"math"
	"time"
	"unicode/utf8"
)

const (
	maxAnnotationEventsPerSpan = 32
	maxMessageEventsPerSpan    = 128
	maxAttributeStringKey      = 128
	maxAttributeStringValue    = 256
	maxSpanNameLen             = 128
	defaultTimeEventNum        = 10
)

// protoFromSpanData returns a protocol buffer representation of a SpanData.
func protoFromSpanData(s *trace.SpanData) *tracepb.Span {
	if s == nil {
		return nil
	}

	sp := &tracepb.Span{
		TraceId:                 s.SpanContext.TraceID[:],
		SpanId:                  s.SpanContext.SpanID[:],
		Name:                    trunc(s.Name, maxSpanNameLen),
		Kind:                    spanKind(s),
		StartTime:               timestampProto(s.StartTime),
		EndTime:                 timestampProto(s.EndTime),
		SameProcessAsParentSpan: &wrapperspb.BoolValue{Value: !s.HasRemoteParent},
		Attributes:              convertToAttributes(s.Attributes),
		//StackTrace: &tracepb.StackTrace{},
		TimeEvents: convertToTimeEvents(s.Annotations, s.MessageEvents),
		Links:      convertToLinks(s.Links),
	}

	if s.ParentSpanID != (trace.SpanID{}) {
		sp.ParentSpanId = make([]byte, 8)
		copy(sp.ParentSpanId, s.ParentSpanID[:])
	}

	if s.Status.Code != 0 || s.Status.Message != "" {
		sp.Status = &tracepb.Status{Code: s.Status.Code, Message: s.Status.Message}
	}
	return sp
}

func convertToAttributes(attrs map[string]interface{}) *tracepb.Span_Attributes {
	if len(attrs) == 0 {
		return nil
	}
	attributes := &tracepb.Span_Attributes{
		AttributeMap: make(map[string]*tracepb.AttributeValue),
	}

	var dropped int32
	for k, v := range attrs {
		av := attributeValue(v)
		if av == nil {
			continue
		}
		if len(k) > maxAttributeStringKey {
			dropped++
			continue
		}
		attributes.AttributeMap[k] = av
	}

	attributes.DroppedAttributesCount = dropped
	return attributes
}

func convertToTimeEvents(as []trace.Annotation, es []trace.MessageEvent) *tracepb.Span_TimeEvents {
	if len(as) == 0 && len(es) == 0 {
		return nil
	}

	timeEvents := &tracepb.Span_TimeEvents{
		TimeEvent: make([]*tracepb.Span_TimeEvent, 0, defaultTimeEventNum),
	}

	var annotations, droppedAnnotationsCount int
	var messageEvents, droppedMessageEventsCount int

	// convert annotations
	for i, a := range as {
		if annotations >= maxAnnotationEventsPerSpan {
			droppedAnnotationsCount = len(as) - i
			break
		}
		annotations++
		timeEvents.TimeEvent = append(timeEvents.TimeEvent,
			&tracepb.Span_TimeEvent{
				Time:  timestampProto(a.Time),
				Value: convertAnnoationToTimeEvent(&a),
			},
		)
	}

	// convert message events
	for i, e := range es {
		if messageEvents >= maxMessageEventsPerSpan {
			droppedMessageEventsCount = len(es) - i
			break
		}
		messageEvents++
		timeEvents.TimeEvent = append(timeEvents.TimeEvent,
			&tracepb.Span_TimeEvent{
				Time:  timestampProto(e.Time),
				Value: convertMessageEventToTimeEvent(&e),
			},
		)
	}

	// process dropped counter
	timeEvents.DroppedAnnotationsCount = clip32(droppedAnnotationsCount)
	timeEvents.DroppedMessageEventsCount = clip32(droppedMessageEventsCount)

	return timeEvents
}

func convertToLinks(links []trace.Link) *tracepb.Span_Links {
	if len(links) == 0 {
		return nil
	}

	sls := &tracepb.Span_Links{
		Link: make([]*tracepb.Span_Link, 0, len(links)),
	}

	for _, l := range links {
		sl := &tracepb.Span_Link{
			TraceId:    l.TraceID[:],
			SpanId:     l.SpanID[:],
			Type:       tracepb.Span_Link_Type(l.Type),
			Attributes: convertToAttributes(l.Attributes),
		}
		sls.Link = append(sls.Link, sl)
	}
	return sls
}

func convertAnnoationToTimeEvent(a *trace.Annotation) *tracepb.Span_TimeEvent_Annotation_ {
	return &tracepb.Span_TimeEvent_Annotation_{
		Annotation: &tracepb.Span_TimeEvent_Annotation{
			Description: trunc(a.Message, maxAttributeStringValue),
			Attributes:  convertToAttributes(a.Attributes),
		},
	}
}

func convertMessageEventToTimeEvent(e *trace.MessageEvent) *tracepb.Span_TimeEvent_MessageEvent_ {
	return &tracepb.Span_TimeEvent_MessageEvent_{
		MessageEvent: &tracepb.Span_TimeEvent_MessageEvent{
			Type:             tracepb.Span_TimeEvent_MessageEvent_Type(e.EventType),
			Id:               uint64(e.MessageID),
			UncompressedSize: uint64(e.UncompressedByteSize),
			CompressedSize:   uint64(e.CompressedByteSize),
		},
	}
}

func spanKind(s *trace.SpanData) tracepb.Span_SpanKind {
	switch s.SpanKind {
	case trace.SpanKindClient:
		return trace.SpanKindClient
	case trace.SpanKindServer:
		return trace.SpanKindServer
	}
	return trace.SpanKindUnspecified
}

func attributeValue(v interface{}) *tracepb.AttributeValue {
	switch value := v.(type) {
	case bool:
		return &tracepb.AttributeValue{
			Value: &tracepb.AttributeValue_BoolValue{BoolValue: value},
		}
	case int64:
		return &tracepb.AttributeValue{
			Value: &tracepb.AttributeValue_IntValue{IntValue: value},
		}
	case string:
		return &tracepb.AttributeValue{
			Value: &tracepb.AttributeValue_StringValue{StringValue: trunc(value, maxAttributeStringValue)},
		}
	}
	return nil
}

// timestampProto creates a timestamp proto for a time.Time.
func timestampProto(t time.Time) *timestamppb.Timestamp {
	return &timestamppb.Timestamp{
		Seconds: t.Unix(),
		Nanos:   int32(t.Nanosecond()),
	}
}

// trunc returns a TruncatableString truncated to the given limit.
func trunc(s string, limit int) *tracepb.TruncatableString {
	if len(s) > limit {
		b := []byte(s[:limit])
		for {
			r, size := utf8.DecodeLastRune(b)
			if r == utf8.RuneError && size == 1 {
				b = b[:len(b)-1]
			} else {
				break
			}
		}
		return &tracepb.TruncatableString{
			Value:              string(b),
			TruncatedByteCount: clip32(len(s) - len(b)),
		}
	}
	return &tracepb.TruncatableString{
		Value:              s,
		TruncatedByteCount: 0,
	}
}

// clip32 clips an int to the range of an int32.
func clip32(x int) int32 {
	if x < math.MinInt32 {
		return math.MinInt32
	}
	if x > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(x)
}
