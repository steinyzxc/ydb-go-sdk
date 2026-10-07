package topicwritercommon

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// WriterInfo contains the stable attributes shared by all streams belonging
// to one logical topic writer.
type WriterInfo struct {
	Endpoint   string
	Database   string
	Topic      string
	WriterName string
}

// WriterMetricDurationEvent is one duration sample observed by a writer
// operation.
type WriterMetricDurationEvent struct {
	Kind     trace.TopicWriterMetricDurationKind
	Duration time.Duration
}

// WriterMetricEvent batches the additive writer metric changes observed by a
// writer operation.
type WriterMetricEvent struct {
	MessagesDelta   int64
	BytesDelta      int64
	WrittenMessages int64
	Durations       []WriterMetricDurationEvent
}

// IsEmpty reports whether the event contains a counter or timer contribution.
func (e WriterMetricEvent) IsEmpty() bool {
	return e.MessagesDelta == 0 && e.BytesDelta == 0 &&
		e.WrittenMessages == 0 && len(e.Durations) == 0
}

// TraceSendingChanged emits the admitted-message and payload-byte deltas for a writer.
func TraceSendingChanged(
	ctx context.Context,
	tracer *trace.Topic,
	info WriterInfo,
	messagesDelta, bytesDelta int64,
) {
	if info.WriterName == "" || tracer == nil || tracer.OnWriterSendingChanged == nil ||
		(messagesDelta == 0 && bytesDelta == 0) {
		return
	}

	gtrace.TopicOnWriterSendingChanged(
		tracer,
		&ctx,
		info.Endpoint,
		info.Database,
		info.Topic,
		info.WriterName,
		messagesDelta,
		bytesDelta,
	)
}

// TraceMessagesWritten emits a writer ACK count event.
func TraceMessagesWritten(
	ctx context.Context,
	tracer *trace.Topic,
	info WriterInfo,
	messagesCount int64,
) {
	if info.WriterName == "" || tracer == nil || tracer.OnWriterMessagesWritten == nil || messagesCount == 0 {
		return
	}

	gtrace.TopicOnWriterMessagesWritten(
		tracer,
		&ctx,
		info.Endpoint,
		info.Database,
		info.Topic,
		info.WriterName,
		messagesCount,
	)
}

// TraceMetricDuration emits one writer duration sample. Zero is a valid
// elapsed duration; only negative values are rejected.
func TraceMetricDuration(
	ctx context.Context,
	tracer *trace.Topic,
	info WriterInfo,
	kind trace.TopicWriterMetricDurationKind,
	duration time.Duration,
) {
	if info.WriterName == "" || tracer == nil || tracer.OnWriterMetricDuration == nil || duration < 0 {
		return
	}

	gtrace.TopicOnWriterMetricDuration(
		tracer,
		&ctx,
		info.Endpoint,
		info.Database,
		info.Topic,
		info.WriterName,
		kind,
		duration,
	)
}

// TraceSessionError emits a writer session error after its retry decision has
// already been made. Classification intentionally matches reader sessions.
func TraceSessionError(
	ctx context.Context,
	tracer *trace.Topic,
	info WriterInfo,
	retryDecision string,
	err error,
) {
	if info.WriterName == "" || tracer == nil || tracer.OnWriterSessionError == nil || err == nil {
		return
	}

	classification := topic.ClassifySessionError(err)
	gtrace.TopicOnWriterSessionError(
		tracer,
		&ctx,
		info.Endpoint,
		info.Database,
		info.Topic,
		info.WriterName,
		retryDecision,
		classification.StatusCode,
		classification.ErrorType,
		err,
	)
}
