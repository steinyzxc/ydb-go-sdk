package topicreadercommon

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// TraceReaderSessionError emits one session error event for a caller
// that has already made the retry or stop decision.
func TraceReaderSessionError(
	ctx context.Context,
	tracer *trace.Topic,
	readerInfo ReaderInfo,
	retryDecision string,
	err error,
) {
	if tracer == nil || tracer.OnReaderSessionError == nil || err == nil {
		return
	}

	classification := topic.ClassifySessionError(err)
	gtrace.TopicOnReaderSessionError(
		tracer,
		&ctx,
		readerInfo.Endpoint,
		readerInfo.Database,
		readerInfo.Consumer,
		readerInfo.ReaderName,
		readerInfo.Listener,
		retryDecision,
		classification.StatusCode,
		classification.ErrorType,
		err,
	)
}
