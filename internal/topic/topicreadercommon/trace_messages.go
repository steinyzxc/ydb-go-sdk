package topicreadercommon

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// TraceMessagesReceived emits a reader message reception trace event.
func TraceMessagesReceived(
	ctx context.Context,
	tracer *trace.Topic,
	readerInfo ReaderInfo,
	topic string,
	messagesCount int,
) {
	gtrace.TopicOnReaderMessagesReceived(
		tracer,
		&ctx,
		readerInfo.Endpoint,
		readerInfo.Database,
		topic,
		readerInfo.Consumer,
		messagesCount,
	)
}

// TraceMessagesDelivered emits a reader message delivery trace event.
func TraceMessagesDelivered(
	ctx context.Context,
	tracer *trace.Topic,
	readerInfo ReaderInfo,
	topic string,
	messagesCount int,
) {
	gtrace.TopicOnReaderMessagesDelivered(
		tracer,
		&ctx,
		readerInfo.Endpoint,
		readerInfo.Database,
		topic,
		readerInfo.Consumer,
		messagesCount,
	)
}
