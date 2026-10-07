package metrics

import "github.com/ydb-platform/ydb-go-sdk/v3/trace"

const (
	topicWriterSendingMessagesName = "ydb.topic.writer.sending.messages"
	topicWriterSendingMessagesUnit = "{message}"
	topicWriterSendingBytesName    = "ydb.topic.writer.sending.bytes"
	topicWriterSendingBytesUnit    = "By"
	topicWriterSessionErrorsName   = "ydb.topic.writer.session.errors"
	topicWriterSessionErrorsUnit   = "{error}"
	topicWriterWrittenMessagesName = "ydb.topic.writer.written.messages"
	topicWriterWrittenMessagesUnit = "{message}"
)

var (
	topicWriterLabels             = []string{"endpoint", "database", "topic", "writer.name"}
	topicWriterSessionErrorLabels = []string{
		"endpoint", "database", "topic", "writer.name",
		"retry_decision", "status_code", "error.type",
	}
)

func setupTopicWriterMetrics(t *trace.Topic, config Config) {
	setupTopicWriterSending(t, config)
	setupTopicWriterMessagesWritten(t, config)
	setupTopicWriterSessionErrors(t, config)
	setupTopicWriterDurations(t, config)
}

func setupTopicWriterSending(t *trace.Topic, config Config) {
	sendingMessages := topicCounter(
		config,
		"sending",
		"messages",
		topicWriterSendingMessagesName,
		topicWriterSendingMessagesUnit,
		topicWriterLabels,
	)
	sendingBytes := topicCounter(
		config,
		"sending",
		"bytes",
		topicWriterSendingBytesName,
		topicWriterSendingBytesUnit,
		topicWriterLabels,
	)
	if config.Details()&trace.TopicWriterStreamEvents == 0 {
		return
	}

	// Both sending counters share the same admission event. Add keeps batched
	// contributions from writers with matching labels cumulative.
	t.OnWriterSendingChanged = func(info trace.TopicWriterSendingChangedInfo) {
		if info.WriterName == "" {
			return
		}

		labels := writerLabels(info.Endpoint, info.Database, info.Topic, info.WriterName)
		addCounter(sendingMessages.With(labels), info.MessagesDelta)
		addCounterWithAdd(sendingBytes.With(labels), info.BytesDelta)
	}
}

func setupTopicWriterMessagesWritten(t *trace.Topic, config Config) {
	writtenMessages := topicCounter(
		config,
		"written",
		"messages",
		topicWriterWrittenMessagesName,
		topicWriterWrittenMessagesUnit,
		topicWriterLabels,
	)
	if config.Details()&trace.TopicWriterStreamEvents == 0 {
		return
	}

	t.OnWriterMessagesWritten = func(info trace.TopicWriterMessagesWrittenInfo) {
		if info.WriterName == "" {
			return
		}

		// ACK status is intentionally not a metric attribute. Every ACK status
		// contributes to the same cumulative message counter exactly once.
		addCounter(writtenMessages.With(writerLabels(
			info.Endpoint,
			info.Database,
			info.Topic,
			info.WriterName,
		)), info.MessagesCount)
	}
}

func setupTopicWriterSessionErrors(t *trace.Topic, config Config) {
	sessionErrors := topicCounter(
		config,
		"session",
		"errors",
		topicWriterSessionErrorsName,
		topicWriterSessionErrorsUnit,
		topicWriterSessionErrorLabels,
	)
	if config.Details()&trace.TopicWriterStreamLifeCycleEvents == 0 {
		return
	}

	t.OnWriterSessionError = func(info trace.TopicWriterSessionErrorInfo) {
		if info.WriterName == "" {
			return
		}

		labels := writerLabels(info.Endpoint, info.Database, info.Topic, info.WriterName)
		labels["retry_decision"] = info.RetryDecision
		labels["status_code"] = info.StatusCode
		labels["error.type"] = info.ErrorType
		addCounter(sessionErrors.With(labels), 1)
	}
}

func setupTopicWriterDurations(t *trace.Topic, config Config) {
	bufferWait := config.TimerVec("buffer.wait.duration", topicWriterLabels...)
	processing := config.TimerVec("message.processing.duration", topicWriterLabels...)
	compression := config.TimerVec("compression.duration", topicWriterLabels...)
	ack := config.TimerVec("message.ack.duration", topicWriterLabels...)
	if config.Details()&trace.TopicWriterStreamEvents == 0 {
		return
	}

	t.OnWriterMetricDuration = func(info trace.TopicWriterMetricDurationInfo) {
		if info.WriterName == "" || info.Duration < 0 {
			return
		}

		var timer TimerVec
		switch info.Kind {
		case trace.TopicWriterMetricDurationBufferWait:
			timer = bufferWait
		case trace.TopicWriterMetricDurationProcessing:
			timer = processing
		case trace.TopicWriterMetricDurationCompression:
			timer = compression
		case trace.TopicWriterMetricDurationAck:
			timer = ack
		default:
			return
		}

		timer.With(writerLabels(
			info.Endpoint,
			info.Database,
			info.Topic,
			info.WriterName,
		)).Record(info.Duration)
	}
}

func writerLabels(endpoint, database, topic, writerName string) map[string]string {
	return map[string]string{
		"endpoint":    endpoint,
		"database":    database,
		"topic":       topic,
		"writer.name": writerName,
	}
}
