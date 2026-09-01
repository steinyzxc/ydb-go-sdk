package metrics

import "github.com/ydb-platform/ydb-go-sdk/v3/trace"

func topic(config Config) trace.Topic {
	config = config.
		WithSystem("topic").
		WithSystem("reader")

	return trace.Topic{
		OnReaderMessagesReceived:  topicReaderMessagesReceived(config),
		OnReaderMessagesDelivered: topicReaderMessagesDelivered(config),
	}
}

func topicReaderMessagesReceived(config Config) func(trace.TopicReaderMessagesReceivedInfo) {
	config = config.WithSystem("received")
	messages := config.CounterVec("messages", "endpoint", "database", "topic", "consumer")

	return func(info trace.TopicReaderMessagesReceivedInfo) {
		if config.Details()&trace.TopicReaderMessageEvents == 0 || info.MessagesCount <= 0 {
			return
		}
		counter := messages.With(map[string]string{
			"endpoint": info.Endpoint,
			"database": info.Database,
			"topic":    info.Topic,
			"consumer": info.Consumer,
		})
		for range info.MessagesCount {
			counter.Inc()
		}
	}
}

func topicReaderMessagesDelivered(config Config) func(trace.TopicReaderMessagesDeliveredInfo) {
	config = config.WithSystem("delivered")
	messages := config.CounterVec("messages", "endpoint", "database", "topic", "consumer")

	return func(info trace.TopicReaderMessagesDeliveredInfo) {
		if config.Details()&trace.TopicReaderMessageEvents == 0 || info.MessagesCount <= 0 {
			return
		}
		counter := messages.With(map[string]string{
			"endpoint": info.Endpoint,
			"database": info.Database,
			"topic":    info.Topic,
			"consumer": info.Consumer,
		})
		for range info.MessagesCount {
			counter.Inc()
		}
	}
}
