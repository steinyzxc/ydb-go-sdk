package metrics

import (
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicWriterMetricDescriptorsAndLegacyNames(t *testing.T) {
	config := newWriterMetricConfig(trace.DetailsAll)
	config.system = "custom"
	topic(config)
	legacyRegistry := newRecordingRegistry()
	topic(recordingConfig{registry: legacyRegistry, system: "custom", details: trace.DetailsAll})

	expectedCounters := []struct {
		name   string
		unit   string
		kind   string
		labels []string
	}{
		{topicWriterSendingMessagesName, topicWriterSendingMessagesUnit, "counter", topicWriterLabels},
		{topicWriterSendingBytesName, topicWriterSendingBytesUnit, "counter", topicWriterLabels},
		{topicWriterSessionErrorsName, topicWriterSessionErrorsUnit, "counter", topicWriterSessionErrorLabels},
		{topicWriterWrittenMessagesName, topicWriterWrittenMessagesUnit, "counter", topicWriterLabels},
	}
	for _, metric := range expectedCounters {
		require.Equal(t, metric.unit, config.capture.units[metric.name], metric.name)
		require.Equal(t, metric.kind, config.registry.kinds[metric.name], metric.name)
		require.Equal(t, metric.labels, config.registry.labelNames[metric.name], metric.name)
		legacyName := "custom.topic.writer." + metric.name[len("ydb.topic.writer."):]
		require.Equal(t, metric.kind, legacyRegistry.kinds[legacyName], metric.name)
		require.Equal(t, metric.labels, legacyRegistry.labelNames[legacyName], metric.name)
	}

	expectedTimers := []string{
		"custom.topic.writer.buffer.wait.duration",
		"custom.topic.writer.message.processing.duration",
		"custom.topic.writer.compression.duration",
		"custom.topic.writer.message.ack.duration",
	}
	require.ElementsMatch(t, expectedTimers, config.timers.names())
}

func TestTopicWriterMetricsHonorDetailsGroups(t *testing.T) {
	for _, test := range []struct {
		name      string
		details   trace.Details
		stream    bool
		lifecycle bool
	}{
		{name: "stream", details: trace.TopicWriterStreamEvents, stream: true},
		{name: "lifecycle", details: trace.TopicWriterStreamLifeCycleEvents, lifecycle: true},
		{name: "grpc", details: trace.TopicWriterStreamGrpcMessageEvents},
		{name: "unrelated", details: trace.TopicReaderEvents},
		{name: "all", details: trace.DetailsAll, stream: true, lifecycle: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			tracer := topic(recordingConfig{
				registry: newRecordingRegistry(),
				details:  test.details,
			})

			if test.stream {
				require.NotNil(t, tracer.OnWriterSendingChanged)
				require.NotNil(t, tracer.OnWriterMessagesWritten)
				require.NotNil(t, tracer.OnWriterMetricDuration)
			} else {
				require.Nil(t, tracer.OnWriterSendingChanged)
				require.Nil(t, tracer.OnWriterMessagesWritten)
				require.Nil(t, tracer.OnWriterMetricDuration)
			}
			if test.lifecycle {
				require.NotNil(t, tracer.OnWriterSessionError)
			} else {
				require.Nil(t, tracer.OnWriterSessionError)
			}
		})
	}
}

func TestTopicWriterCountersUseCumulativeSemantics(t *testing.T) {
	config := newWriterMetricConfig(trace.DetailsAll)
	tracer := topic(config)
	info := trace.TopicWriterSendingChangedInfo{
		Endpoint: "node:2135", Database: "/db", Topic: "/topic", WriterName: "writer",
		MessagesDelta: 7, BytesDelta: 100,
	}
	tracer.OnWriterSendingChanged(info)
	info.MessagesDelta = 3
	info.BytesDelta = 40
	tracer.OnWriterSendingChanged(info)
	info.MessagesDelta = -4
	info.BytesDelta = -20
	tracer.OnWriterSendingChanged(info)

	labels := map[string]string{
		"endpoint": "node:2135", "database": "/db", "topic": "/topic", "writer.name": "writer",
	}
	require.Equal(t, float64(10), config.registry.value(topicWriterSendingMessagesName, labels))
	require.Equal(t, float64(140), config.registry.value(topicWriterSendingBytesName, labels))
	require.Equal(t, []int64{7, 3}, config.capture.adds[topicWriterSendingMessagesName])
	require.Equal(t, []int64{100, 40}, config.capture.adds[topicWriterSendingBytesName])

	tracer.OnWriterMessagesWritten(trace.TopicWriterMessagesWrittenInfo{
		Endpoint: "node:2135", Database: "/db", Topic: "/topic", WriterName: "writer",
		MessagesCount: 5,
	})
	tracer.OnWriterMessagesWritten(trace.TopicWriterMessagesWrittenInfo{
		Endpoint: "node:2135", Database: "/db", Topic: "/topic", WriterName: "writer",
		MessagesCount: 2,
	})
	require.Equal(t, float64(7), config.registry.value(topicWriterWrittenMessagesName, labels))
	require.Equal(t, []int64{5, 2}, config.capture.adds[topicWriterWrittenMessagesName])
}

func TestTopicWriterBytesDoNotFallBackToInc(t *testing.T) {
	capture := &topicCounterCapture{}
	config := topicFallbackCounterConfig{
		recordingConfig: recordingConfig{registry: newRecordingRegistry(), details: trace.DetailsAll},
		capture:         capture,
	}
	tracer := topic(config)

	tracer.OnWriterSendingChanged(trace.TopicWriterSendingChangedInfo{
		Endpoint: "node", Database: "/db", Topic: "/topic", WriterName: "writer",
		MessagesDelta: 2, BytesDelta: 1024,
	})

	require.Equal(t, 2, capture.incs["topic.writer.sending.messages"])
	require.Zero(t, capture.incs["topic.writer.sending.bytes"])
}

func TestTopicWriterSessionErrors(t *testing.T) {
	info := trace.TopicWriterSessionErrorInfo{
		Endpoint: "node:2135", Database: "/db", Topic: "/topic", WriterName: "writer",
		RetryDecision: "retry", StatusCode: "UNAVAILABLE", ErrorType: "transport_error",
	}
	labels := map[string]string{
		"endpoint": "node:2135", "database": "/db", "topic": "/topic", "writer.name": "writer",
		"retry_decision": "retry", "status_code": "UNAVAILABLE", "error.type": "transport_error",
	}

	t.Run("descriptor adapter", func(t *testing.T) {
		config := newWriterMetricConfig(trace.DetailsAll)
		tracer := topic(config)

		tracer.OnWriterSessionError(info)
		topic(strictTopicConfig{recordingConfig: recordingConfig{details: trace.DetailsAll}, t: t}).
			OnWriterSessionError(info)

		require.Equal(t, float64(1), config.registry.value(topicWriterSessionErrorsName, labels))
		require.Equal(t, []int64{1}, config.capture.adds[topicWriterSessionErrorsName])
	})

	t.Run("legacy Inc-only adapter", func(t *testing.T) {
		capture := &topicCounterCapture{}
		registry := newRecordingRegistry()
		tracer := topic(topicFallbackCounterConfig{
			recordingConfig: recordingConfig{registry: registry, details: trace.DetailsAll},
			capture:         capture,
		})

		tracer.OnWriterSessionError(info)

		require.Equal(t, map[string]int{"topic.writer.session.errors": 1}, capture.incs)
		require.Empty(t, capture.adds)
		require.Equal(t, float64(1), registry.value("topic.writer.session.errors", labels))
	})
}

func TestTopicWriterMetricCallbacksSuppressEmptyWriterName(t *testing.T) {
	config := newWriterMetricConfig(trace.DetailsAll)
	tracer := topic(config)

	tracer.OnWriterSendingChanged(trace.TopicWriterSendingChangedInfo{MessagesDelta: 1, BytesDelta: 1})
	tracer.OnWriterMessagesWritten(trace.TopicWriterMessagesWrittenInfo{MessagesCount: 1})
	tracer.OnWriterSessionError(trace.TopicWriterSessionErrorInfo{})
	tracer.OnWriterMetricDuration(trace.TopicWriterMetricDurationInfo{
		Kind:     trace.TopicWriterMetricDurationBufferWait,
		Duration: time.Second,
	})

	require.Empty(t, config.registry.values)
	require.Empty(t, config.capture.adds)
	require.Empty(t, config.timers.samples())
}

func TestTopicWriterDurationsUseExpectedNamesAndPassThroughValues(t *testing.T) {
	config := newWriterMetricConfig(trace.DetailsAll)
	config.system = "custom"
	tracer := topic(config)
	info := trace.TopicWriterMetricDurationInfo{
		Endpoint: "node", Database: "/db", Topic: "/topic", WriterName: "writer",
		Duration: 1500 * time.Millisecond,
	}
	for _, test := range []struct {
		kind trace.TopicWriterMetricDurationKind
		path string
	}{
		{trace.TopicWriterMetricDurationBufferWait, "custom.topic.writer.buffer.wait.duration"},
		{trace.TopicWriterMetricDurationProcessing, "custom.topic.writer.message.processing.duration"},
		{trace.TopicWriterMetricDurationCompression, "custom.topic.writer.compression.duration"},
		{trace.TopicWriterMetricDurationAck, "custom.topic.writer.message.ack.duration"},
	} {
		info.Kind = test.kind
		tracer.OnWriterMetricDuration(info)
		require.Equal(t, []time.Duration{1500 * time.Millisecond}, config.timers.values(
			test.path,
			writerLabels("node", "/db", "/topic", "writer"),
		))
	}

	info.Duration = 0
	info.Kind = trace.TopicWriterMetricDurationAck
	tracer.OnWriterMetricDuration(info)
	require.Equal(t, []time.Duration{1500 * time.Millisecond, 0}, config.timers.values(
		"custom.topic.writer.message.ack.duration",
		writerLabels("node", "/db", "/topic", "writer"),
	))

	info.Duration = -time.Nanosecond
	tracer.OnWriterMetricDuration(info)
	require.Len(t, config.timers.values(
		"custom.topic.writer.message.ack.duration",
		writerLabels("node", "/db", "/topic", "writer"),
	), 2)
}

type writerMetricConfig struct {
	topicAddCounterConfig

	timers *writerTimerRegistry
}

func newWriterMetricConfig(details trace.Details) writerMetricConfig {
	return writerMetricConfig{
		topicAddCounterConfig: topicAddCounterConfig{
			recordingConfig: recordingConfig{
				registry: newRecordingRegistry(),
				details:  details,
			},
			capture: &topicCounterCapture{},
		},
		timers: newWriterTimerRegistry(),
	}
}

func (c writerMetricConfig) WithSystem(system string) Config {
	scoped := c.topicAddCounterConfig.recordingConfig.WithSystem(system).(recordingConfig)

	return writerMetricConfig{
		topicAddCounterConfig: topicAddCounterConfig{
			recordingConfig: scoped,
			capture:         c.capture,
		},
		timers: c.timers,
	}
}

func (c writerMetricConfig) TimerVec(name string, labelNames ...string) TimerVec {
	path := c.path(name)
	c.timers.register(path, labelNames)

	return writerTimerVec{registry: c.timers, path: path}
}

type writerTimerRegistry struct {
	mu      sync.Mutex
	entries map[string]*writerTimerEntry
}

type writerTimerEntry struct {
	labelNames []string
	samples    map[string][]time.Duration
}

func newWriterTimerRegistry() *writerTimerRegistry {
	return &writerTimerRegistry{entries: make(map[string]*writerTimerEntry)}
}

func (r *writerTimerRegistry) register(path string, labelNames []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.entries[path]; !ok {
		r.entries[path] = &writerTimerEntry{
			labelNames: append([]string(nil), labelNames...),
			samples:    make(map[string][]time.Duration),
		}
	}
}

func (r *writerTimerRegistry) names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	names := make([]string, 0, len(r.entries))
	for name := range r.entries {
		names = append(names, name)
	}
	sort.Strings(names)

	return names
}

func (r *writerTimerRegistry) samples() map[string][]time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	res := make(map[string][]time.Duration, len(r.entries))
	for path, entry := range r.entries {
		for labels, values := range entry.samples {
			res[path+"|"+labels] = append([]time.Duration(nil), values...)
		}
	}

	return res
}

func (r *writerTimerRegistry) values(path string, labels map[string]string) []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.entries[path]
	if entry == nil {
		return nil
	}
	key := writerTimerLabelsKey(entry.labelNames, labels)

	return append([]time.Duration(nil), entry.samples[key]...)
}

type writerTimerVec struct {
	registry *writerTimerRegistry
	path     string
}

func (v writerTimerVec) With(labels map[string]string) Timer {
	return writerTimer{registry: v.registry, path: v.path, labels: labels}
}

type writerTimer struct {
	registry *writerTimerRegistry
	path     string
	labels   map[string]string
}

func (t writerTimer) Record(value time.Duration) {
	t.registry.mu.Lock()
	defer t.registry.mu.Unlock()
	entry := t.registry.entries[t.path]
	key := writerTimerLabelsKey(entry.labelNames, t.labels)
	entry.samples[key] = append(entry.samples[key], value)
}

func writerTimerLabelsKey(labelNames []string, labels map[string]string) string {
	parts := make([]string, 0, len(labelNames))
	for _, name := range labelNames {
		parts = append(parts, name+"="+labels[name])
	}

	return strings.Join(parts, ",")
}
