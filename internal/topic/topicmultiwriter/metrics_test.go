package topicmultiwriter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter/partitionchooser"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter/stubs"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestMultiWriterMetricsAdmissionAndAck(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	client := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	factory := newMetricsTestWritersFactory()
	tracer, sending, written := newMetricsTrace()
	multiWriter := newTestMultiWriterWithCustomWritersFactory(
		t,
		client.Describe,
		factory,
		topicwriterinternal.WithTrace(tracer),
		topicwriterinternal.WithWriterName("multi-metrics-test"),
		topicwriterinternal.WithClock(xtest.FastClock(t)),
	)

	require.NoError(t, multiWriter.WaitInit(ctx))
	require.NotNil(t, multiWriter.orchestrator.writerCfg.Tracer)
	require.True(t, multiWriter.orchestrator.metricsEnabled)
	require.False(t, multiWriter.orchestrator.metricsDurationEnabled)
	require.NoError(t, multiWriter.Write(ctx, []topicwriterinternal.PublicMessage{
		{SeqNo: 1, Key: "partition-key-1", Data: bytes.NewReader([]byte("hello"))},
	}))

	admitted := receiveSendingMetric(t, sending)
	require.Equal(t, int64(1), admitted.MessagesDelta)
	require.Equal(t, int64(5), admitted.BytesDelta)
	require.Equal(t, "test/topic", admitted.Topic)
	require.Equal(t, "multi-metrics-test", admitted.WriterName)

	child := factory.nextWriter(t)
	multiWriter.orchestrator.mu.WithLock(func() {
		message := multiWriter.orchestrator.buf.inFlightMessages.Front()
		require.NotNil(t, message)
		require.Nil(t, message.Value.Timing)
	})
	child.Ack(1)

	writtenMetric := receiveWrittenMetric(t, written)
	require.Equal(t, int64(1), writtenMetric.MessagesCount)
	require.Equal(t, "multi-metrics-test", writtenMetric.WriterName)
	select {
	case extra := <-sending:
		t.Fatalf("unexpected sending metric after ACK: %+v", extra)
	case <-time.After(50 * time.Millisecond):
	}

	require.NoError(t, multiWriter.Close(ctx))
}

func TestMultiWriterMetricsCloseLeavesCountersAndIgnoresLateAck(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	client := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	factory := newMetricsTestWritersFactory()
	tracer, sending, written := newMetricsTrace()
	multiWriter := newTestMultiWriterWithCustomWritersFactory(
		t,
		client.Describe,
		factory,
		topicwriterinternal.WithTrace(tracer),
		topicwriterinternal.WithWriterName("multi-metrics-close-test"),
		topicwriterinternal.WithClock(xtest.FastClock(t)),
	)

	require.NoError(t, multiWriter.WaitInit(ctx))
	require.NoError(t, multiWriter.Write(ctx, []topicwriterinternal.PublicMessage{
		{
			SeqNo: 1,
			Key:   "partition-key-1",
			Data:  bytes.NewReader([]byte("pending")),
		},
	}))
	receiveSendingMetric(t, sending)
	child := factory.nextWriter(t)

	closeCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, multiWriter.Close(closeCtx), context.DeadlineExceeded)

	child.Ack(1)
	multiWriter.orchestrator.mu.WithLock(func() {
		event := multiWriter.orchestrator.onAckReceivedNeedLock(child.partitionID, 1)
		require.True(t, event.IsEmpty())
		message := multiWriter.orchestrator.buf.inFlightMessages.Front()
		require.NotNil(t, message)
		require.False(t, message.Value.ackReceived)
	})

	select {
	case extra := <-sending:
		t.Fatalf("unexpected sending metric after close: %+v", extra)
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case extra := <-written:
		t.Fatalf("unexpected written metric after late ACK: %+v", extra)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestMultiWriterMetricsRealWriterReconnectCountsOnceAndEmitsDurations(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(xtest.Context(t), 5*time.Second)
	defer cancel()
	clock := newMetricsAdjustableClock()
	client := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	streams := make(chan *metricsRawWriterStream, 8)
	writes := make(chan metricsRawWriterWrite, 8)
	tracer, sending, written := newMetricsTrace()
	durations := make(chan trace.TopicWriterMetricDurationInfo, 8)
	tracer.OnWriterMetricDuration = func(info trace.TopicWriterMetricDurationInfo) {
		durations <- info
	}
	connect := func(context.Context, *trace.Topic) (topicwriterinternal.RawTopicWriterStream, error) {
		stream := newMetricsRawWriterStream(writes)
		streams <- stream

		return stream, nil
	}
	multiWriter := newMetricsRealMultiWriter(
		t, client.Describe,
		topicwriterinternal.WithTrace(tracer),
		topicwriterinternal.WithWriterName("multi-reconnect-metrics-test"),
		topicwriterinternal.WithClock(clock),
		topicwriterinternal.WithCodec(rawtopiccommon.CodecRaw),
		topicwriterinternal.WithConnectFunc(connect),
		topicwriterinternal.WithAutoSetSeqNo(false),
	)

	require.NoError(t, multiWriter.WaitInit(ctx))
	for len(streams) > 0 {
		<-streams
	}
	data := &multiWriterMetricClockReader{
		advance: func() { clock.Advance(5 * time.Second) },
		reader:  bytes.NewReader([]byte("hello")),
	}
	require.NoError(t, multiWriter.Write(ctx, []topicwriterinternal.PublicMessage{
		{SeqNo: 1, Key: "partition-key-1", Data: data},
	}))
	admitted := receiveSendingMetric(t, sending)
	require.Equal(t, int64(1), admitted.MessagesDelta)
	require.Equal(t, int64(5), admitted.BytesDelta)
	require.Equal(t, "multi-reconnect-metrics-test", admitted.WriterName)
	processing := receiveDuration(t, durations, trace.TopicWriterMetricDurationProcessing)
	require.GreaterOrEqual(t, processing.Duration, 5*time.Second)
	require.GreaterOrEqual(
		t,
		receiveDuration(t, durations, trace.TopicWriterMetricDurationBufferWait).Duration,
		time.Duration(0),
	)

	firstWrite := receiveMetricsRawWriterWrite(t, writes)
	firstStream := firstWrite.stream
	seqNo := firstWrite.request.Messages[0].SeqNo
	clock.Advance(24 * time.Hour)
	require.NoError(t, firstStream.CloseSend())

	secondStream := receiveMetricsRawWriterStream(t, streams)
	secondWrite := receiveMetricsRawWriterWrite(t, writes)
	require.Same(t, secondStream, secondWrite.stream)
	require.Equal(t, seqNo, secondWrite.request.Messages[0].SeqNo)
	require.NoError(t, secondStream.ack(seqNo))

	writtenMetric := receiveWrittenMetric(t, written)
	require.Equal(t, int64(1), writtenMetric.MessagesCount)
	require.Equal(t, "multi-reconnect-metrics-test", writtenMetric.WriterName)
	ack := receiveDuration(t, durations, trace.TopicWriterMetricDurationAck)
	require.GreaterOrEqual(t, ack.Duration, 24*time.Hour)
	require.NoError(t, multiWriter.Close(ctx))

	select {
	case extra := <-sending:
		t.Fatalf("unexpected extra sending metric after reconnect: %+v", extra)
	default:
	}
	select {
	case extra := <-written:
		t.Fatalf("unexpected extra written metric after reconnect: %+v", extra)
	default:
	}
	select {
	case extra := <-durations:
		t.Fatalf("unexpected extra duration metric after reconnect: %+v", extra)
	default:
	}
}

func TestMultiWriterProcessingDurationFinishesAtAdmissionForRawCodec(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	clock := clockwork.NewFakeClockAt(time.Unix(150, 0))
	client := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	factory := newMetricsTestWritersFactory()
	durations := make(chan trace.TopicWriterMetricDurationInfo, 8)
	tracer := &trace.Topic{
		OnWriterMetricDuration: func(info trace.TopicWriterMetricDurationInfo) {
			durations <- info
		},
	}
	multiWriter := newTestMultiWriterWithCustomWritersFactory(
		t,
		client.Describe,
		factory,
		topicwriterinternal.WithTrace(tracer),
		topicwriterinternal.WithWriterName("multi-raw-processing-test"),
		topicwriterinternal.WithClock(clock),
		topicwriterinternal.WithCodec(rawtopiccommon.CodecRaw),
	)

	require.NoError(t, multiWriter.WaitInit(ctx))
	data := &multiWriterMetricClockReader{
		advance: func() { clock.Advance(5 * time.Second) },
		reader:  bytes.NewReader([]byte("hello")),
	}
	require.NoError(t, multiWriter.Write(ctx, []topicwriterinternal.PublicMessage{
		{SeqNo: 1, Key: "partition-key-1", Data: data},
	}))
	require.Equal(
		t,
		5*time.Second,
		receiveDuration(t, durations, trace.TopicWriterMetricDurationProcessing).Duration,
	)
	require.Zero(t, receiveDuration(t, durations, trace.TopicWriterMetricDurationBufferWait).Duration)

	factory.nextWriter(t).Ack(1)
	require.NoError(t, multiWriter.Close(ctx))
}

func TestMultiWriterMetricsRealChildProcessingAndCompression(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(xtest.Context(t), 5*time.Second)
	defer cancel()
	clock := clockwork.NewFakeClockAt(time.Now())
	client := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	streams := make(chan *metricsRawWriterStream, 8)
	writes := make(chan metricsRawWriterWrite, 8)
	tracer, sending, written := newMetricsTrace()
	durations := make(chan trace.TopicWriterMetricDurationInfo, 8)
	var advanceOnCompression sync.Once
	tracer.OnWriterMetricDuration = func(info trace.TopicWriterMetricDurationInfo) {
		if info.Kind == trace.TopicWriterMetricDurationCompression {
			advanceOnCompression.Do(func() { clock.Advance(2 * time.Second) })
		}
		durations <- info
	}
	connect := func(context.Context, *trace.Topic) (topicwriterinternal.RawTopicWriterStream, error) {
		stream := newMetricsRawWriterStream(writes)
		streams <- stream

		return stream, nil
	}
	multiWriter := newMetricsRealMultiWriter(
		t, client.Describe,
		topicwriterinternal.WithTrace(tracer),
		topicwriterinternal.WithWriterName("multi-gzip-metrics-test"),
		topicwriterinternal.WithClock(clock),
		topicwriterinternal.WithCodec(rawtopiccommon.CodecGzip),
		topicwriterinternal.WithConnectFunc(connect),
		topicwriterinternal.WithAutoSetSeqNo(false),
	)

	require.NoError(t, multiWriter.WaitInit(ctx))
	for len(streams) > 0 {
		<-streams
	}
	data := &multiWriterMetricClockReader{
		advance: func() { clock.Advance(5 * time.Second) },
		reader:  bytes.NewReader([]byte("hello")),
	}
	require.NoError(t, multiWriter.Write(ctx, []topicwriterinternal.PublicMessage{
		{SeqNo: 1, Key: "partition-key-1", Data: data},
	}))
	admitted := receiveSendingMetric(t, sending)
	require.Equal(t, int64(1), admitted.MessagesDelta)
	require.Equal(t, int64(5), admitted.BytesDelta)
	require.Equal(t, "multi-gzip-metrics-test", admitted.WriterName)
	durationEvents := receiveMetricDurations(
		t,
		durations,
		trace.TopicWriterMetricDurationBufferWait,
		trace.TopicWriterMetricDurationCompression,
		trace.TopicWriterMetricDurationProcessing,
	)
	require.Equal(t, 7*time.Second, durationEvents[trace.TopicWriterMetricDurationProcessing].Duration)

	write := receiveMetricsRawWriterWrite(t, writes)
	require.Equal(t, rawtopiccommon.CodecGzip, write.request.Codec)
	clock.Advance(3 * time.Second)
	require.NoError(t, write.stream.ack(write.request.Messages[0].SeqNo))
	writtenMetric := receiveWrittenMetric(t, written)
	require.Equal(t, int64(1), writtenMetric.MessagesCount)
	require.Equal(t, "multi-gzip-metrics-test", writtenMetric.WriterName)
	require.GreaterOrEqual(
		t,
		receiveDuration(t, durations, trace.TopicWriterMetricDurationAck).Duration,
		3*time.Second,
	)
	require.NoError(t, multiWriter.Close(ctx))

	select {
	case extra := <-sending:
		t.Fatalf("unexpected extra sending metric: %+v", extra)
	default:
	}
	select {
	case extra := <-written:
		t.Fatalf("unexpected extra written metric: %+v", extra)
	default:
	}
	select {
	case extra := <-durations:
		t.Fatalf("unexpected extra duration metric: %+v", extra)
	default:
	}
}

func TestMultiWriterMetricsEmptyNameDisablesTracking(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	client := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	factory := newMetricsTestWritersFactory()
	sending := make(chan trace.TopicWriterSendingChangedInfo, 1)
	written := make(chan trace.TopicWriterMessagesWrittenInfo, 1)
	durations := make(chan trace.TopicWriterMetricDurationInfo, 1)
	tracer := &trace.Topic{
		OnWriterSendingChanged:  func(info trace.TopicWriterSendingChangedInfo) { sending <- info },
		OnWriterMessagesWritten: func(info trace.TopicWriterMessagesWrittenInfo) { written <- info },
		OnWriterMetricDuration:  func(info trace.TopicWriterMetricDurationInfo) { durations <- info },
	}
	multiWriter := newTestMultiWriterWithCustomWritersFactory(
		t,
		client.Describe,
		factory,
		topicwriterinternal.WithTrace(tracer),
	)

	require.NoError(t, multiWriter.WaitInit(ctx))
	require.False(t, multiWriter.orchestrator.metricsEnabled)
	require.False(t, multiWriter.orchestrator.metricsDurationEnabled)
	require.NoError(t, multiWriter.Write(ctx, []topicwriterinternal.PublicMessage{
		{SeqNo: 1, Key: "empty-name", Data: bytes.NewReader([]byte("hello"))},
	}))
	child := factory.nextWriter(t)
	multiWriter.orchestrator.mu.WithLock(func() {
		message := multiWriter.orchestrator.buf.inFlightMessages.Front()
		require.NotNil(t, message)
		require.Nil(t, message.Value.Timing)
	})
	child.Ack(1)
	require.Eventually(t, func() bool {
		var empty bool
		multiWriter.orchestrator.mu.WithLock(func() {
			empty = multiWriter.orchestrator.buf.inFlightMessages.Len() == 0
		})

		return empty
	}, time.Second, time.Millisecond)
	require.NoError(t, multiWriter.Close(ctx))

	select {
	case info := <-sending:
		t.Fatalf("unexpected sending callback: %+v", info)
	default:
	}
	select {
	case info := <-written:
		t.Fatalf("unexpected written callback: %+v", info)
	default:
	}
	select {
	case info := <-durations:
		t.Fatalf("unexpected duration callback: %+v", info)
	default:
	}
}

func TestMultiWriterMetricsChildIdentityPreservesExplicitName(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	client := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	factory := newMetricsTestWritersFactory()
	tracer := &trace.Topic{OnWriterSendingChanged: func(trace.TopicWriterSendingChangedInfo) {}}
	multiWriter := newTestMultiWriterWithCustomWritersFactory(
		t,
		client.Describe,
		factory,
		topicwriterinternal.WithTrace(tracer),
		topicwriterinternal.WithWriterName("explicit-multi-name"),
		topicwriterinternal.WithClock(xtest.FastClock(t)),
	)

	require.NoError(t, multiWriter.WaitInit(ctx))
	infos := factory.drainWriterInfos()
	require.NotEmpty(t, infos)
	for _, info := range infos {
		require.Equal(t, "explicit-multi-name", info.WriterName)
		require.Equal(t, "test/topic", info.Topic)
	}

	require.NoError(t, multiWriter.Close(ctx))
}

type metricsTestWriter struct {
	ack         func(seqNo int64)
	ready       chan *metricsTestWriter
	seqNo       int64
	partitionID int64
}

type multiWriterMetricClockReader struct {
	advance  func()
	reader   *bytes.Reader
	advanced bool
}

type metricsAdjustableClock struct {
	clockwork.Clock

	offset atomic.Int64
}

func newMetricsAdjustableClock() *metricsAdjustableClock {
	return &metricsAdjustableClock{Clock: clockwork.NewRealClock()}
}

func (c *metricsAdjustableClock) Now() time.Time {
	return c.Clock.Now().Add(time.Duration(c.offset.Load()))
}

func (c *metricsAdjustableClock) Since(t time.Time) time.Duration {
	return c.Now().Sub(t)
}

func (c *metricsAdjustableClock) Until(t time.Time) time.Duration {
	return t.Sub(c.Now())
}

func (c *metricsAdjustableClock) Advance(duration time.Duration) {
	c.offset.Add(int64(duration))
}

func (r *multiWriterMetricClockReader) Read(p []byte) (int, error) {
	if !r.advanced {
		r.advance()
		r.advanced = true
	}

	return r.reader.Read(p)
}

func (w *metricsTestWriter) WriteInternal(
	_ context.Context,
	messages []topicwritercommon.MessageWithDataContent,
) error {
	if len(messages) > 0 {
		w.seqNo = messages[0].SeqNo
		w.partitionID = messages[0].PartitionID
	}
	w.ready <- w

	return nil
}

func (w *metricsTestWriter) WaitInitInfo(context.Context) (topicwriterinternal.InitialInfo, error) {
	return topicwriterinternal.InitialInfo{}, nil
}

func (w *metricsTestWriter) Close(context.Context) error {
	return nil
}

func (w *metricsTestWriter) Ack(seqNo int64) {
	if w.ack != nil {
		w.ack(seqNo)
	}
}

type metricsTestWritersFactory struct {
	writers     chan *metricsTestWriter
	writerInfos chan topicwritercommon.WriterInfo
}

func newMetricsTestWritersFactory() *metricsTestWritersFactory {
	return &metricsTestWritersFactory{
		writers:     make(chan *metricsTestWriter, 8),
		writerInfos: make(chan topicwritercommon.WriterInfo, 64),
	}
}

func (f *metricsTestWritersFactory) Create(
	cfg topicwriterinternal.WriterReconnectorConfig,
) (writer, error) {
	f.writerInfos <- cfg.WriterInfo

	return &metricsTestWriter{
		ack:   cfg.OnAckReceivedCallback,
		ready: f.writers,
	}, nil
}

type metricsRawWriterStream struct {
	responses   chan rawtopicwriter.ServerMessage
	writes      chan<- metricsRawWriterWrite
	closed      chan struct{}
	closeOnce   sync.Once
	partitionID int64
}

type metricsRawWriterWrite struct {
	stream  *metricsRawWriterStream
	request *rawtopicwriter.WriteRequest
}

func newMetricsRawWriterStream(writes chan<- metricsRawWriterWrite) *metricsRawWriterStream {
	return &metricsRawWriterStream{
		responses: make(chan rawtopicwriter.ServerMessage, 8),
		writes:    writes,
		closed:    make(chan struct{}),
	}
}

func (s *metricsRawWriterStream) Send(message rawtopicwriter.ClientMessage) error {
	switch message := message.(type) {
	case *rawtopicwriter.InitRequest:
		s.partitionID = message.Partitioning.PartitionID
		result := &rawtopicwriter.InitResult{
			SessionID:   "metrics-session",
			PartitionID: s.partitionID,
			SupportedCodecs: rawtopiccommon.SupportedCodecs{
				rawtopiccommon.CodecRaw,
				rawtopiccommon.CodecGzip,
			},
		}
		result.SetStatus(rawydb.StatusSuccess)
		select {
		case s.responses <- result:
			return nil
		case <-s.closed:
			return io.EOF
		}
	case *rawtopicwriter.WriteRequest:
		select {
		case s.writes <- metricsRawWriterWrite{stream: s, request: message}:
			return nil
		case <-s.closed:
			return io.EOF
		}
	case *rawtopicwriter.UpdateTokenRequest:
		return nil
	default:
		return fmt.Errorf("unexpected raw writer request: %T", message)
	}
}

func (s *metricsRawWriterStream) Recv() (rawtopicwriter.ServerMessage, error) {
	select {
	case response := <-s.responses:
		return response, nil
	case <-s.closed:
		return nil, io.EOF
	}
}

func (s *metricsRawWriterStream) CloseSend() error {
	s.closeOnce.Do(func() { close(s.closed) })

	return nil
}

func (*metricsRawWriterStream) Endpoint() endpoint.Endpoint {
	return nil
}

func (s *metricsRawWriterStream) ack(seqNo int64) error {
	response := &rawtopicwriter.WriteResult{
		Acks: []rawtopicwriter.WriteAck{{
			SeqNo: seqNo,
			MessageWriteStatus: rawtopicwriter.MessageWriteStatus{
				Type:          rawtopicwriter.WriteStatusTypeWritten,
				WrittenOffset: seqNo - 1,
			},
		}},
		PartitionID: s.partitionID,
	}
	response.SetStatus(rawydb.StatusSuccess)
	select {
	case s.responses <- response:
		return nil
	case <-s.closed:
		return io.EOF
	}
}

func receiveMetricsRawWriterStream(t *testing.T, streams <-chan *metricsRawWriterStream) *metricsRawWriterStream {
	t.Helper()

	select {
	case stream := <-streams:
		return stream
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a real writer stream")

		return nil
	}
}

func receiveMetricsRawWriterWrite(t *testing.T, writes <-chan metricsRawWriterWrite) metricsRawWriterWrite {
	t.Helper()

	select {
	case write := <-writes:
		return write
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a real writer write request")

		return metricsRawWriterWrite{}
	}
}

func newMetricsRealMultiWriter(
	t testing.TB,
	describer TopicDescriber,
	opts ...topicwriterinternal.PublicWriterOption,
) *MultiWriter {
	t.Helper()

	cfg := MultiWriterConfig{}
	withWritersFactory(newBaseWritersFactory())(&cfg)
	WithProducerIDPrefix("test-producer")(&cfg)
	WithWriterPartitionByKey(partitionchooser.NewBoundPartitionChooser())(&cfg)

	writerCfg := topicwriterinternal.NewWriterReconnectorConfig(opts...)
	topicwriterinternal.WithTopic("test/topic")(&writerCfg)
	topicwriterinternal.WithMaxQueueLen(100)(&writerCfg)
	topicwriterinternal.WithAutosetCreatedTime(false)(&writerCfg)

	multiWriter, err := NewMultiWriter(describer, &writerCfg, &cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = multiWriter.Close(ctx)
	})

	return multiWriter
}

func (f *metricsTestWritersFactory) nextWriter(t *testing.T) *metricsTestWriter {
	t.Helper()

	select {
	case w := <-f.writers:
		return w
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for a partition writer")

		return nil
	}
}

func (f *metricsTestWritersFactory) drainWriterInfos() []topicwritercommon.WriterInfo {
	var infos []topicwritercommon.WriterInfo
	for {
		select {
		case info := <-f.writerInfos:
			infos = append(infos, info)
		default:
			return infos
		}
	}
}

func newMetricsTrace() (
	*trace.Topic,
	chan trace.TopicWriterSendingChangedInfo,
	chan trace.TopicWriterMessagesWrittenInfo,
) {
	sending := make(chan trace.TopicWriterSendingChangedInfo, 8)
	written := make(chan trace.TopicWriterMessagesWrittenInfo, 8)

	return &trace.Topic{
		OnWriterSendingChanged: func(info trace.TopicWriterSendingChangedInfo) {
			sending <- info
		},
		OnWriterMessagesWritten: func(info trace.TopicWriterMessagesWrittenInfo) {
			written <- info
		},
	}, sending, written
}

func receiveSendingMetric(
	t *testing.T,
	events <-chan trace.TopicWriterSendingChangedInfo,
) trace.TopicWriterSendingChangedInfo {
	t.Helper()

	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for sending metric")

		return trace.TopicWriterSendingChangedInfo{}
	}
}

func receiveWrittenMetric(
	t *testing.T,
	events <-chan trace.TopicWriterMessagesWrittenInfo,
) trace.TopicWriterMessagesWrittenInfo {
	t.Helper()

	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for written metric")

		return trace.TopicWriterMessagesWrittenInfo{}
	}
}

func receiveDuration(
	t *testing.T,
	events <-chan trace.TopicWriterMetricDurationInfo,
	kind trace.TopicWriterMetricDurationKind,
) trace.TopicWriterMetricDurationInfo {
	t.Helper()

	select {
	case event := <-events:
		require.Equal(t, kind, event.Kind)

		return event
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s duration", kind)

		return trace.TopicWriterMetricDurationInfo{}
	}
}

func receiveMetricDurations(
	t *testing.T,
	events <-chan trace.TopicWriterMetricDurationInfo,
	kinds ...trace.TopicWriterMetricDurationKind,
) map[trace.TopicWriterMetricDurationKind]trace.TopicWriterMetricDurationInfo {
	t.Helper()

	expected := make(map[trace.TopicWriterMetricDurationKind]struct{}, len(kinds))
	for _, kind := range kinds {
		expected[kind] = struct{}{}
	}
	received := make(map[trace.TopicWriterMetricDurationKind]trace.TopicWriterMetricDurationInfo, len(kinds))
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for len(received) < len(expected) {
		select {
		case event := <-events:
			if _, ok := expected[event.Kind]; !ok {
				t.Fatalf("unexpected %s duration while waiting for metrics", event.Kind)
			}
			if _, ok := received[event.Kind]; ok && event.Kind != trace.TopicWriterMetricDurationCompression {
				t.Fatalf("unexpected duplicate %s duration", event.Kind)
			}
			received[event.Kind] = event
		case <-timer.C:
			t.Fatalf("timed out waiting for duration metrics %v", kinds)
		}
	}

	return received
}

func waitForMultiWriterAck(t *testing.T, o *orchestrator, seqNo int64) {
	t.Helper()

	require.Eventually(t, func() bool {
		acked := false
		o.mu.WithLock(func() {
			for iter := o.buf.inFlightMessages.Front(); iter != nil; iter = iter.Next() {
				if iter.Value.SeqNo == seqNo {
					acked = iter.Value.ackReceived

					return
				}
			}
		})

		return acked
	}, time.Second, time.Millisecond)
}
