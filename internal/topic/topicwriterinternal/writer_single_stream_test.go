package topicwriterinternal

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestWriterImpl_CreateInitMessage(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		ctx := xtest.Context(t)
		cfg := SingleStreamWriterConfig{
			WritersCommonConfig: WritersCommonConfig{
				producerID:      "producer",
				topic:           "topic",
				writerMeta:      map[string]string{"key": "val"},
				partitioning:    rawtopicwriter.NewPartitioningPartitionID(5),
				compressorCount: 1,
			},
			getLastSeqNum: true,
		}
		w := newSingleStreamWriterStopped(ctx, cfg)
		expected := rawtopicwriter.InitRequest{
			Path:             w.cfg.topic,
			ProducerID:       w.cfg.producerID,
			WriteSessionMeta: w.cfg.writerMeta,
			Partitioning:     w.cfg.partitioning,
			GetLastSeqNo:     true,
		}
		require.Equal(t, expected, w.createInitRequest())
	})

	t.Run("WithoutGetLastSeq", func(t *testing.T) {
		ctx := xtest.Context(t)
		w := newSingleStreamWriterStopped(ctx,
			SingleStreamWriterConfig{getLastSeqNum: false},
		)
		require.False(t, w.createInitRequest().GetLastSeqNo)
	})
}

func TestCutRequestBytes(t *testing.T) {
	t.Run("SingleLargeMessage", func(t *testing.T) {
		orig := &rawtopicwriter.WriteRequest{Messages: []rawtopicwriter.MessageData{
			{Data: make([]byte, 1000)},
		}}
		head, rest := cutRequestBytes(orig, 10)
		require.Same(t, orig, head)
		require.Nil(t, rest)
	})
	t.Run("SingleSmallMessage", func(t *testing.T) {
		orig := &rawtopicwriter.WriteRequest{Messages: []rawtopicwriter.MessageData{
			{Data: make([]byte, 1000)},
		}}
		head, rest := cutRequestBytes(orig, 1000)
		require.Same(t, orig, head)
		require.Nil(t, rest)
	})
	t.Run("TwoSmall", func(t *testing.T) {
		orig := &rawtopicwriter.WriteRequest{Messages: []rawtopicwriter.MessageData{
			{Data: make([]byte, 100)},
			{Data: make([]byte, 200)},
		}}
		head, rest := cutRequestBytes(orig, 1000)
		require.Same(t, orig, head)
		require.Nil(t, rest)
	})
	t.Run("Split", func(t *testing.T) {
		orig := &rawtopicwriter.WriteRequest{Messages: []rawtopicwriter.MessageData{
			{Data: make([]byte, 100)},
			{Data: make([]byte, 200)},
			{Data: make([]byte, 300)},
		}}

		head, rest := cutRequestBytes(orig, 300)

		JSON := func(a any) string {
			res, _ := json.MarshalIndent(a, "", "  ")

			return string(res)
		}

		// it will take only one message, becase it has overhead for grpc messages struct
		expectedHead := JSON(&rawtopicwriter.WriteRequest{Messages: []rawtopicwriter.MessageData{
			{Data: make([]byte, 100)},
		}})
		expectedRest := JSON(&rawtopicwriter.WriteRequest{Messages: []rawtopicwriter.MessageData{
			{Data: make([]byte, 200)},
			{Data: make([]byte, 300)},
		}})

		headJSON := JSON(head)
		restJSON := JSON(rest)
		require.Equal(t, expectedHead, headJSON)
		require.Equal(t, expectedRest, restJSON)
	})
}

func TestSendMessagesToStreamMarksOnlyMessagesInSentSplit(t *testing.T) {
	ctrl := gomock.NewController(t)
	stream := NewMockRawTopicWriterStream(ctrl)
	stream.EXPECT().Send(gomock.Any()).Return(errors.New("send failed"))

	base := time.Unix(500, 0)
	messages := []messageWithDataContent{
		newTestMessageWithDataContent(1),
		newTestMessageWithDataContent(2),
	}
	for i := range messages {
		messages[i].Data = bytes.NewReader([]byte("message"))
		messages[i].Timing = &topicwritercommon.MessageTiming{}
		require.NoError(t, messages[i].CacheMessageData(rawtopiccommon.CodecRaw))
	}

	err := sendMessagesToStream(
		stream,
		1,
		rawtopiccommon.CodecRaw,
		messages,
		func() time.Time { return base },
		func(trace.TopicWriterMetricDurationKind, time.Duration) {},
	)
	require.Error(t, err)

	duration, ok := messages[0].Timing.MarkAck(base.Add(time.Second))
	require.True(t, ok)
	require.Equal(t, time.Second, duration)
	_, ok = messages[1].Timing.MarkAck(base.Add(time.Second))
	require.False(t, ok)
}

func TestSendMessagesToStreamFinishesProcessingAfterRequestBuild(t *testing.T) {
	ctrl := gomock.NewController(t)
	stream := NewMockRawTopicWriterStream(ctrl)
	stream.EXPECT().Send(gomock.Any()).Return(nil)

	base := time.Unix(510, 0)
	now := base
	const codec = rawtopiccommon.Codec(rawtopiccommon.CodecCustomerFirst)
	encoders := topicwritercommon.NewMultiEncoder()
	encoders.AddEncoder(codec, func(writer io.Writer) (io.WriteCloser, error) {
		return advancingEncoderWriter{
			writer:  writer,
			advance: func() { now = now.Add(2 * time.Second) },
		}, nil
	})
	message := newTestMessageWithDataContent(1)
	message.Encoders = encoders
	message.RawBuf.WriteString("message")
	message.HasRawContent = true
	message.BufUncompressedSize = len("message")
	message.Timing = &topicwritercommon.MessageTiming{}
	message.Timing.StartProcessing(base)

	var gotKind trace.TopicWriterMetricDurationKind
	var gotDuration time.Duration
	err := sendMessagesToStream(
		stream,
		100000,
		codec,
		[]messageWithDataContent{message},
		func() time.Time { return now },
		func(kind trace.TopicWriterMetricDurationKind, duration time.Duration) {
			gotKind = kind
			gotDuration = duration
		},
	)
	require.NoError(t, err)
	require.Equal(t, trace.TopicWriterMetricDurationProcessing, gotKind)
	require.Equal(t, 2*time.Second, gotDuration)
}

func TestWriterProcessingDurationIncludesRawSendBacklogOnlyForAutoCodec(t *testing.T) {
	for _, test := range []struct {
		name     string
		auto     bool
		expected time.Duration
	}{
		{name: "automatic codec finishes at request build", auto: true, expected: 5 * time.Second},
		{name: "forced raw codec finishes at admission", expected: 2 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := xtest.ContextWithCommonTimeout(xtest.Context(t), t)
			clock := clockwork.NewFakeClockAt(time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC))
			var processingDurations []time.Duration
			tracer := &trace.Topic{
				OnWriterMetricDuration: func(info trace.TopicWriterMetricDurationInfo) {
					if info.Kind == trace.TopicWriterMetricDurationProcessing {
						processingDurations = append(processingDurations, info.Duration)
					}
				},
			}
			writerOptions := []PublicWriterOption{
				WithClock(clock),
				WithWriterName("raw-backlog-timing"),
				WithTrace(tracer),
			}
			if test.auto {
				writerOptions = append(writerOptions, WithAutoCodec())
			}
			writer := newTestWriterStopped(writerOptions...)
			writer.firstConnectionHandled.Store(true)

			ctrl := gomock.NewController(t)
			stream := NewMockRawTopicWriterStream(ctrl)
			firstSendStarted := make(chan struct{})
			releaseFirstSend := make(chan struct{})
			t.Cleanup(func() {
				select {
				case <-releaseFirstSend:
				default:
					close(releaseFirstSend)
				}
			})
			stream.EXPECT().Send(gomock.Any()).DoAndReturn(func(rawtopicwriter.ClientMessage) error {
				close(firstSendStarted)
				<-releaseFirstSend

				return nil
			})
			stream.EXPECT().Send(gomock.Any()).Return(nil)

			require.NoError(t, writer.Write(ctx, []PublicMessage{{
				SeqNo: 1,
				Data:  bytes.NewReader([]byte("first")),
			}}))
			firstMessages, err := writer.queue.GetMessagesForSend(ctx)
			require.NoError(t, err)

			firstSendDone := make(chan error, 1)
			go func() {
				firstSendDone <- sendMessagesToStream(
					stream,
					writer.cfg.maxBytesPerMessage,
					rawtopiccommon.CodecRaw,
					firstMessages,
					clock.Now,
					writer.onMetricDuration,
				)
			}()
			select {
			case <-firstSendStarted:
			case err := <-firstSendDone:
				t.Fatalf("first send completed before entering the blocked Send: %v", err)
			case <-ctx.Done():
				t.Fatalf("timed out waiting for first Send to start: %v", ctx.Err())
			}
			processingDurations = nil

			require.NoError(t, writer.Write(ctx, []PublicMessage{{
				SeqNo: 2,
				Data: &writerMetricClockReader{
					advance: func() { clock.Advance(2 * time.Second) },
					reader:  bytes.NewReader([]byte("second")),
				},
			}}))
			clock.Advance(3 * time.Second)
			close(releaseFirstSend)
			select {
			case err := <-firstSendDone:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatalf("timed out waiting for first Send to finish: %v", ctx.Err())
			}

			secondMessages, err := writer.queue.GetMessagesForSend(ctx)
			require.NoError(t, err)
			require.Len(t, secondMessages, 1)
			require.NoError(t, sendMessagesToStream(
				stream,
				writer.cfg.maxBytesPerMessage,
				rawtopiccommon.CodecRaw,
				secondMessages,
				clock.Now,
				writer.onMetricDuration,
			))
			require.Equal(t, []time.Duration{test.expected}, processingDurations)
		})
	}
}

type advancingEncoderWriter struct {
	writer  io.Writer
	advance func()
}

func (w advancingEncoderWriter) Write(data []byte) (int, error) {
	w.advance()

	return w.writer.Write(data)
}

func (advancingEncoderWriter) Close() error {
	return nil
}
