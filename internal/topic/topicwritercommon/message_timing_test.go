package topicwritercommon

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
)

func TestMessageTimingFinishesProcessingOnceIncludingZero(t *testing.T) {
	start := time.Unix(100, 0)
	timing := &MessageTiming{}

	timing.StartProcessing(start)
	duration, ok := timing.FinishProcessing(start)
	if !ok || duration != 0 {
		t.Fatalf("FinishProcessing() = (%v, %v), want (0, true)", duration, ok)
	}
	if duration, ok := timing.FinishProcessing(start.Add(time.Second)); ok || duration != 0 {
		t.Fatalf("second FinishProcessing() = (%v, %v), want (0, false)", duration, ok)
	}
}

func TestMessageTimingAckUsesFirstSendAcrossRetries(t *testing.T) {
	start := time.Unix(200, 0)
	timing := &MessageTiming{}

	if duration, ok := timing.MarkAck(start); ok || duration != 0 {
		t.Fatalf("ACK before send = (%v, %v), want (0, false)", duration, ok)
	}
	timing.MarkFirstSend(start.Add(time.Second))
	timing.MarkFirstSend(start.Add(2 * time.Second))

	duration, ok := timing.MarkAck(start.Add(4 * time.Second))
	if !ok || duration != 3*time.Second {
		t.Fatalf("MarkAck() = (%v, %v), want (3s, true)", duration, ok)
	}
	if duration, ok := timing.MarkAck(start.Add(5 * time.Second)); ok || duration != 0 {
		t.Fatalf("second MarkAck() = (%v, %v), want (0, false)", duration, ok)
	}
}

func TestCompressionTimingStartsAfterInputRead(t *testing.T) {
	var samples []time.Duration
	message := MessageWithDataContent{
		PublicMessage: PublicMessage{Data: failingReader{}},
		Encoders:      NewMultiEncoder(),
		Clock:         func() time.Time { return time.Unix(600, 0) },
		CompressionObserver: func(duration time.Duration) {
			samples = append(samples, duration)
		},
	}

	_, err := message.GetEncodedBytes(rawtopiccommon.CodecGzip)
	if err == nil {
		t.Fatal("GetEncodedBytes() error = nil, want read error")
	}
	if len(samples) != 0 {
		t.Fatalf("compression samples after failed read = %d, want 0", len(samples))
	}
}

func TestCompressionTimingIncludesFailedEncoderAttempt(t *testing.T) {
	const codec = rawtopiccommon.Codec(rawtopiccommon.CodecCustomerFirst)
	encoders := NewMultiEncoder()
	encoders.AddEncoder(codec, func(io.Writer) (io.WriteCloser, error) {
		return failingWriter{}, nil
	})

	var samples []time.Duration
	message := MessageWithDataContent{
		PublicMessage: PublicMessage{Data: bytes.NewReader([]byte("payload"))},
		Encoders:      encoders,
		Clock:         func() time.Time { return time.Unix(601, 0) },
		CompressionObserver: func(duration time.Duration) {
			samples = append(samples, duration)
		},
	}

	_, err := message.GetEncodedBytes(codec)
	if err == nil {
		t.Fatal("GetEncodedBytes() error = nil, want encoder error")
	}
	if len(samples) != 1 || samples[0] != 0 {
		t.Fatalf("compression samples = %v, want [0]", samples)
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) {
	return 0, errors.New("read failed")
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("encode failed")
}

func (failingWriter) Close() error {
	return nil
}
