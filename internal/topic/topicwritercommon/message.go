package topicwritercommon

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type PublicMessage struct {
	SeqNo     int64
	CreatedAt time.Time
	Data      io.Reader
	Metadata  map[string][]byte

	// This parameter can be used ONLY for topic partition selection mode by key.
	Key string
	// This parameter can be used ONLY for topic partition selection mode by partition ID.
	PartitionID int64

	Tx tx.Transaction

	// partitioning at level message available by protocol, but doesn't available by current server implementation
	// the field hidden from public access for prevent runtime errors.
	// it will be published after implementation on server side.
	FuturePartitioning PublicFuturePartitioning
}

// PublicFuturePartitioning will be published in feature, after server implementation completed.
type PublicFuturePartitioning struct {
	MessageGroupID string
	PartitionID    int64
	HasPartitionID bool
}

func (p PublicFuturePartitioning) ToRaw() rawtopicwriter.Partitioning {
	if p.HasPartitionID {
		return rawtopicwriter.NewPartitioningPartitionID(p.PartitionID)
	}

	return rawtopicwriter.NewPartitioningMessageGroup(p.MessageGroupID)
}

func NewPartitioningWithMessageGroupID(id string) PublicFuturePartitioning {
	return PublicFuturePartitioning{
		MessageGroupID: id,
	}
}

func NewPartitioningWithPartitionID(id int64) PublicFuturePartitioning {
	return PublicFuturePartitioning{
		PartitionID:    id,
		HasPartitionID: true,
	}
}

type MessageWithDataContent struct {
	PublicMessage

	DataWasRead         bool
	HasRawContent       bool
	HasEncodedContent   bool
	MetadataCached      bool
	BufCodec            rawtopiccommon.Codec
	BufEncoded          bytes.Buffer
	RawBuf              bytes.Buffer
	Encoders            *MultiEncoder
	BufUncompressedSize int

	// Timing is shared by copies of a logical message made while routing a
	// message through a multi-writer. It is intentionally a pointer so child
	// writers cannot create duplicate processing, send, or ACK samples.
	Timing                     *MessageTiming
	ProcessingReadyAtAdmission bool
	// Clock and CompressionObserver are internal hooks installed by the
	// logical writer. They are nil on messages that do not collect metrics.
	Clock               func() time.Time
	CompressionObserver func(time.Duration)
}

// MessageTiming stores the timestamps for one logical message. Each method
// is safe for concurrent calls from the sender and ACK receiver goroutines.
type MessageTiming struct {
	mu sync.Mutex

	processingStartedAt time.Time
	processingFinished  bool
	firstSentAt         time.Time
	acknowledged        bool
}

// EnsureTiming allocates timing state once and returns the shared pointer.
func (m *MessageWithDataContent) EnsureTiming() *MessageTiming {
	if m.Timing == nil {
		m.Timing = &MessageTiming{}
	}

	return m.Timing
}

// StartProcessing records the beginning of message preparation once.
func (t *MessageTiming) StartProcessing(now time.Time) {
	if t == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.processingStartedAt.IsZero() {
		t.processingStartedAt = now
	}
}

// FinishProcessing completes message preparation once and returns its
// duration. A false result means preparation was not started or was already
// completed.
func (t *MessageTiming) FinishProcessing(now time.Time) (time.Duration, bool) {
	if t == nil {
		return 0, false
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.processingStartedAt.IsZero() || t.processingFinished {
		return 0, false
	}
	t.processingFinished = true
	if now.Before(t.processingStartedAt) {
		return 0, true
	}

	return now.Sub(t.processingStartedAt), true
}

// MarkFirstSend records the first actual transport send once. It is safe to
// call immediately before every split request send; retries preserve the
// original timestamp.
func (t *MessageTiming) MarkFirstSend(now time.Time) {
	if t == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.firstSentAt.IsZero() {
		t.firstSentAt = now
	}
}

// MarkAck completes ACK timing once. ACKs received before any transport send
// produce no sample.
func (t *MessageTiming) MarkAck(now time.Time) (time.Duration, bool) {
	if t == nil {
		return 0, false
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.acknowledged || t.firstSentAt.IsZero() {
		return 0, false
	}
	t.acknowledged = true
	if now.Before(t.firstSentAt) {
		return 0, true
	}

	return now.Sub(t.firstSentAt), true
}

func (m *MessageWithDataContent) GetEncodedBytes(codec rawtopiccommon.Codec) ([]byte, error) {
	if codec == rawtopiccommon.CodecRaw {
		return m.getRawBytes()
	}

	return m.getEncodedBytes(codec)
}

func (m *MessageWithDataContent) cacheMetadata() {
	if m.MetadataCached {
		return
	}

	// ensure message metadata can't be changed by external code
	if len(m.Metadata) > 0 {
		ownCopy := make(map[string][]byte, len(m.Metadata))
		for key, val := range m.Metadata {
			ownCopy[key] = bytes.Clone(val)
		}
		m.Metadata = ownCopy
	} else {
		m.Metadata = nil
	}
	m.MetadataCached = true
}

func (m *MessageWithDataContent) CacheMessageData(codec rawtopiccommon.Codec) error {
	m.cacheMetadata()
	_, err := m.GetEncodedBytes(codec)

	return err
}

func FillCreatedAt(messages []MessageWithDataContent, now time.Time, preserveAssigned bool) error {
	for i := range messages {
		if messages[i].CreatedAt.IsZero() {
			messages[i].CreatedAt = now

			continue
		}

		if !preserveAssigned {
			return xerrors.WithStackTrace(ErrNonZeroCreatedAt)
		}
	}

	return nil
}

func (m *MessageWithDataContent) encodeRawContent(codec rawtopiccommon.Codec) ([]byte, error) {
	if !m.HasRawContent {
		return nil, xerrors.WithStackTrace(errNoRawContent)
	}

	m.BufEncoded.Reset()

	_, err := m.Encoders.encodeBytesWithTiming(
		codec,
		&m.BufEncoded,
		m.RawBuf.Bytes(),
		m.now,
		m.CompressionObserver,
	)
	if err != nil {
		return nil, xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf(
			"ydb: failed to compress message, codec '%v': %w",
			codec,
			err,
		)))
	}

	m.BufCodec = codec

	return m.BufEncoded.Bytes(), nil
}

func (m *MessageWithDataContent) readDataToRawBuf() error {
	m.RawBuf.Reset()
	m.HasRawContent = true
	if m.Data != nil {
		writtenBytes, err := io.Copy(&m.RawBuf, m.Data)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
		m.BufUncompressedSize = int(writtenBytes)
		m.Data = nil
	}

	return nil
}

func (m *MessageWithDataContent) readDataToTargetCodec(codec rawtopiccommon.Codec) error {
	m.DataWasRead = true
	m.HasEncodedContent = true
	m.BufCodec = codec
	m.BufEncoded.Reset()

	reader := m.Data
	if reader == nil {
		reader = &bytes.Reader{}
	}

	bytesCount, err := m.Encoders.encodeWithTiming(
		codec,
		&m.BufEncoded,
		reader,
		m.now,
		m.CompressionObserver,
	)
	if err != nil {
		return xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf(
			"ydb: failed compress message with codec '%v': %w",
			codec,
			err,
		)))
	}
	m.BufUncompressedSize = bytesCount
	m.Data = nil

	return nil
}

func (m *MessageWithDataContent) now() time.Time {
	if m.Clock != nil {
		return m.Clock()
	}

	return time.Now()
}

func (m *MessageWithDataContent) getRawBytes() ([]byte, error) {
	if m.HasRawContent {
		return m.RawBuf.Bytes(), nil
	}
	if m.DataWasRead {
		return nil, xerrors.WithStackTrace(errNoRawContent)
	}

	if err := m.readDataToRawBuf(); err != nil {
		return nil, err
	}

	return m.RawBuf.Bytes(), nil
}

func (m *MessageWithDataContent) getEncodedBytes(codec rawtopiccommon.Codec) ([]byte, error) {
	switch {
	case m.HasEncodedContent && m.BufCodec == codec:
		return m.BufEncoded.Bytes(), nil
	case m.HasRawContent:
		return m.encodeRawContent(codec)
	case m.DataWasRead:
		return nil, errNoRawContent
	default:
		err := m.readDataToTargetCodec(codec)
		if err != nil {
			return nil, err
		}

		return m.BufEncoded.Bytes(), nil
	}
}

func NewMessageDataWithContent(
	message PublicMessage, //nolint:gocritic
	encoders *MultiEncoder,
) MessageWithDataContent {
	return MessageWithDataContent{
		PublicMessage: message,
		Encoders:      encoders,
	}
}
