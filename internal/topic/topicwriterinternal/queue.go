package topicwriterinternal

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	errCloseClosedMessageQueue   = xerrors.Wrap(errors.New("ydb: close closed message queue"))
	errAckOnClosedMessageQueue   = xerrors.Wrap(errors.New("ydb: ack on closed message queue"))
	errGetMessageFromClosedQueue = xerrors.Wrap(errors.New("ydb: get message from closed message queue"))
	errAddUnorderedMessages      = xerrors.Wrap(errors.New("ydb: add unordered messages"))
	errAckUnexpectedMessage      = xerrors.Wrap(errors.New("ydb: ack unexpected message"))
)

const (
	//nolint:mnd
	intSize = 32 << (^uint(0) >> 63) // copy from math package for use in go <= 1.16
	maxInt  = 1<<(intSize-1) - 1     // copy from math package for use in go <= 1.16
	minInt  = -1 << (intSize - 1)    // copy from math package for use in go <= 1.16

	minPositiveIndexWhichOrderLessThenNegative = maxInt / 2
)

type messageQueue struct {
	OnAckReceived func(count int)
	AckCallback   func(seqNo int64)
	OnMetricEvent func(topicwritercommon.WriterMetricEvent)
	Now           func() time.Time

	hasNewMessages empty.Chan
	closedErr      error

	m                         xsync.RWMutex
	stopReceiveMessagesReason error
	closed                    bool
	closedChan                empty.Chan
	lastWrittenIndex          int
	lastSentIndex             int
	lastSeqNo                 int64

	messagesByOrder map[int]*queuedMessage
	seqNoToOrderID  map[int64]int
}

type queuedMessage struct {
	messageWithDataContent

	// acked is created on the first Wait and shared by all waiters of this message.
	acked empty.Chan
}

func newMessageQueue() messageQueue {
	return messageQueue{
		messagesByOrder: make(map[int]*queuedMessage),
		seqNoToOrderID:  make(map[int64]int),
		hasNewMessages:  make(empty.Chan, 1),
		closedChan:      make(empty.Chan),
		lastSeqNo:       -1,
	}
}

func (q *messageQueue) AddMessages(messages []messageWithDataContent) error {
	_, event, err := q.addMessages(messages, false)
	q.emitMetricEvent(event)

	return err
}

func (q *messageQueue) AddMessagesWithWaiter(messages []messageWithDataContent) (
	waiter MessageQueueAckWaiter,
	err error,
) {
	waiter, event, err := q.addMessages(messages, true)
	q.emitMetricEvent(event)

	return waiter, err
}

func (q *messageQueue) addMessages(messages []messageWithDataContent, needWaiter bool) (
	waiter MessageQueueAckWaiter,
	event topicwritercommon.WriterMetricEvent,
	err error,
) {
	q.m.Lock()
	defer q.m.Unlock()

	if q.stopReceiveMessagesReason != nil {
		return waiter, event, xerrors.WithStackTrace(
			fmt.Errorf("ydb: add message to closed message queue: %w", q.stopReceiveMessagesReason),
		)
	}

	if err = q.checkNewMessagesBeforeAddNeedLock(messages); err != nil {
		return waiter, event, err
	}

	for i := range messages {
		messageIndex := q.addMessageNeedLock(messages[i])

		if needWaiter {
			waiter.AddWaitIndex(messageIndex)
		}
	}

	if q.OnMetricEvent != nil {
		var bytesCount int64
		for i := range messages {
			bytesCount += int64(messages[i].BufUncompressedSize)
		}
		event = topicwritercommon.WriterMetricEvent{
			MessagesDelta: int64(len(messages)),
			BytesDelta:    bytesCount,
		}
		var processingFinishedAt time.Time
		for i := range messages {
			if !messages[i].ProcessingReadyAtAdmission {
				continue
			}
			if processingFinishedAt.IsZero() {
				processingFinishedAt = q.now()
			}
			if duration, ok := messages[i].Timing.FinishProcessing(processingFinishedAt); ok {
				event.Durations = append(event.Durations, topicwritercommon.WriterMetricDurationEvent{
					Kind:     trace.TopicWriterMetricDurationProcessing,
					Duration: duration,
				})
			}
		}
	}
	q.notifyNewMessages()

	return waiter, event, nil
}

func (q *messageQueue) notifyNewMessages() {
	select {
	case q.hasNewMessages <- empty.Struct{}:
		// pass
	default:
	}
}

func (q *messageQueue) checkNewMessagesBeforeAddNeedLock(messages []messageWithDataContent) error {
	if len(messages) == 0 {
		return nil
	}

	checkedSeqNo := q.lastSeqNo
	for i := range messages {
		if messages[i].SeqNo <= checkedSeqNo {
			return xerrors.WithStackTrace(errAddUnorderedMessages)
		}
		checkedSeqNo = messages[i].SeqNo
	}

	return nil
}

func (q *messageQueue) addMessageNeedLock(
	mess messageWithDataContent, //nolint:gocritic
) (messageIndex int) {
	q.lastWrittenIndex++
	messageIndex = q.lastWrittenIndex

	if messageIndex == minInt {
		q.ensureNoSmallIntIndexes()
	}

	if _, ok := q.messagesByOrder[messageIndex]; ok {
		panic(fmt.Errorf("ydb: bad internal state os message queue - already exists with index: %v", messageIndex))
	}

	q.messagesByOrder[messageIndex] = &queuedMessage{messageWithDataContent: mess}
	q.seqNoToOrderID[mess.SeqNo] = messageIndex
	q.lastSeqNo = mess.SeqNo

	return messageIndex
}

func (q *messageQueue) AcksReceived(acks []rawtopicwriter.WriteAck) error {
	ackReceivedCounter, event, err := q.acksReceived(acks)

	if q.OnAckReceived != nil {
		q.OnAckReceived(ackReceivedCounter)
	}
	q.emitMetricEvent(event)

	return err
}

func (q *messageQueue) acksReceived(
	acks []rawtopicwriter.WriteAck,
) (ackReceivedCounter int, event topicwritercommon.WriterMetricEvent, err error) {
	q.m.Lock()
	defer q.m.Unlock()

	if q.closed {
		return 0, event, xerrors.WithStackTrace(errAckOnClosedMessageQueue)
	}

	for i := range acks {
		message, ackErr := q.ackReceivedNeedLock(acks[i].SeqNo)
		if ackErr != nil {
			return ackReceivedCounter, event, ackErr
		}

		if q.OnMetricEvent != nil {
			event.WrittenMessages++
			if message.Timing != nil {
				if duration, ok := message.Timing.MarkAck(q.now()); ok {
					event.Durations = append(event.Durations, topicwritercommon.WriterMetricDurationEvent{
						Kind:     trace.TopicWriterMetricDurationAck,
						Duration: duration,
					})
				}
			}
		}

		// AckCallback enqueues ACKs for the multiwriter orchestrator, whose
		// in-flight messages must be processed in the same order as the queue.
		// Keep it under q.m so concurrent stream ACK batches cannot overtake.
		if q.AckCallback != nil {
			q.AckCallback(acks[i].SeqNo)
		}
		ackReceivedCounter++
	}

	return ackReceivedCounter, event, nil
}

func (q *messageQueue) ackReceivedNeedLock(seqNo int64) (messageWithDataContent, error) {
	orderID, ok := q.seqNoToOrderID[seqNo]
	if !ok {
		return messageWithDataContent{}, xerrors.WithStackTrace(errAckUnexpectedMessage)
	}
	message := q.messagesByOrder[orderID]

	if acked := q.messagesByOrder[orderID].acked; acked != nil {
		close(acked)
	}
	delete(q.seqNoToOrderID, seqNo)
	delete(q.messagesByOrder, orderID)

	return message.messageWithDataContent, nil
}

func (q *messageQueue) StopAddNewMessages(reason error) {
	q.m.Lock()
	defer q.m.Unlock()

	q.stopAddNewMessagesNeedLock(reason)
}

func (q *messageQueue) stopAddNewMessagesNeedLock(reason error) {
	if q.stopReceiveMessagesReason == nil {
		q.stopReceiveMessagesReason = reason
	}
}

func (q *messageQueue) Close(err error) error {
	isFirstTimeClosed := false
	q.m.Lock()
	defer func() {
		seqNoToOrderIDLen := len(q.seqNoToOrderID)
		q.m.Unlock()

		// release all
		if isFirstTimeClosed && q.OnAckReceived != nil {
			q.OnAckReceived(seqNoToOrderIDLen)
		}
	}()

	q.stopAddNewMessagesNeedLock(err)

	if q.closed {
		return xerrors.WithStackTrace(errCloseClosedMessageQueue)
	}
	isFirstTimeClosed = true

	q.closed = true
	q.closedErr = err
	close(q.closedChan)

	return nil
}

func (q *messageQueue) emitMetricEvent(event topicwritercommon.WriterMetricEvent) {
	if q.OnMetricEvent != nil && !event.IsEmpty() {
		q.OnMetricEvent(event)
	}
}

func (q *messageQueue) now() time.Time {
	if q.Now != nil {
		return q.Now()
	}

	return time.Now()
}

func (q *messageQueue) ensureNoSmallIntIndexes() {
	for k := range q.messagesByOrder {
		if k >= 0 && k < minPositiveIndexWhichOrderLessThenNegative {
			panic("ydb: message queue has bad state - overflow or has very old element")
		}
	}
}

// GetMessagesForSend one or more messages for send
// it blocked until context cancelled of have least one message for send
func (q *messageQueue) GetMessagesForSend(ctx context.Context) ([]messageWithDataContent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var closed bool
	q.m.WithLock(func() {
		closed = q.closed
	})
	if closed {
		return nil, xerrors.WithStackTrace(errGetMessageFromClosedQueue)
	}

	for {
		res := q.getMessagesForSendWithLock()
		if len(res) != 0 {
			return res, nil
		}

		select {
		case <-ctx.Done():
			return nil, xerrors.WithStackTrace(ctx.Err())
		case <-q.hasNewMessages:
			// pass
		case <-q.closedChan:
			return nil, xerrors.WithStackTrace(fmt.Errorf("ydb: message queue closed with: %w", q.closedErr))
		}
	}
}

func (q *messageQueue) ResetSentProgress() {
	q.m.Lock()
	defer q.m.Unlock()

	minKey := q.lastWrittenIndex
	for k := range q.messagesByOrder {
		if isFirstCycledIndexLess(k, minKey) {
			minKey = k
		}
	}

	q.lastSentIndex = minKey - 1
	q.notifyNewMessages()
}

func (q *messageQueue) getMessagesForSendWithLock() []messageWithDataContent {
	q.m.Lock()
	defer q.m.Unlock()

	if q.lastWrittenIndex == q.lastSentIndex {
		return nil
	}

	var res []messageWithDataContent

	// use  "!=" stop instead of  "<" - for work with negative indexes after overflow
	for q.lastWrittenIndex != q.lastSentIndex {
		q.lastSentIndex++

		// msg may be unexisted if it already has ack from server
		// pass
		if msg, ok := q.messagesByOrder[q.lastSentIndex]; ok {
			res = append(res, msg.messageWithDataContent)
		}
	}

	return res
}

func (q *messageQueue) Wait(ctx context.Context, waiter MessageQueueAckWaiter) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	for {
		var acked empty.ChanReadonly
		q.m.WithLock(func() {
			for len(waiter.sequenseNumbers) > 0 {
				index := waiter.sequenseNumbers[0]
				if msg, ok := q.messagesByOrder[index]; ok {
					if msg.acked == nil {
						msg.acked = make(empty.Chan)
					}
					acked = msg.acked

					return
				}
				waiter.sequenseNumbers = waiter.sequenseNumbers[1:]
			}
		})
		if acked == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-q.closedChan:
			return q.closedErr
		case <-acked:
			// Check the remaining messages.
		}
	}
}

// WaitLastWritten waits for last written message gets ack.
func (q *messageQueue) WaitLastWritten(ctx context.Context) error {
	var lastIndex int
	q.m.WithRLock(func() {
		lastIndex = q.lastWrittenIndex
	})

	return q.Wait(ctx, MessageQueueAckWaiter{sequenseNumbers: []int{lastIndex}})
}

type MessageQueueAckWaiter struct {
	sequenseNumbers []int
}

func (m *MessageQueueAckWaiter) AddWaitIndex(index int) {
	m.sequenseNumbers = append(m.sequenseNumbers, index)
}

// sortMessageQueueIndexes deprecated
func sortMessageQueueIndexes(keys []int) {
	sort.Ints(keys)
	// check index overflow
	if len(keys) > 0 && keys[0] < 0 && keys[len(keys)-1] > 0 {
		sort.Slice(keys, func(i, k int) bool {
			return isFirstCycledIndexLess(keys[i], keys[k])
		})
	}
}

func isFirstCycledIndexLess(first, second int) bool {
	switch {
	case first > minPositiveIndexWhichOrderLessThenNegative && second < 0:
		return true
	case first < 0 && second > minPositiveIndexWhichOrderLessThenNegative:
		return false
	default:
		return first < second
	}
}
