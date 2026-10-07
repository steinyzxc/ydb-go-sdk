package topicmultiwriter

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"golang.org/x/sync/errgroup"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter/partitionchooser"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type orchestrator struct {
	ctx    context.Context //nolint:containedctx
	err    error
	cancel context.CancelFunc

	multiWriterCfg *MultiWriterConfig
	writerCfg      *topicwriterinternal.WriterReconnectorConfig
	mu             *xsync.Mutex

	partitionChooser PartitionChooser
	topicDescriber   TopicDescriber

	partitions map[int64]*PartitionInfo
	initDone   empty.Chan

	currentSeqNo int64

	background             *background.Worker
	metricsEnabled         bool
	metricsDurationEnabled bool

	buf                    *inflightBuffer
	writerPool             *partitionWriterPool
	ackReceiver            *ackReceiver
	partitionSplitReceiver *partitionSplitReceiver
	sender                 *sender
}

//nolint:funlen
func newOrchestrator(
	ctx context.Context,
	stop context.CancelFunc,
	topicDescriber TopicDescriber,
	background *background.Worker,
	writerCfg *topicwriterinternal.WriterReconnectorConfig,
	multiWriterCfg *MultiWriterConfig,
) *orchestrator {
	if multiWriterCfg.writersFactory == nil {
		multiWriterCfg.writersFactory = newBaseWritersFactory()
	}

	if multiWriterCfg.WriterIdleTimeout == 0 {
		multiWriterCfg.WriterIdleTimeout = defaultWriterIdleTimeout
	}

	if writerCfg.MaxQueueLen == 0 {
		writerCfg.MaxQueueLen = defaultInFlightMessagesBufferSize
	}

	o := &orchestrator{
		writerCfg:        writerCfg,
		multiWriterCfg:   multiWriterCfg,
		mu:               &xsync.Mutex{},
		topicDescriber:   topicDescriber,
		ctx:              ctx,
		cancel:           stop,
		partitions:       make(map[int64]*PartitionInfo),
		initDone:         make(empty.Chan),
		background:       background,
		partitionChooser: multiWriterCfg.PartitionChooser,
		metricsEnabled: writerCfg.WriterInfo.WriterName != "" && writerCfg.Tracer != nil &&
			(writerCfg.Tracer.OnWriterSendingChanged != nil ||
				writerCfg.Tracer.OnWriterMessagesWritten != nil ||
				writerCfg.Tracer.OnWriterMetricDuration != nil),
		metricsDurationEnabled: writerCfg.WriterInfo.WriterName != "" && writerCfg.Tracer != nil &&
			writerCfg.Tracer.OnWriterMetricDuration != nil,
	}
	o.buf = newInflightBuffer(ctx, o.mu, writerCfg, func() error { return o.getResultErr() })
	o.ackReceiver = newAckReceiver(func(partitionID, seqNo int64) {
		var event topicwritercommon.WriterMetricEvent
		o.mu.WithLock(func() {
			event = o.onAckReceivedNeedLock(partitionID, seqNo)
		})
		o.emitMetricEvent(event)
	})
	o.partitionSplitReceiver = newPartitionSplitReceiver(
		o.onPartitionSplit,
		o.stopWithError,
	)
	o.writerPool = newPartitionWriterPool(
		ctx,
		multiWriterCfg,
		writerCfg,
		background,
		o.ackReceiver.push,
		o.partitionSplitReceiver.push,
		func() {
			o.sender.wakeup()
		},
		o.stopWithError,
	)
	o.sender = newSender(
		ctx,
		o.partitions,
		o.mu,
		o.buf,
		o.writerPool,
		o.partitionSplitReceiver,
		o.stopWithError,
	)

	return o
}

func (o *orchestrator) startWorkers() {
	o.background.Start("ack receiver", func(ctx context.Context) {
		o.ackReceiver.run(o.ctx)
	})
	o.background.Start("partition splitter", func(ctx context.Context) {
		o.partitionSplitReceiver.run(ctx)
	})
	o.background.Start("sender", func(ctx context.Context) {
		o.sender.run()
	})
}

func isOperationErrorOverloaded(err error) bool {
	return xerrors.IsOperationError(err, Ydb.StatusIds_OVERLOADED)
}

func (o *orchestrator) sleepOrDone(delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-o.ctx.Done():
		return o.ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (o *orchestrator) init() (err error) {
	defer close(o.initDone)

	describeResult, err := o.topicDescriber(o.ctx, o.writerCfg.Topic())
	if err != nil {
		o.stopWithError(err)

		return err
	}

	o.mu.WithLock(func() {
		for _, partition := range describeResult.Partitions {
			o.partitions[partition.PartitionID] = &PartitionInfo{
				PartitionInfo: partition,
			}
		}
	})

	if err := o.initSeqNo(); err != nil {
		o.stopWithError(err)

		return err
	}

	o.mu.WithLock(func() {
		if o.partitionChooser == nil {
			o.partitionChooser = partitionchooser.NewByPartitionIDPartitionChooser()
		}

		partitionsToAdd := make([]topictypes.PartitionInfo, 0, len(o.partitions))
		for _, partition := range o.partitions {
			if partition.Splitted() || !partition.Active {
				continue
			}

			partitionsToAdd = append(partitionsToAdd, partition.PartitionInfo)
		}

		err = o.partitionChooser.AddNewPartitions(partitionsToAdd...)
	})
	if err != nil {
		o.stopWithError(err)

		return err
	}

	o.startWorkers()

	return nil
}

func (o *orchestrator) choosePartition(msg message) (partitionID int64, err error) {
	if msg.Key == "" {
		msg.Key = o.multiWriterCfg.ProducerIDPrefix
	}

	partitionID, err = o.partitionChooser.ChoosePartition(msg.PublicMessage)
	if err != nil {
		return 0, fmt.Errorf("choose partition: %w", err)
	}

	return partitionID, nil
}

func (o *orchestrator) pushMessage(ctx context.Context, msg message) (err error) {
	acquired := false
	var bufferWait time.Duration
	defer func() {
		o.finishPushMessage(err, acquired, bufferWait)
	}()

	autoSetSeqNo := o.writerCfg.AutoSetSeqNo
	if err := validateMultiWriterSeqNo(autoSetSeqNo, msg.SeqNo); err != nil {
		return err
	}

	bufferWait, err = o.acquireMessageForPush(ctx, &msg)
	if err != nil {
		return err
	}
	acquired = true

	if o.writerCfg.AutoSetCreatedTime {
		if err := topicwritercommon.FillCreatedAt(
			[]topicwritercommon.MessageWithDataContent{msg.MessageWithDataContent},
			o.writerCfg.Now(),
			false,
		); err != nil {
			return err
		}
	}

	if msg.Metadata == nil {
		msg.Metadata = make(map[string][]byte)
	}
	o.mu.WithLock(func() {
		msg.PartitionID, err = o.choosePartition(msg)
	})
	if err != nil {
		return err
	}

	// saveMessageContent must run after choosePartition: BoundPartitionChooser may
	// write choose_partition_key into msg.Metadata; CacheMessageData (inside saveMessageContent)
	// freezes Metadata and would discard that key if it ran earlier. Keeping this outside
	// o.mu avoids holding the lock over user io.Reader reads.
	if err := o.saveMessageContent(&msg); err != nil {
		return err
	}

	var event topicwritercommon.WriterMetricEvent
	o.mu.WithLock(func() {
		event, err = o.enqueueMessageNeedLock(&msg, autoSetSeqNo, bufferWait)
		if err == nil {
			acquired = false
		}
	})
	o.emitMetricEvent(event)

	return err
}

func (o *orchestrator) acquireMessageForPush(
	ctx context.Context,
	msg *message,
) (bufferWait time.Duration, err error) {
	var startedAt time.Time
	if o.metricsDurationEnabled {
		startedAt = o.writerCfg.Now()
	}
	if err = o.buf.acquireMessage(ctx); err != nil {
		return 0, err
	}
	if o.metricsDurationEnabled {
		bufferWait = o.writerCfg.Now().Sub(startedAt)
	}
	if o.metricsDurationEnabled {
		o.configureMessageTiming(msg)
	}

	return bufferWait, nil
}

func (o *orchestrator) finishPushMessage(
	err error,
	acquired bool,
	bufferWait time.Duration,
) {
	if err != nil && acquired {
		o.buf.releaseMessage()
	}
	if o.metricsDurationEnabled && err != nil && bufferWait >= 0 && acquired {
		o.emitMetricEvent(topicwritercommon.WriterMetricEvent{
			Durations: []topicwritercommon.WriterMetricDurationEvent{{
				Kind:     trace.TopicWriterMetricDurationBufferWait,
				Duration: bufferWait,
			}},
		})
	}
}

func validateMultiWriterSeqNo(autoSetSeqNo bool, seqNo int64) error {
	switch {
	case !autoSetSeqNo && seqNo == 0:
		return ErrNoSeqNo
	case autoSetSeqNo && seqNo != 0:
		return topicwriterinternal.ErrNonZeroSeqNo
	default:
		return nil
	}
}

func (o *orchestrator) configureMessageTiming(msg *message) {
	now := o.writerCfg.Now()
	msg.Timing = msg.EnsureTiming()
	msg.Timing.StartProcessing(now)
	msg.Clock = o.writerCfg.Now
	if o.writerCfg.Tracer.OnWriterMetricDuration == nil {
		return
	}
	msg.CompressionObserver = func(duration time.Duration) {
		ctx := o.writerCfg.LogContext
		if ctx == nil {
			ctx = o.ctx
		}
		topicwritercommon.TraceMetricDuration(
			ctx,
			o.writerCfg.Tracer,
			o.writerCfg.WriterInfo,
			trace.TopicWriterMetricDurationCompression,
			duration,
		)
	}
}

func (o *orchestrator) enqueueMessageNeedLock(
	msg *message,
	autoSetSeqNo bool,
	bufferWait time.Duration,
) (event topicwritercommon.WriterMetricEvent, err error) {
	if o.ctx.Err() != nil {
		err = o.err
		if err == nil {
			err = ErrAlreadyClosed
		}

		return event, err
	}
	if autoSetSeqNo {
		o.currentSeqNo++
		msg.SeqNo = o.currentSeqNo
	} else {
		err = o.reserveSeqNoNeedLock(msg.PartitionID, msg.SeqNo)
		if err != nil {
			return event, err
		}
	}

	if o.metricsEnabled {
		event = topicwritercommon.WriterMetricEvent{
			MessagesDelta: 1,
			BytesDelta:    int64(msg.BufUncompressedSize),
		}
	}
	o.buf.pushNeedLock(*msg)
	if o.metricsDurationEnabled && o.writerCfg.ForcedCodec() == rawtopiccommon.CodecRaw {
		processingFinishedAt := o.writerCfg.Now()
		if duration, ok := msg.Timing.FinishProcessing(processingFinishedAt); ok {
			event.Durations = append(event.Durations, topicwritercommon.WriterMetricDurationEvent{
				Kind:     trace.TopicWriterMetricDurationProcessing,
				Duration: duration,
			})
		}
	}
	if o.metricsDurationEnabled && bufferWait >= 0 {
		event.Durations = append(event.Durations, topicwritercommon.WriterMetricDurationEvent{
			Kind:     trace.TopicWriterMetricDurationBufferWait,
			Duration: bufferWait,
		})
	}
	o.sender.wakeup()

	return event, nil
}

func (o *orchestrator) saveMessageContent(msg *message) error {
	tracer := o.writerCfg.Tracer
	if tracer == nil {
		tracer = &trace.Topic{}
	}
	logCtx := o.writerCfg.LogContext
	onCompressDone := gtrace.TopicOnWriterCompressMessages(
		tracer,
		&logCtx,
		o.multiWriterCfg.ProducerIDPrefix,
		"",
		rawtopiccommon.CodecRaw.ToInt32(),
		msg.SeqNo,
		1,
		trace.TopicWriterCompressMessagesReasonCompressDataOnWriteReadData,
	)
	// Materialize raw payload before the message is exposed to writer reconnector.
	// This keeps multiwriter-owned messages resendable even if a downstream writer
	// reads the original reader and fails before enqueueing to its own buffer.
	err := msg.CacheMessageData(rawtopiccommon.CodecRaw)
	onCompressDone(err)
	if err != nil {
		return err
	}

	return nil
}

func (o *orchestrator) emitMetricEvent(event topicwritercommon.WriterMetricEvent) {
	if !o.metricsEnabled || event.IsEmpty() {
		return
	}

	ctx := o.writerCfg.LogContext
	if ctx == nil {
		ctx = o.ctx
	}
	info := o.writerCfg.WriterInfo
	tracer := o.writerCfg.Tracer
	topicwritercommon.TraceSendingChanged(
		ctx,
		tracer,
		info,
		event.MessagesDelta,
		event.BytesDelta,
	)
	topicwritercommon.TraceMessagesWritten(
		ctx,
		tracer,
		info,
		event.WrittenMessages,
	)
	for _, duration := range event.Durations {
		topicwritercommon.TraceMetricDuration(ctx, tracer, info, duration.Kind, duration.Duration)
	}
}

func (o *orchestrator) onAckReceivedNeedLock(partitionID, seqNo int64) topicwritercommon.WriterMetricEvent {
	if o.ctx.Err() != nil {
		return topicwritercommon.WriterMetricEvent{}
	}

	indexChain, ok := o.buf.inFlightMessagesIndex[partitionID]
	if !ok {
		return topicwritercommon.WriterMetricEvent{}
	}

	message := indexChain.Front()
	if message == nil {
		return topicwritercommon.WriterMetricEvent{}
	}
	if message.Value.Value.ackReceived {
		return topicwritercommon.WriterMetricEvent{}
	}

	if message.Value.Value.SeqNo != 0 && message.Value.Value.SeqNo != seqNo {
		panic(fmt.Sprintf("seqNo mismatch, expected: %d, got: %d", message.Value.Value.SeqNo, seqNo))
	}

	message.Value.Value.ackReceived = true
	var event topicwritercommon.WriterMetricEvent
	if o.metricsEnabled {
		event = topicwritercommon.WriterMetricEvent{WrittenMessages: 1}
		if o.metricsDurationEnabled {
			if ackDuration, ok := message.Value.Value.Timing.MarkAck(o.writerCfg.Now()); ok {
				event.Durations = append(event.Durations, topicwritercommon.WriterMetricDurationEvent{
					Kind:     trace.TopicWriterMetricDurationAck,
					Duration: ackDuration,
				})
			}
		}
	}

	indexChain.Remove(message)
	if indexChain.Len() == 0 {
		delete(o.buf.inFlightMessagesIndex, partitionID)
		o.writerPool.evict(partitionID)
	}

	partition := o.partitions[partitionID]
	if partition != nil && partition.PendingResend > 0 {
		partition.PendingResend--
		if partition.PendingResend == 0 {
			partition.Locked = false
		}
	}

	o.buf.sweep()
	if len(o.buf.pendingMessagesIndex) > 0 || (partition != nil && partition.PendingResend == 0) {
		o.sender.wakeup()
	}

	return event
}

func (o *orchestrator) getSplittedPartitionAncestors(
	describeResult *topictypes.TopicDescription,
	partitionID int64,
) []int64 {
	partitionToParent := make(map[int64]int64)
	for _, partition := range describeResult.Partitions {
		if len(partition.ParentPartitionIDs) == 0 {
			continue
		}
		partitionToParent[partition.PartitionID] = partition.ParentPartitionIDs[0]
	}

	var (
		ancestors          = []int64{partitionID}
		currentPartitionID = partitionID
	)

	for {
		parentID, ok := partitionToParent[currentPartitionID]
		if !ok {
			break
		}

		ancestors = append(ancestors, parentID)
		currentPartitionID = parentID
	}

	return ancestors
}

func (o *orchestrator) addNewPartitions(
	parentPartition *PartitionInfo,
	describeResult *topictypes.TopicDescription,
	splittedPartitionID int64,
) error {
	var (
		childPartitions = make([]int64, 0, 2)
		partitionsToAdd = make([]topictypes.PartitionInfo, 0, 2)
	)

	for _, partition := range describeResult.Partitions {
		if len(partition.ParentPartitionIDs) > 0 && partition.ParentPartitionIDs[0] == splittedPartitionID {
			childPartitions = append(childPartitions, partition.PartitionID)
			partitionsToAdd = append(partitionsToAdd, partition)
			o.partitions[partition.PartitionID] = &PartitionInfo{
				PartitionInfo: partition,
				Locked:        true,
			}
		}
	}

	parentPartition.ChildPartitionIDs = childPartitions

	return o.partitionChooser.AddNewPartitions(partitionsToAdd...)
}

func (o *orchestrator) rechoosePartition(msg *message) (err error) {
	msg.PartitionID = 0
	msg.PartitionID, err = o.choosePartition(*msg)

	return err
}

func (o *orchestrator) reserveSeqNoNeedLock(partitionID, seqNo int64) error {
	partition := o.partitions[partitionID]
	if partition == nil {
		return fmt.Errorf("partition not found: %d", partitionID)
	}
	if seqNo <= partition.LastQueuedSeqNo {
		return fmt.Errorf(
			"%w: seqNo %d <= last seqNo %d for partition %d",
			ErrUnorderedSeqNo,
			seqNo,
			partition.LastQueuedSeqNo,
			partitionID,
		)
	}

	partition.LastQueuedSeqNo = seqNo

	return nil
}

//nolint:funlen
func (o *orchestrator) scheduleResendMessages(
	partitionID,
	maxSeqNo int64,
) (event topicwritercommon.WriterMetricEvent, err error) {
	select {
	case <-o.ctx.Done():
		return event, nil
	default:
	}

	inFlightIndexChain, ok := o.buf.inFlightMessagesIndex[partitionID]
	if !ok {
		return event, nil
	}

	var (
		inFlightMessagesToAdd    []messagePtr
		messagesToResendToAdd    []messagePtr
		pendingResendByPartition = make(map[int64]int)
	)

	for inFlightIndexChain.Len() > 0 {
		iter := inFlightIndexChain.Front()

		msg := iter.Value.Value
		if msg.SeqNo <= maxSeqNo {
			if msg.ackReceived {
				o.buf.sweep()

				continue
			}

			ackEvent := o.onAckReceivedNeedLock(partitionID, msg.SeqNo)
			event.WrittenMessages += ackEvent.WrittenMessages
			event.Durations = append(event.Durations, ackEvent.Durations...)

			continue
		}

		if err := o.rechoosePartition(&msg); err != nil {
			return event, err
		}

		iter.Value.Value.PartitionID = msg.PartitionID
		if partition := o.partitions[msg.PartitionID]; partition != nil {
			partition.LastQueuedSeqNo = max(partition.LastQueuedSeqNo, msg.SeqNo)
		}
		inFlightMessagesToAdd = append(inFlightMessagesToAdd, iter.Value)
		iter.Value.Value.sent = false
		messagesToResendToAdd = append(messagesToResendToAdd, iter.Value)
		pendingResendByPartition[msg.PartitionID]++
		inFlightIndexChain.Remove(iter)
	}

	for i := len(inFlightMessagesToAdd) - 1; i >= 0; i-- {
		o.buf.getInflightMessagesIndex(inFlightMessagesToAdd[i].Value.PartitionID).PushFront(inFlightMessagesToAdd[i])
	}
	for i := len(messagesToResendToAdd) - 1; i >= 0; i-- {
		o.buf.getMessagesToResendIndex(messagesToResendToAdd[i].Value.PartitionID).PushFront(messagesToResendToAdd[i])
	}

	for resendPartitionID, count := range pendingResendByPartition {
		partition := o.partitions[resendPartitionID]
		partition.PendingResend += count
		partition.Locked = true
	}

	delete(o.buf.inFlightMessagesIndex, partitionID)
	delete(o.buf.pendingMessagesIndex, partitionID)
	delete(o.buf.messagesToResendIndex, partitionID)
	o.buf.sweep()

	if len(o.buf.pendingMessagesIndex) > 0 || len(o.buf.messagesToResendIndex) > 0 {
		o.sender.wakeup()
	}

	return event, nil
}

func (o *orchestrator) initSeqNo() error {
	const (
		maxRetries = 5
		retryDelay = 100 * time.Millisecond
	)

	partitions := make([]int64, 0, len(o.partitions))
	for partitionID := range o.partitions {
		partitions = append(partitions, partitionID)
	}

	var (
		maxSeqNo int64
		err      error
	)

	for i := range maxRetries {
		maxSeqNo, err = o.getMaxSeqNo(partitions)
		if err == nil {
			break
		}

		if !isOperationErrorOverloaded(err) || i == maxRetries-1 {
			return err
		}

		for _, partitionID := range partitions {
			o.writerPool.forceEvict(partitionID)
		}
		if err := o.sleepOrDone(retryDelay); err != nil {
			return err
		}
	}

	o.mu.WithLock(func() {
		o.currentSeqNo = maxSeqNo
	})

	for _, partitionID := range partitions {
		o.writerPool.evict(partitionID)
	}

	return nil
}

//nolint:funlen
func (o *orchestrator) getMaxSeqNo(partitions []int64) (maxSeqNo int64, err error) {
	var errGroup errgroup.Group
	errGroup.SetLimit(10)

	for _, partition := range partitions {
		errGroup.Go(func() (resultErr error) {
			var (
				partitionInfo      *PartitionInfo
				seqNoAlreadyCached bool
				splitted           bool
			)
			o.mu.WithLock(func() {
				partitionInfo = o.partitions[partition]
				if partitionInfo == nil {
					return
				}
				seqNoAlreadyCached = partitionInfo.CachedMaxSeqNo != 0
				maxSeqNo = max(maxSeqNo, partitionInfo.CachedMaxSeqNo)
				splitted = partitionInfo.Splitted()
			})
			if partitionInfo == nil {
				return fmt.Errorf("partition not found: %d", partition)
			}
			if seqNoAlreadyCached {
				return nil
			}

			var writer *writerWrapper
			if splitted {
				writer, resultErr = o.writerPool.get(partition, false)
				if resultErr != nil {
					return resultErr
				}
			} else {
				o.mu.WithLock(func() {
					writer, resultErr = o.writerPool.get(partition, true)
				})
			}

			if resultErr != nil {
				return resultErr
			}

			initInfo, err := writer.WaitInitInfo(o.ctx)
			if err != nil {
				return err
			}

			o.mu.WithLock(func() {
				maxSeqNo = max(maxSeqNo, initInfo.LastSeqNum)
				partitionInfo.CachedMaxSeqNo = initInfo.LastSeqNum
			})

			return nil
		})
	}

	if err := errGroup.Wait(); err != nil {
		return 0, err
	}

	return maxSeqNo, nil
}

func (o *orchestrator) describeTopicWithRetries(splitPartitionID int64) (topictypes.TopicDescription, error) {
	const (
		maxRetries = 5
		retryDelay = 100 * time.Millisecond
	)

	for range maxRetries {
		describeResult, err := o.topicDescriber(o.ctx, o.writerCfg.Topic())
		if err == nil {
			var needRetry bool
			for _, partition := range describeResult.Partitions {
				if partition.PartitionID == splitPartitionID {
					needRetry = len(partition.ChildPartitionIDs) == 0

					break
				}
			}

			if !needRetry {
				return describeResult, nil
			}
		}

		if err := o.sleepOrDone(retryDelay); err != nil {
			return topictypes.TopicDescription{}, err
		}
	}

	return topictypes.TopicDescription{}, errors.New("failed to describe topic")
}

//nolint:funlen
func (o *orchestrator) onPartitionSplit(partitionID int64) (resultErr error) {
	var isAlreadySplitted bool

	describeResult, err := o.describeTopicWithRetries(partitionID)
	if err != nil {
		return err
	}

	o.mu.WithLock(func() {
		partition := o.partitions[partitionID]
		if partition == nil {
			resultErr = fmt.Errorf("partition not found: %d", partitionID)

			return
		}
		if partition.Splitted() {
			isAlreadySplitted = true

			return
		}

		partition.Locked = true
		if resultErr = o.addNewPartitions(partition, &describeResult, partitionID); resultErr != nil {
			return
		}
		o.partitionChooser.RemovePartition(partitionID)
	})

	if resultErr != nil || isAlreadySplitted {
		return resultErr
	}

	ancestors := o.getSplittedPartitionAncestors(&describeResult, partitionID)
	maxSeqNo, err := o.getMaxSeqNo(ancestors)
	if err != nil {
		return err
	}
	var metricEvent topicwritercommon.WriterMetricEvent
	o.mu.WithLock(func() {
		partition := o.partitions[partitionID]
		if partition == nil {
			resultErr = fmt.Errorf("partition not found: %d", partitionID)

			return
		}

		metricEvent, err = o.scheduleResendMessages(partitionID, maxSeqNo)
		if err != nil {
			resultErr = err

			return
		}

		partition.Locked = false
		for _, child := range partition.ChildPartitionIDs {
			o.partitions[child].Locked = false
		}

		for _, ancestor := range ancestors {
			o.writerPool.evict(ancestor)
		}
	})
	o.emitMetricEvent(metricEvent)

	return resultErr
}

func (o *orchestrator) getResultErr() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.err
}

func (o *orchestrator) stop() {
	o.mu.WithLock(func() {
		o.cancel()
	})
}

func (o *orchestrator) stopWithError(err error) {
	o.mu.WithLock(func() {
		if o.ctx.Err() != nil {
			return
		}

		o.err = err
		o.cancel()
	})
}

func (o *orchestrator) flush(ctx context.Context) error {
	waitCh := make(empty.Chan)

	o.mu.WithLock(func() {
		if o.buf.inFlightMessages.Len() == 0 {
			close(waitCh)

			return
		}

		lastInFlightMessage := o.buf.inFlightMessages.Back()
		prevAckCallback := lastInFlightMessage.Value.onAckCallback
		lastInFlightMessage.Value.onAckCallback = func() {
			if prevAckCallback != nil {
				prevAckCallback()
			}

			close(waitCh)
		}
	})

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-o.ctx.Done():
		return o.getResultErr()
	case <-waitCh:
		return nil
	}
}

func (o *orchestrator) waitInitDone(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-o.initDone:
		return o.getResultErr()
	}
}
