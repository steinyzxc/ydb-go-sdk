package topicwriterinternal

import (
	"context"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type WritersCommonConfig struct {
	WriterInfo         topicwritercommon.WriterInfo
	producerID         string
	topic              string
	writerMeta         map[string]string
	partitioning       rawtopicwriter.Partitioning
	compressorCount    int
	maxBytesPerMessage int

	LogContext         context.Context //nolint:containedctx
	Tracer             *trace.Topic
	rawTopicClient     *rawtopic.Client
	cred               credentials.Credentials
	credUpdateInterval time.Duration
	clock              clockwork.Clock
	forceCodec         rawtopiccommon.Codec
}

// FinalizeWriterInfo fills the canonical topic in the stable logical-writer
// attributes. An empty writer name intentionally remains empty, which disables
// writer metrics.
func (c *WritersCommonConfig) FinalizeWriterInfo() {
	c.WriterInfo.Topic = c.topic
}

func (c *WritersCommonConfig) Topic() string {
	return c.topic
}

func (c *WritersCommonConfig) PartitionID() (int64, bool) {
	switch c.partitioning.Type {
	case rawtopicwriter.PartitioningPartitionID,
		rawtopicwriter.PartitioningPartitionWithGeneration:
		return c.partitioning.PartitionID, true
	default:
		return 0, false
	}
}

func (c *WritersCommonConfig) ProducerID() string {
	return c.producerID
}

func (c *WritersCommonConfig) Now() time.Time {
	return c.clock.Now()
}

// ForcedCodec returns the codec selected for every message when auto-selection is disabled.
func (c *WritersCommonConfig) ForcedCodec() rawtopiccommon.Codec {
	return c.forceCodec
}
