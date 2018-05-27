package events

import (
	"fmt"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/logger"
	"github.com/jeffrom/logd/protocol"
	"github.com/pkg/errors"
)

// NOTE keep this updated when protocol changes
const reasonableBatchSize = 4096 * 100

type partitions struct {
	conf   *config.Config
	logp   logger.PartitionManager
	head   *partition
	parts  []*partition
	nparts int
}

func newPartitions(conf *config.Config, logp logger.PartitionManager) *partitions {
	p := &partitions{
		conf:  conf,
		parts: make([]*partition, conf.MaxPartitions),
		logp:  logp,
	}

	for i := 0; i < len(p.parts); i++ {
		p.parts[i] = newPartition(conf)
	}
	p.head = p.parts[0]
	return p
}

func (p *partitions) String() string {
	return fmt.Sprint(p.parts)
}

func (p *partitions) reset() {
	p.nparts = 0
	p.head = p.parts[0]
}

// add is used when loading the log from disk
func (p *partitions) add(offset uint64, size int) {
	part := p.parts[p.nparts]
	part.reset()
	part.startOffset = offset
	part.size = size
	p.head = part

	if p.nparts+1 == p.conf.MaxPartitions {
		p.rotate()
	} else if p.nparts+1 < p.conf.MaxPartitions {
		p.nparts++
	}
}

func (p *partitions) rotate() {
	parts := p.parts
	if len(parts) <= 1 {
		return
	}

	for i := 0; i < len(parts)-1; i++ {
		parts[i], parts[i+1] = parts[i+1], parts[i]
	}
}

func (p *partitions) available() int {
	return p.conf.PartitionSize - p.head.size
}

func (p *partitions) shouldRotate(size int) bool {
	return size >= p.conf.PartitionSize-p.head.size
}

func (p *partitions) nextOffset() uint64 {
	next := p.head.startOffset + uint64(p.head.size)
	return next
}

func (p *partitions) addBatch(b *protocol.Batch, size int) {
	if p.shouldRotate(size) {
		p.add(p.nextOffset(), 0)
	}
	p.head.addBatch(b, size)
}

func (p *partitions) headOffset() uint64 {
	return p.head.startOffset + uint64(p.head.size)
}

// getStartOffset gets the start offset from a global offset
func (p *partitions) getStartOffset(off uint64) (uint64, error) {
	for i := 0; i < p.nparts; i++ {
		part := p.parts[i]
		if off >= part.startOffset {
			return part.startOffset, nil
		}
	}
	return 0, protocol.ErrNotFound
}

func (p *partitions) lookup(off uint64) (uint64, int, error) {
	if p.nparts <= 0 {
		return 0, 0, errors.New("no partitions loaded")
	}
	for i := p.nparts - 1; i >= 0; i-- {
		part := p.parts[i]
		if off >= part.startOffset {
			return part.startOffset, int(off - part.startOffset), nil
		}
	}
	return 0, 0, protocol.ErrNotFound
}

type partition struct {
	conf        *config.Config
	startOffset uint64
	nbatches    int
	size        int
}

func newPartition(conf *config.Config) *partition {
	p := &partition{
		conf: conf,
	}

	return p
}

func (p *partition) String() string {
	// return fmt.Sprintf("partition<startOffset: %d, size: %d>", p.startOffset, p.size)
	return fmt.Sprintf("%d", p.startOffset)
}

func (p *partition) reset() {
	p.startOffset = 0
	p.nbatches = 0
	p.size = 0
}

func (p *partition) addBatch(b *protocol.Batch, size int) {
	p.nbatches++
	p.size += size
}

// func (p *partition) addBatch(offset uint64, firstOffset uint64, size int, fullSize int) {
// 	// allocate more memory for batches if needed
// 	if p.nbatches > len(p.batches)-1 {
// 		p.batches = append(p.batches, newBatch())
// 		for i := p.nbatches; i < cap(p.batches)-1; i++ {
// 			p.batches[i] = newBatch()
// 		}
// 	}

// 	b := p.batches[p.nbatches]
// 	b.reset()
// 	b.offset = offset
// 	b.firstOffset = firstOffset
// 	b.size = size
// 	b.fullSize = fullSize
// 	p.nbatches++

// 	p.size += b.fullSize
// }

// func (p *partition) writeOffset() uint64 {
// 	return p.startOffset + uint64(p.size)
// }

type batch struct {
	offset      uint64
	firstOffset uint64
	size        int // size of the body
	fullSize    int // size of the batch including its envelope
	nmessages   int
}

func newBatch() *batch {
	return &batch{}
}

func (b *batch) reset() {
	b.offset = 0
	b.firstOffset = 0
	b.size = 0
	b.fullSize = 0
	b.nmessages = 0
}

type partitionArgList struct {
	conf   *config.Config
	parts  []partitionArgs
	nparts int
}

type partitionArgs struct {
	offset uint64
	delta  int
	limit  int
}

func (pa partitionArgs) String() string {
	return fmt.Sprintf("partitionArgs<offset: %d, delta: %d, limit: %d>", pa.offset, pa.delta, pa.limit)
}

func newPartitionArgList(conf *config.Config) *partitionArgList {
	pl := &partitionArgList{
		conf:  conf,
		parts: make([]partitionArgs, conf.MaxPartitions),
	}

	return pl
}

func (pl *partitionArgList) reset() {
	pl.nparts = 0
}

func (pl *partitionArgList) add(soff uint64, delta int, limit int) {
	if pl.nparts >= pl.conf.MaxPartitions {
		panic("appended too many partitions")
	}
	pl.parts[pl.nparts].offset = soff
	pl.parts[pl.nparts].delta = delta
	pl.parts[pl.nparts].limit = limit

	pl.nparts++
}
