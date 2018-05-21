package events

import (
	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/protocol"
)

// NOTE keep this updated when protocol changes
const reasonableBatchSize = 4096 * 100

type partitions struct {
	conf   *config.Config
	head   *partition
	parts  []*partition
	nparts int
}

func newPartitions(conf *config.Config) *partitions {
	p := &partitions{
		conf:  conf,
		parts: make([]*partition, conf.MaxPartitions),
	}

	for i := 0; i < len(p.parts); i++ {
		p.parts[i] = newPartition(conf)
	}
	p.head = p.parts[0]
	return p
}

func (p *partitions) reset() {
	p.nparts = 0
	p.head = p.parts[0]
}

// add is used when loading the log from disk
func (p *partitions) add(offset uint64, size int) {
	if p.nparts == p.conf.MaxPartitions-1 {
		p.rotate()
	}

	part := p.parts[p.nparts]
	part.reset()
	part.startOffset = offset
	part.size = size
	p.head = part

	if p.nparts < p.conf.MaxPartitions-1 {
		p.nparts++
	}
}

func (p *partitions) rotate() {
	parts := p.parts
	if len(parts) <= 1 {
		return
	}

	for i := len(parts) - 2; i >= 0; i-- {
		parts[i], parts[i+1] = parts[i+1], parts[i]
	}
}

func (p *partitions) available() int {
	return p.conf.PartitionSize - p.head.size
}

func (p *partitions) shouldRotate(size int) bool {
	return size > p.conf.PartitionSize-p.head.size
}

func (p *partitions) nextStartOffset() uint64 {
	return p.head.startOffset + uint64(p.head.size)
}

func (p *partitions) addBatch(b *protocol.Batch, size int) {
	if p.shouldRotate(size) {
		p.add(p.nextStartOffset(), 0)
	}
	p.head.addBatch(b, size)
}

func (p *partitions) headOffset() uint64 {
	return p.head.startOffset + uint64(p.head.size)
}

// addBatch is used as batches come in from clients
// func (p *partitions) addBatch(offset uint64, firstOffset uint64, size int, fullSize int) {
// 	part := p.head
// 	// TODO if fullSize + partition.written > conf.MaxBatchSize, swap out the partition
// 	part.addBatch(offset, firstOffset, size, fullSize)
// }

type partition struct {
	conf        *config.Config
	startOffset uint64
	nbatches    int
	size        int
}

func newPartition(conf *config.Config) *partition {
	// nAllocatedBatches := conf.PartitionSize / reasonableBatchSize
	// if nAllocatedBatches < 50 {
	// 	nAllocatedBatches = 50
	// }
	p := &partition{
		conf: conf,
	}

	// for i := 0; i < nAllocatedBatches; i++ {
	// 	p.batches[i] = newBatch()
	// }

	return p
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
