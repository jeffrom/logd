package events

import (
	"sync"
)

type readIndex interface {
	Query(off uint64, messages int) (*partitionArgList, error)
	Push(off, part uint64, size, messages int) error
}

type queryIndex struct {
	maxPartitions int
	batches       []*queryIndexBatch
	batchesN      int
	parts         map[uint64]int
	mu            sync.Mutex
}

type queryIndexBatch struct {
	offset    uint64
	partition uint64
	size      int
	messages  int
}

func newQueryIndex(maxPartitions int) *queryIndex {
	return &queryIndex{
		maxPartitions: maxPartitions,
		parts:         make(map[uint64]int),
	}
}

func (r *queryIndex) reset() {
	r.batchesN = 0
}

func (r *queryIndex) Query(off uint64, messages int) (*partitionArgList, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return nil, nil
}

func (r *queryIndex) Push(off, part uint64, size, messages int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.pushBatch(off, part, size, messages)
	return nil
}

func (r *queryIndex) pushBatch(off, part uint64, size, messages int) {
	if _, ok := r.parts[part]; !ok {
		if len(r.parts) >= r.maxPartitions {
			r.rotate(part)
		} else {
			r.parts[part] = r.batchesN
		}
	}

	i := r.batchesN
	if cap(r.batches) <= i {
		nextSize := 1000
		if cap(r.batches) > 0 {
			nextSize = cap(r.batches) * 2
		}
		batches := make([]*queryIndexBatch, nextSize)
		copy(batches, r.batches)
		r.batches = batches
		// fmt.Println("grew to", len(r.batches))
	}

	batch := r.batches[i]
	if batch == nil {
		batch = &queryIndexBatch{}
	}
	batch.offset = off
	batch.partition = part
	batch.size = size
	batch.messages = messages
	r.batches[i] = batch

	r.batchesN++
}

func (r *queryIndex) rotate(newPart uint64) {
	minPart, minIdx := r.minPart()
	copy(r.batches, r.batches[minIdx:])
	for idx := range r.parts {
		r.parts[idx] -= minIdx
	}
	r.batchesN -= minIdx
	delete(r.parts, minPart)

	r.parts[newPart] = r.batchesN
}

func (r *queryIndex) minPart() (uint64, int) {
	var finaln uint64
	var finalidx int
	var started bool

	for n, idx := range r.parts {
		if !started {
			finaln = n
			finalidx = idx
		}
		started = true

		if n < finaln {
			finaln = n
			finalidx = idx
		}
	}

	return finaln, finalidx
}
