package events

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/jeffrom/logd/logger"
	"github.com/jeffrom/logd/protocol"
)

// type readIndex interface {
// 	Query(off uint64, messages int) (*partitionArgList, error)
// 	Push(off, part uint64, size, messages int) error
// }

type queryIndex struct {
	maxPartitions int
	topic         string
	manager       logger.IndexManager
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

func (b *queryIndexBatch) String() string {
	return fmt.Sprintf("queryIndexBatch<offset: %d, partition: %d, size: %d, messages %d>", b.offset, b.partition, b.size, b.messages)
}

func newQueryIndex(workDir string, topic string, maxPartitions int) *queryIndex {
	return &queryIndex{
		maxPartitions: maxPartitions,
		topic:         topic,
		manager:       logger.NewFileIndex(workDir),
		parts:         make(map[uint64]int),
	}
}

func (r *queryIndex) reset() {
	r.batchesN = 0
}

func (r *queryIndex) Query(off uint64, messages int, res *partitionArgList) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	n, found := r.findStart(off)
	if !found {
		return protocol.ErrNotFound
	}

	batch := r.batches[n]
	head := n + 1
	collectedMessages := batch.messages
	currPartOff := int(batch.offset - batch.partition)
	currSize := batch.size
	currPart := batch.partition

	for collectedMessages < messages {
		if head >= r.batchesN { // we're out of batches
			break
		}
		batch = r.batches[head]

		if batch.partition > currPart {
			res.add(currPart, currPartOff, currSize)
			currPart = batch.partition
			currPartOff = 0
			currSize = 0
		}

		currSize += batch.size
		collectedMessages += batch.messages

		head++
	}

	if currSize > 0 {
		res.add(currPart, currPartOff, currSize)
	}
	// fmt.Println(n, head, r.batchesN, len(r.batches))

	return nil
}

func (r *queryIndex) Push(off, part uint64, size, messages int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.pushBatch(off, part, size, messages)
}

func (r *queryIndex) findStart(off uint64) (int, bool) {
	for i := 0; i < r.batchesN; i++ {
		batch := r.batches[i]
		if batch.offset > off {
			break
		}
		if batch.offset == off {
			return i, true
		}
	}

	return -1, false
}

func (r *queryIndex) pushBatch(off, part uint64, size, messages int) error {
	if _, ok := r.parts[part]; !ok {
		if len(r.parts) >= r.maxPartitions {
			if err := r.rotate(part); err != nil {
				return err
			}
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
	return nil
}

func (r *queryIndex) rotate(newPart uint64) error {
	minPart, minIdx := r.minPart()
	copy(r.batches, r.batches[minIdx:])
	for idx := range r.parts {
		r.parts[idx] -= minIdx
	}
	r.batchesN -= minIdx
	delete(r.parts, minPart)

	if err := r.manager.RemoveIndex(r.topic, minPart); err != nil {
		return err
	}

	r.parts[newPart] = r.batchesN
	return nil
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

func (r *queryIndex) writeIndex(part uint64) error {
	w, err := r.manager.GetIndex(r.topic, part)
	if err != nil {
		return err
	}
	defer w.Close()

	buf := make([]byte, 8)
	bw := bufio.NewWriter(w)

	start, end := r.partBounds(part)
	// fmt.Println("writing index at", part, ", from", start, r.batches[start], "to", end, r.batches[end-1])
	for i := start; i < end; i++ {
		if _, err := r.writeIndexBatch(bw, buf, r.batches[i]); err != nil {
			return err
		}
	}

	return bw.Flush()
}

func (r *queryIndex) partBounds(part uint64) (int, int) {
	start, ok := r.parts[part]
	if !ok {
		return -1, -1
	}
	end := -1

	for i := 0; i < r.batchesN; i++ {
		batch := r.batches[i]
		if batch.partition < part {
			continue
		}
		if batch.partition > part {
			end = i
			break
		}
		if batch.partition != part {
			return -1, -1
		}
	}

	if end < 0 {
		end = r.batchesN
	}
	return start, end
}

func (r *queryIndex) readIndex(part uint64) error {
	rdr, err := r.manager.GetIndex(r.topic, part)
	if err != nil {
		return err
	}
	defer rdr.Close()

	br := bufio.NewReader(rdr)
	r.batchesN = 0
	for i := 0; i < r.maxPartitions; i++ {
		if r.batches[i] == nil {
			break
		}
		// fmt.Println("readIndex", r.batches[i])
		if _, err := r.readIndexBatch(br, r.batches[i]); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		r.batchesN++
	}
	return nil
}

func (r *queryIndex) writeIndexBatch(w io.Writer, buf []byte, batch *queryIndexBatch) (int, error) {
	var total int

	n := binary.PutUvarint(buf, batch.offset)
	total += n
	if _, err := w.Write(buf[:n]); err != nil {
		return total, err
	}

	n = binary.PutUvarint(buf, batch.partition)
	total += n
	if _, err := w.Write(buf[:n]); err != nil {
		return total, err
	}

	n = binary.PutVarint(buf, int64(batch.size))
	total += n
	if _, err := w.Write(buf[:n]); err != nil {
		return total, err
	}

	n = binary.PutVarint(buf, int64(batch.messages))
	total += n
	if _, err := w.Write(buf[:n]); err != nil {
		return total, err
	}
	return total, nil
}

func (r *queryIndex) readIndexBatch(rdr io.ByteReader, batch *queryIndexBatch) (int, error) {
	var total int

	n, err := binary.ReadUvarint(rdr)
	total += 8
	if err != nil {
		return total, err
	}
	batch.offset = n

	n, err = binary.ReadUvarint(rdr)
	total += 8
	if err != nil {
		return total, err
	}
	batch.partition = n

	sn, err := binary.ReadVarint(rdr)
	total += 8
	if err != nil {
		return total, err
	}
	batch.size = int(sn)

	sn, err = binary.ReadVarint(rdr)
	total += 8
	if err != nil {
		return total, err
	}
	batch.messages = int(sn)

	return total, nil
}
