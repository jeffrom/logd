package events

import (
	"bufio"
	"bytes"
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

func (r *queryIndex) String() string {
	b := &bytes.Buffer{}
	for i := 0; i < r.batchesN; i++ {
		b.WriteString(r.batches[i].String())
		b.WriteString("\n")
	}
	return b.String()
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
	if currPartOff < 0 {
		currPartOff = 0
	}
	collectedSize := batch.size
	currPart := batch.partition

	// fmt.Println("Query", off, messages, n)
	for collectedMessages < messages {
		if head >= r.batchesN { // we're out of batches
			break
		}
		batch = r.batches[head]

		if batch.partition > currPart && !r.alreadyAdded(res, currPart, currPartOff, collectedSize) {
			res.add(currPart, currPartOff, collectedSize)
			currPart = batch.partition
			currPartOff = 0
			collectedSize = 0
		}

		collectedSize += batch.size
		collectedMessages += batch.messages

		head++
	}

	if collectedSize > 0 && !r.alreadyAdded(res, currPart, currPartOff, collectedSize) {
		res.add(currPart, currPartOff, collectedSize)
	}
	// fmt.Println(n, head, r.batchesN, len(r.batches), res)

	return nil
}

func (r *queryIndex) alreadyAdded(res *partitionArgList, part uint64, off, size int) bool {
	if r.batchesN == 0 || len(r.batches) == 0 || res.nparts == 0 {
		return false
	}
	head := res.parts[res.nparts-1]
	if head.offset == part {
		return true
	}
	return false
}

func (r *queryIndex) Push(off, part uint64, size, messages int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.pushBatch(off, part, size, messages)
}

func (r *queryIndex) findStart(off uint64) (int, bool) {
	start := 0
	stop := r.batchesN
	mid := stop / 2
	for start < stop {
		batch := r.batches[mid]
		// fmt.Println("searching for", off, ":", start, mid, stop, batch)
		if batch.offset == off {
			return mid, true
		}

		if off < batch.offset {
			stop = mid - 1
		} else {
			start = mid + 1
		}
		mid = (start + stop) / 2
		// fmt.Println("shifted window", start, mid, stop)
	}

	// fmt.Println("final check for", off, ":", mid, r.batches[mid])
	if r.batches[mid] != nil && r.batches[mid].offset == off {
		return mid, true
	}
	return -1, false
}

func (r *queryIndex) ensureBatches() {
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
}

func (r *queryIndex) handleAddPartition(part uint64) error {
	if _, ok := r.parts[part]; !ok {
		// fmt.Println("handleAddPartition", r.parts, len(r.parts), r.maxPartitions)
		if len(r.parts) >= r.maxPartitions {
			if err := r.rotate(part); err != nil {
				return err
			}
		} else {
			r.parts[part] = r.batchesN
		}
	}

	return nil
}

func (r *queryIndex) pushBatch(off, part uint64, size, messages int) error {
	if err := r.handleAddPartition(part); err != nil {
		return err
	}

	i := r.batchesN
	r.ensureBatches()

	batch := r.batches[i]
	if batch == nil {
		batch = &queryIndexBatch{}
	}
	batch.offset = off
	batch.partition = part
	batch.size = size
	batch.messages = messages
	r.batches[i] = batch
	// fmt.Println("pushBatch", batch)

	r.batchesN++
	return nil
}

func (r *queryIndex) rotate(newPart uint64) error {
	minPart, minIdx := r.minPart()
	// fmt.Println("rotate", minPart, minIdx)
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

	return finaln, finalidx + 1
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
	// r.batchesN = 0
	for {
		r.ensureBatches()
		batch := r.batches[r.batchesN]
		if batch == nil {
			batch = &queryIndexBatch{}
		}
		if _, err := r.readIndexBatch(br, batch); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err := r.handleAddPartition(batch.partition); err != nil {
			return err
		}
		r.batches[r.batchesN] = batch
		r.batchesN++
		// fmt.Println("readIndex", r.batchesN, batch)
	}
}

func (r *queryIndex) zeroBuf(buf []byte) {
	for i := range buf {
		buf[i] = '0'
	}
}

// func (r *queryIndex) writeZero(w io.Writer, n int) error {
// 	for i := 0; i < n; i++ {
// 		if _, err := w.Write([]byte(0)); err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

// TODO this and the read needs to read/write a known size
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
