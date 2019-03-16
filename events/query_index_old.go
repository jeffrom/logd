package events

import (
	"io"
	"sync"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/protocol"
	"github.com/pkg/errors"
)

var queryIndexOldPool = sync.Pool{
	New: func() interface{} {
		return &queryIndexOld{
			// partArgBuf:   newPartitionArgList(1),
			// batchScanner: protocol.NewBatchScanner(nil, nil),
		}
	},
}

type queryIndexOld struct {
	topic *topic

	// stuff that needs to work concurrently
	partArgBuf   *partitionArgList
	batchScanner *protocol.BatchScanner
}

func newQueryIndexOld(topic *topic) *queryIndexOld {
	return &queryIndexOld{}
}
func (q *queryIndexOld) initialize(topic *topic, conf *config.Config) *queryIndexOld {
	q.topic = topic
	if q.partArgBuf == nil || q.partArgBuf.max < conf.MaxBatchSize {
		q.partArgBuf = newPartitionArgList(conf.MaxBatchSize)
	}
	if q.batchScanner == nil {
		q.batchScanner = protocol.NewBatchScanner(conf, nil)
	} else if q.batchScanner.MaxSize() < conf.MaxBatchSize {
		// TODO could just grow the batch buffer if needed
		q.batchScanner = protocol.NewBatchScanner(conf, nil)
	}
	return q
}

func (q *queryIndexOld) Query(off uint64, messages int) (*partitionArgList, error) {
	soff, delta, err := q.topic.parts.lookup(off)
	// fmt.Printf("%v\ngatherReadArgs: offset: %d, partition: %d, delta: %d, err: %v\n", topic.parts, off, soff, delta, err)
	if err != nil {
		return nil, err
	}

	q.partArgBuf.reset()
	scanner := q.batchScanner
	n := 0
	currstart := soff
Loop:
	for n < messages {
		p, gerr := q.topic.parts.logp.Get(currstart, delta, 0)
		if gerr != nil {
			// if we've successfully read anything, we've read the last
			// partition by now
			if q.partArgBuf.nparts > 0 {
				// fmt.Println("all done", q.partArgBuf.nparts)
				return q.partArgBuf, nil
			}
			return nil, gerr
		}
		defer p.Close()

		scanner.Reset(p)
		for scanner.Scan() {
			q.partArgBuf.nbatches++
			b := scanner.Batch()
			n += b.Messages
			if n >= messages {
				q.partArgBuf.add(currstart, delta, scanner.Scanned())
				// fmt.Println("scanned enough", currstart, q.partArgBuf.parts[:q.partArgBuf.nparts])
				break Loop
			}
		}
		// fmt.Println("finished part", currstart, q.partArgBuf.parts[:q.partArgBuf.nparts])

		serr := scanner.Error()
		// if we've read a partition and and we haven't read any messages, it's
		// an error. probably an incorrect offset near the end of the partition
		if serr == io.EOF && n > 0 {
			q.partArgBuf.add(currstart, delta, p.Size()-delta)
			currstart = p.Offset() + uint64(p.Size())
			delta = 0
			// fmt.Println("next part", currstart, q.partArgBuf.parts[:q.partArgBuf.nparts])
		} else if serr == io.EOF {
			return nil, io.ErrUnexpectedEOF
		} else if serr != nil {
			return nil, errors.Wrap(protocol.ErrInvalidOffset, serr.Error())
		}
	}

	return q.partArgBuf, nil
}
