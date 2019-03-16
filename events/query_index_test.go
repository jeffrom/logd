package events

import (
	"testing"
)

func TestQueryIndexPush(t *testing.T) {
	maxParts := 4
	qi := newQueryIndex(maxParts)
	testQueryIndexPush(t, qi, 1000)

	if qi.batchesN >= 500 {
		t.Fatal("rotation failed, size should be 499, but is:", qi.batchesN)
	}
}

func testQueryIndexPush(t *testing.T, qi *queryIndex, iterations int) {
	var off uint64
	var part uint64
	size := 67
	messages := 3

	for i := 0; i < iterations; i++ {
		if err := qi.Push(off, part, size, messages); err != nil {
			t.Fatal(err)
		}
		if len(qi.parts) > qi.maxPartitions {
			t.Fatal("too many partitions, should be", qi.maxPartitions, ", but is:", len(qi.parts))
		}

		off += uint64(size)
		if i%100 == 0 {
			part = off
		}
	}

}

func TestQueryIndex(t *testing.T) {
	maxParts := 4
	qi := newQueryIndex(maxParts)
	testQueryIndexPush(t, qi, 1000)
	args := partitionArgListPool.Get().(*partitionArgList).initialize(maxParts)
	defer partitionArgListPool.Put(args)

	off := qi.batches[0].offset
	if err := qi.Query(off, 150, args); err != nil {
		t.Fatal(err)
	}
	if args.nparts != 1 {
		t.Fatal("expected 1 partitions but got ", args.nparts)
	}

	off = qi.batches[0].offset
	if err := qi.Query(off, 500, args); err != nil {
		t.Fatal(err)
	}
	if args.nparts != 3 {
		t.Fatal("expected 3 partitions but got ", args.nparts)
	}
	// fmt.Println(args)
}
