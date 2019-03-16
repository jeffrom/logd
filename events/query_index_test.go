package events

import (
	"testing"
)

func TestQueryIndexPush(t *testing.T) {
	maxParts := 4
	qi := newQueryIndex(maxParts)
	var off uint64
	var part uint64
	size := 67
	messages := 3

	for i := 0; i < 1000; i++ {
		if err := qi.Push(off, part, size, messages); err != nil {
			t.Fatal(err)
		}
		if len(qi.parts) > maxParts {
			t.Fatal("too many partitions, should be", maxParts, ", but is:", len(qi.parts))
		}

		if i%100 == 0 {
			part = off
		}
		off += uint64(size)
	}

	if qi.batchesN >= 500 {
		t.Fatal("rotation failed, size should be 499, but is:", qi.batchesN)
	}
}
