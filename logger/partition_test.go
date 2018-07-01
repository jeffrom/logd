package logger

import (
	"log"
	"testing"

	"github.com/jeffrom/logd/testhelper"
)

func TestPartition(t *testing.T) {
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	t.Log("starting in", conf.LogFile)
	p := NewPartitions(conf)
	w := NewWriter(conf)
	defer w.Close()

	if err := w.SetPartition(0); err != nil {
		t.Fatalf("unexpected error setting partition: %+v", err)
	}

	checkList(t, p, 1, []uint64{0})

	part, err := p.Get(0, 0, 0)
	if err != nil {
		t.Fatalf("unexpected error getting partition: %+v", err)
	}

	if rerr := p.Remove(part.Offset()); rerr != nil {
		t.Fatalf("error removing %d: %+v", part.Offset(), rerr)
	}

	checkList(t, p, 0, []uint64{})
	tmpParts, err := p.listTempDir()
	if err != nil {
		t.Fatalf("error listing temp partitions: %+v", err)
	}
	if len(tmpParts) != 1 {
		t.Fatalf("expected 1 temp partition but got %d", len(tmpParts))
	}
	if tmpParts[0].Offset() != 0 {
		t.Fatalf("expected temp partition offset %d but got %d", 0, tmpParts[0].Offset())
	}

	if cerr := part.Close(); cerr != nil {
		t.Fatalf("error closing %d: %+v", part.Offset(), cerr)
	}

	checkList(t, p, 0, []uint64{})

	// NOTE it's not practical to test if the file is removed from the filesystem
	// tmpParts, err = p.listTempDir()
	// if err != nil {
	// 	t.Fatalf("error listing temp partitions: %+v", err)
	// }
	// if len(tmpParts) != 0 {
	// 	t.Fatalf("expected 0 temp partition but got %d", len(tmpParts))
	// }
}

func checkList(t testing.TB, p PartitionManager, l int, offs []uint64) []Partitioner {
	parts, err := p.List()
	if err != nil {
		log.Panicf("unexpected error listing partitions: %+v", err)
	}
	if len(parts) != l {
		log.Panicf("expected %d partition but got %d", l, len(parts))
	}
	if offs != nil {
		for i := 0; i < len(offs); i++ {
			if parts[i].Offset() != offs[i] {
				log.Panicf("expected partition %d to be offset %d but got %d", i, offs[i], parts[i].Offset())
			}
		}
	}
	return parts
}
