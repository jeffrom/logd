package events

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/jeffrom/logd/logger"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

func TestQueryIndexPush(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	maxParts := 4
	conf.MaxPartitions = maxParts
	qi := newQueryIndex(conf.WorkDir, "default", maxParts)
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
		if err := qi.Push(off, part, size, messages); err != nil && !os.IsNotExist(err) {
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
	conf := testhelper.DefaultConfig(testing.Verbose())
	maxParts := 4
	conf.MaxPartitions = maxParts
	qi := newQueryIndex(conf.WorkDir, "default", maxParts)
	testQueryIndexPush(t, qi, 1000)
	args := newPartitionArgList(500)

	off := qi.batches[0].offset
	if err := qi.Query(off, 150, args); err != nil {
		t.Fatal(err)
	}
	if args.nparts != 1 {
		t.Fatal("expected 1 partitions but got ", args.nparts)
	}

	args.initialize(500)
	off = qi.batches[0].offset
	if err := qi.Query(off, 500, args); err != nil {
		t.Fatal(err)
	}
	if args.nparts != 2 {
		t.Fatal("expected 2 partitions but got ", args.nparts)
	}
}

func TestQueryIndexReadWrite(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	maxParts := 4
	conf.MaxPartitions = maxParts
	qi := newQueryIndex(conf.WorkDir, "default", maxParts)
	testQueryIndexPush(t, qi, 10)

	b := &bytes.Buffer{}
	buf := make([]byte, 8)
	for i := 0; i < qi.batchesN; i++ {
		if _, err := qi.writeIndexBatch(b, buf, qi.batches[i]); err != nil {
			t.Fatal(err)
		}
	}
	// fmt.Printf("%q\n", b.Bytes())

	rdr := bufio.NewReader(b)
	for i := 0; i < 10; i++ {
		batch := qi.batches[i]
		if _, err := qi.readIndexBatch(rdr, batch); err != nil {
			t.Fatal(err)
		}
		// fmt.Println(batch)
	}

	// TODO assert the serialized batches match the unserialized version
}

func TestQueryIndexRotateSync(t *testing.T) {
	maxParts := 3

	m := newMockIndexManager()
	qi := newMockQueryIndex("default", maxParts, m)
	partArgs := newPartitionArgList(500)

	checkError(t, qi.Push(0, 0, 100, 1))
	expectNumBatches(t, 1, qi)
	// fmt.Println("before", qi.batches[0])
	checkError(t, qi.writeIndex(0))

	qi = newMockQueryIndex("default", maxParts, m)
	checkError(t, qi.readIndex(0))
	// fmt.Println("after", qi.batches[0])
	expectNumBatches(t, 1, qi)

	checkError(t, qi.Query(0, 1, partArgs))
	checkPartArgListSize(t, partArgs, 1)
	checkPartArg(t, partArgs.parts[0], 0, 0, 100)
	// fmt.Printf("%+v\n", partArgs.parts[0])

	checkError(t, qi.Push(100, 100, 100, 2))
	expectNumBatches(t, 2, qi)

	partArgs.initialize(500)
	checkError(t, qi.Query(0, 1, partArgs))
	checkPartArgListSize(t, partArgs, 1)
	checkPartArg(t, partArgs.parts[0], 0, 0, 100)

	partArgs.initialize(500)
	checkError(t, qi.Query(0, 2, partArgs))
	checkPartArgListSize(t, partArgs, 2)
	checkPartArg(t, partArgs.parts[0], 0, 0, 100)
	checkPartArg(t, partArgs.parts[1], 100, 0, 100)

	partArgs.initialize(500)
	checkError(t, qi.Query(0, 3, partArgs))
	checkPartArgListSize(t, partArgs, 2)
	checkPartArg(t, partArgs.parts[0], 0, 0, 100)
	checkPartArg(t, partArgs.parts[1], 100, 0, 100)

	partArgs.initialize(500)
	checkError(t, qi.Query(0, 4, partArgs))
	checkPartArgListSize(t, partArgs, 2)
	checkPartArg(t, partArgs.parts[0], 0, 0, 100)
	checkPartArg(t, partArgs.parts[1], 100, 0, 100)

	checkError(t, qi.Push(200, 200, 100, 3))
	expectNumBatches(t, 3, qi)
	checkError(t, qi.Push(300, 300, 100, 4))

	// fmt.Println(qi.batchesN, qi.batches[:qi.batchesN])
	expectNumBatches(t, 3, qi)
	expectError(t, protocol.ErrNotFound, qi.Query(0, 1, partArgs))

	partArgs.initialize(500)
	checkError(t, qi.Query(100, 1, partArgs))
	checkPartArgListSize(t, partArgs, 1)
	checkPartArg(t, partArgs.parts[0], 100, 0, 100)

	partArgs.initialize(500)
	checkError(t, qi.Query(100, 2, partArgs))
	checkPartArgListSize(t, partArgs, 1)
	checkPartArg(t, partArgs.parts[0], 100, 0, 100)

	partArgs.initialize(500)
	checkError(t, qi.Query(100, 3, partArgs))
	checkPartArgListSize(t, partArgs, 2)
	checkPartArg(t, partArgs.parts[0], 100, 0, 100)
	checkPartArg(t, partArgs.parts[1], 200, 0, 100)

	checkError(t, qi.Push(400, 400, 100, 2))
	expectNumBatches(t, 3, qi)
	// fmt.Println(qi.batchesN, qi.batches[:qi.batchesN])

	expectError(t, protocol.ErrNotFound, qi.Query(100, 1, partArgs))

	partArgs.initialize(500)
	checkError(t, qi.Query(200, 1, partArgs))
	checkPartArgListSize(t, partArgs, 1)
	checkPartArg(t, partArgs.parts[0], 200, 0, 100)

	fmt.Println("write 200", m)
	// fmt.Println(qi.batchesN, qi.batches[:qi.batchesN])
	checkError(t, qi.writeIndex(200))
	// fmt.Println("write 300", m)
	checkError(t, qi.writeIndex(300))
	checkError(t, qi.writeIndex(400))
	// t.Fatalf("welp, 200 didn't get written for some reason")

	// fmt.Println("newMockQueryIndex")
	qi = newMockQueryIndex("default", maxParts, m)
	checkError(t, qi.readIndex(200))
	expectNumBatches(t, 1, qi)
	checkError(t, qi.readIndex(300))
	expectNumBatches(t, 2, qi)
	checkError(t, qi.readIndex(400))
	expectNumBatches(t, 3, qi)

	partArgs.initialize(500)
	expectError(t, protocol.ErrNotFound, qi.Query(0, 1, partArgs))
	expectError(t, protocol.ErrNotFound, qi.Query(0, 100, partArgs))

	partArgs.initialize(500)
	checkError(t, qi.Query(200, 1, partArgs))
	checkPartArgListSize(t, partArgs, 1)
	checkPartArg(t, partArgs.parts[0], 200, 0, 100)

	partArgs.initialize(500)
	checkError(t, qi.Query(200, 3, partArgs))
	checkPartArgListSize(t, partArgs, 1)
	checkPartArg(t, partArgs.parts[0], 200, 0, 100)

	partArgs.initialize(500)
	checkError(t, qi.Query(200, 4, partArgs))
	checkPartArgListSize(t, partArgs, 2)
	checkPartArg(t, partArgs.parts[0], 200, 0, 100)
	checkPartArg(t, partArgs.parts[1], 300, 0, 100)

	partArgs.initialize(500)
	checkError(t, qi.Query(200, 7, partArgs))
	checkPartArgListSize(t, partArgs, 2)
	checkPartArg(t, partArgs.parts[0], 200, 0, 100)
	checkPartArg(t, partArgs.parts[1], 300, 0, 100)

	partArgs.initialize(500)
	checkError(t, qi.Query(200, 8, partArgs))
	checkPartArgListSize(t, partArgs, 3)
	checkPartArg(t, partArgs.parts[0], 200, 0, 100)
	checkPartArg(t, partArgs.parts[1], 300, 0, 100)
	checkPartArg(t, partArgs.parts[2], 400, 0, 100)
}

func newMockQueryIndex(topic string, maxPartitions int, m logger.IndexManager) *queryIndex {
	qi := newQueryIndex("", topic, maxPartitions)
	qi.manager = m
	return qi
}

type mockFile struct {
	bytes.Buffer
	closed bool
}

func (f *mockFile) Close() error {
	f.closed = true
	return nil
}

type mockIndexManager struct {
	indexes map[string]map[uint64]*mockFile
}

func newMockIndexManager() *mockIndexManager {
	return &mockIndexManager{
		indexes: make(map[string]map[uint64]*mockFile),
	}
}

func (m *mockIndexManager) GetIndex(topic string, part uint64) (io.ReadWriteCloser, error) {
	fmt.Println("GetIndex", topic, part)
	parts, ok := m.indexes[topic]
	if !ok {
		m.indexes[topic] = make(map[uint64]*mockFile)
		parts = m.indexes[topic]
	}

	idx, ok := parts[part]
	if !ok {
		fmt.Println("GetIndex not found", topic, part)
		parts[part] = &mockFile{Buffer: bytes.Buffer{}}
		idx = parts[part]
	}
	return idx, nil
}

func (m *mockIndexManager) RemoveIndex(topic string, part uint64) error {
	parts, ok := m.indexes[topic]
	if !ok {
		m.indexes[topic] = make(map[uint64]*mockFile)
		parts = m.indexes[topic]
	}

	delete(parts, part)
	return nil
}

func checkPartArgListSize(t testing.TB, partArgs *partitionArgList, size int) {
	t.Helper()
	if partArgs.nparts != size {
		t.Fatalf("expected arg length of %d but got: %+v", size, partArgs)
	}
}

func checkPartArg(t testing.TB, arg *partitionArgs, off uint64, delta, limit int) {
	t.Helper()
	if arg.offset != off {
		t.Fatalf("expected offset %d but got: %+v", off, arg)
	}
	if arg.delta != delta {
		t.Fatalf("expected delta %d but got: %+v", delta, arg)
	}
	if arg.limit != limit {
		t.Fatalf("expected limit %d but got: %+v", limit, arg)
	}
}

func checkError(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func expectError(t testing.TB, expected error, err error) {
	t.Helper()
	if err != expected {
		t.Fatalf("expected %v but got %v", expected, err)
	}
}

func expectNumBatches(t testing.TB, n int, qi *queryIndex) {
	t.Helper()
	count := 0
	for i := 0; i < len(qi.batches); i++ {
		if qi.batches[i] == nil {
			break
		}
		count++
	}
	if count != n {
		t.Fatalf("expected query index to have %d batches but had %d: %+v", n, count, qi.batches[:count])
	}
}
