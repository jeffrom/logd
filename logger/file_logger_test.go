package logger

import (
	"bytes"
	"flag"
	"io/ioutil"
	"path"
	"runtime/debug"
	"testing"

	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

func init() {
	// each test module must define this flag and pass its value to the
	// testhelper module.
	flag.BoolVar(&testhelper.Golden, "golden", false, "write the golden file for this module")
	flag.Parse()
}

// var randSrc *rand.Rand

// func init() {
// 	randSrc = rand.New(rand.NewSource(time.Now().UnixNano()))
// }

// func randStr(l int) string {
// 	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
// 	result := make([]byte, l)
// 	for i := range result {
// 		result[i] = chars[randSrc.Intn(len(chars))]
// 	}
// 	return string(result)
// }

func TestFileLoggerCreate(t *testing.T) {
	_, _, teardown := SetupTestFileLogger(testing.Verbose())
	defer teardown()
}

func TestFileLoggerWrite(t *testing.T) {
	config, l, teardown := SetupTestFileLogger(testing.Verbose())
	defer teardown()

	l.Write([]byte("really cool message\n"))
	l.Write([]byte("another really cool message\n"))
	l.Write([]byte("yet another really cool message\n"))
	l.Write([]byte("finally a last really cool message\n"))

	b, err := ioutil.ReadFile(config.LogFile + ".0")
	if err != nil {
		t.Fatalf("failed reading log: %+v", err)
	}
	testhelper.CheckGoldenFile("file_logger_write", b, testhelper.Golden)
}

func TestFileLoggerReadSeek(t *testing.T) {
	config, l, teardown := SetupTestFileLogger(testing.Verbose())
	defer teardown()

	fileWriteLog(t, l, 1, "one")
	fileWriteLog(t, l, 2, "two")
	fileWriteLog(t, l, 3, "three")
	fileWriteLog(t, l, 4, "four")

	if head, err := l.Head(); err != nil {
		t.Fatalf("failed getting head of log: %+v", err)
	} else if head != 4 {
		t.Fatalf("expected head to be 4 but was %d", head)
	}

	if err := l.SeekToID(2); err != nil {
		t.Fatalf("unexpected error seeking: %+v", err)
	}

	out := testhelper.GetLogOutput(config, l)
	testhelper.CheckGoldenFile("file_logger_read_seek", out, testhelper.Golden)
}

func TestFileLoggerReadPartition(t *testing.T) {
	config := testhelper.TestConfig(testing.Verbose())
	config.IndexCursorSize = 10
	config.PartitionSize = 500
	config.LogFile = path.Join("testdata", "benjamin_lines")
	config, l, teardown := SetupTestFileLoggerConfig(config, testing.Verbose())
	defer teardown()

	b := removeMetaFromLog(readRange(t, l, 1, 1))
	if !bytes.Equal(b, testhelper.BenjaminLines[0]) {
		t.Fatalf("expected:\n\n%q\n\ngot:\n\n%q\n", testhelper.BenjaminLines[0], b)
	}
}

func TestFileLoggerIndex(t *testing.T) {
	config, logger, teardown := SetupTestFileLogger(testing.Verbose())
	defer teardown()

	for i := 1; i < 501; i++ {
		n := i
		if n > 2 {
			n /= 2
		}
		if n > 250 {
			n /= 2
		}
		fileWriteLog(t, logger, uint64(i), testhelper.Repeat("word", n))
	}

	out, _ := ioutil.ReadFile(config.IndexFileName())
	testhelper.CheckGoldenFile("file_logger_index", out, testhelper.Golden)
}

func TestFilePartitionWrites(t *testing.T) {
	config := testhelper.TestConfig(testing.Verbose())
	config.IndexCursorSize = 10
	config.PartitionSize = 500
	config.LogFile = testhelper.TmpLog()
	config, l, teardown := SetupTestFileLoggerConfig(config, testing.Verbose())
	defer teardown()

	msg := []byte(testhelper.Repeat("A", 50))
	for i := 0; i < 10; i++ {
		truncated := msg[:len(msg)-(10%(i+1))]
		l.Write(truncated)
	}
	l.Flush()

	part1, _ := ioutil.ReadFile(config.LogFile + ".0")
	testhelper.CheckGoldenFile("file_partition_write.0", part1, testhelper.Golden)
	part2, _ := ioutil.ReadFile(config.LogFile + ".1")
	testhelper.CheckGoldenFile("file_partition_write.1", part2, testhelper.Golden)
	_, err := ioutil.ReadFile(config.LogFile + ".2")
	if err == nil {
		t.Fatal("Expected no third partition")
	}
}

func TestFilePartitionDeleteOldOnes(t *testing.T) {
	config := testhelper.TestConfig(testing.Verbose())
	config.IndexCursorSize = 10
	config.PartitionSize = 500
	config.MaxPartitions = 2
	config.LogFile = testhelper.TmpLog()
	config, l, teardown := SetupTestFileLoggerConfig(config, testing.Verbose())
	defer teardown()

	msg := []byte(testhelper.Repeat("A", 500))
	for i := 0; i < 10; i++ {
		l.Write(msg)
	}
	l.Flush()

	part1, _ := ioutil.ReadFile(config.LogFile + ".9")
	testhelper.CheckGoldenFile("file_partition_delete_old_ones.9", part1, testhelper.Golden)
	part2, _ := ioutil.ReadFile(config.LogFile + ".10")
	testhelper.CheckGoldenFile("file_partition_delete_old_ones.10", part2, testhelper.Golden)
	_, err := ioutil.ReadFile(config.LogFile + ".11")
	if err == nil {
		t.Fatal("Expected no third partition")
	}
}

func fileWriteLog(t *testing.T, l Logger, id uint64, body string) (int, error) {
	l.SetID(id)
	b := protocol.NewProtocolWriter().WriteLogLine(protocol.NewMessage(id, []byte(body)))
	n, err := l.Write(b)
	if err != nil {
		t.Fatalf("unexpectedly failed to write to log: %+v", err)
		return n, err
	}
	return n, err
}

func readRange(t testing.TB, l Logger, start, end uint64) []byte {
	p, err := l.Range(1, 1)
	if err != nil {
		t.Logf("%s", debug.Stack())
		t.Fatalf("failed to read range: %+v", err)
	}

	var finalb []byte
	for p.Next() {
		if err := p.Error(); err != nil {
			t.Logf("%s", debug.Stack())
			t.Fatalf("failed to iterate partition range: %+v", err)
		}

		lf := p.LogFile()
		if b, err := ioutil.ReadAll(lf); err != nil {
			t.Logf("%s", debug.Stack())
			t.Fatalf("failed to read partition: %+v", err)
		} else {
			finalb = append(finalb, b...)
		}
	}

	return finalb
}

func removeMetaFromLog(b []byte) []byte {
	end := bytes.SplitN(b, []byte(" "), 4)[3]
	return end[:len(end)-2]
}
