package protocol

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/testhelper"
)

func TestWriteBatch(t *testing.T) {
	conf := testhelper.TestConfig(testing.Verbose())
	testWriteBatch(t, conf, "batch.small", []string{
		"hi", "hallo", "sup",
	})
}

func TestWriteBatchSmallest(t *testing.T) {
	conf := testhelper.TestConfig(testing.Verbose())
	testWriteBatch(t, conf, "batch.smallest", []string{"0"})
}

func TestWriteBatchMedium(t *testing.T) {
	conf := testhelper.TestConfig(testing.Verbose())
	testWriteBatch(t, conf, "batch.medium", []string{
		string(testhelper.SomeLines[0]),
		string(testhelper.SomeLines[1]),
		string(testhelper.SomeLines[2]),
	})
}

func TestWriteBatchLarge(t *testing.T) {
	conf := testhelper.TestConfig(testing.Verbose())
	conf.MaxBatchSize = 1024 * 1024
	conf.PartitionSize = 1024 * 1024 * 10

	fixture, err := ioutil.ReadFile("../testdata/work_of_art.txt")
	if err != nil {
		panic(err)
	}

	lines := bytes.Split(fixture, []byte("\n"))
	var check []string
	for _, line := range lines {
		if len(line) < 2 {
			continue
		}
		check = append(check, string(line))
	}

	testWriteBatch(t, conf, "batch.large", check)
}

func testWriteBatch(t *testing.T, conf *config.Config, goldenFileName string, args []string) {
	batch := NewBatch(conf)
	for _, arg := range args {
		batch.AppendMessage(newTestMessage(conf, arg))
	}

	b := &bytes.Buffer{}
	if _, err := batch.WriteTo(b); err != nil {
		t.Fatalf("unexpected error writing batch: %v", err)
	}
	testhelper.CheckGoldenFile(goldenFileName, b.Bytes(), testhelper.Golden)

	batch.Reset()
	b.Reset()
	for _, arg := range args {
		batch.Append([]byte(arg))
	}

	if _, err := batch.WriteTo(b); err != nil {
		t.Fatalf("unexpected error writing batch: %v", err)
	}
	testhelper.CheckGoldenFile(goldenFileName, b.Bytes(), testhelper.Golden)
}

func TestReadBatch(t *testing.T) {
	conf := testhelper.TestConfig(testing.Verbose())
	testReadBatch(t, conf, "batch.small")
}

func TestReadBatchSmallest(t *testing.T) {
	conf := testhelper.TestConfig(testing.Verbose())
	testReadBatch(t, conf, "batch.smallest")
}

func TestReadBatchMedium(t *testing.T) {
	conf := testhelper.TestConfig(testing.Verbose())
	testReadBatch(t, conf, "batch.medium")
}

func TestReadBatchLarge(t *testing.T) {
	conf := testhelper.TestConfig(testing.Verbose())
	conf.MaxBatchSize = 1024 * 1024
	conf.PartitionSize = 1024 * 1024 * 10
	testReadBatch(t, conf, "batch.large")
}

func TestReadBatchTooLarge(t *testing.T) {
	conf := testhelper.TestConfig(testing.Verbose())
	conf.MaxBatchSize = 1024
	fixture := testhelper.LoadFixture("batch.large")
	batch := NewBatch(conf)
	b := &bytes.Buffer{}
	b.Write(fixture)
	br := bufio.NewReader(b)

	if _, err := batch.ReadFrom(br); err != errTooLarge {
		t.Fatalf("expected %s but got: %+v", errTooLarge, err)
	}
}

func testReadBatch(t *testing.T, conf *config.Config, fixtureName string) {
	fixture := testhelper.LoadFixture(fixtureName)
	batch := NewBatch(conf)
	b := &bytes.Buffer{}
	b.Write(fixture)
	br := bufio.NewReader(b)

	if _, err := batch.ReadFrom(br); err != nil {
		t.Fatalf("unexpected error reading batch: %v", err)
	}

	b = &bytes.Buffer{}
	if _, err := batch.WriteTo(b); err != nil {
		t.Fatalf("unexpected error writing batch: %v", err)
	}

	actual := b.Bytes()
	testhelper.CheckGoldenFile(fixtureName, actual, testhelper.Golden)
}
