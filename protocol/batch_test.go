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
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	testWriteBatch(t, conf, "batch.small", []string{
		"hi", "hallo", "sup",
	})
}

func TestWriteBatchSmallest(t *testing.T) {
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	testWriteBatch(t, conf, "batch.smallest", []string{"0"})
}

func TestWriteBatchMedium(t *testing.T) {
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	fixture := testhelper.LoadFixture("batch.medium")
	if conf.MaxBatchSize < len(fixture) {
		conf.MaxBatchSize = len(fixture) * 2
	}

	testWriteBatch(t, conf, "batch.medium", []string{
		string(testhelper.SomeLines[0]),
		string(testhelper.SomeLines[1]),
		string(testhelper.SomeLines[2]),
	})
}

func TestWriteBatchLarge(t *testing.T) {
	conf := testhelper.DefaultTestConfig(testing.Verbose())
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
	batch.SetTopic([]byte("default"))
	for _, arg := range args {
		batch.AppendMessage(newTestMessage(conf, arg))
	}

	b := &bytes.Buffer{}
	if _, err := batch.WriteTo(b); err != nil {
		t.Fatalf("unexpected error writing batch: %v", err)
	}
	testhelper.CheckGoldenFile(goldenFileName, b.Bytes(), testhelper.Golden)

	batch.Reset()
	batch.SetTopic([]byte("default"))
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
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	batch := NewBatch(conf)
	testReadBatch(t, conf, "batch.small", batch)
	batch.Reset()
	testReadBatch(t, conf, "batch.small", batch)
}

func TestReadBatchSmallest(t *testing.T) {
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	testRead(t, conf, "batch.smallest")
}

func TestReadBatchMedium(t *testing.T) {
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	fixture := testhelper.LoadFixture("batch.medium")
	if conf.MaxBatchSize < len(fixture) {
		conf.MaxBatchSize = len(fixture) * 2
	}
	testRead(t, conf, "batch.medium")
}

func TestReadBatchLarge(t *testing.T) {
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	conf.MaxBatchSize = 1024 * 1024
	conf.PartitionSize = 1024 * 1024 * 10
	testRead(t, conf, "batch.large")
}

func TestReadBatchTooLarge(t *testing.T) {
	conf := testhelper.DefaultTestConfig(testing.Verbose())
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

func TestScanBatches(t *testing.T) {
	conf := testhelper.DefaultTestConfig(testing.Verbose())
	data := testhelper.LoadFixture("batch.small")
	fixture := []byte{}

	nbatches := 3
	for i := 0; i < nbatches; i++ {
		fixture = append(fixture, data...)
	}

	b := &bytes.Buffer{}
	b.Write(data)
	expectedBatch := NewBatch(conf)
	if _, err := expectedBatch.ReadFrom(bufio.NewReader(b)); err != nil {
		t.Fatal("unexpected error reading batch")
	}

	b.Reset()
	b.Write(fixture)
	scanner := NewBatchScanner(conf, b)

	for i := 0; i < nbatches; i++ {
		if !scanner.Scan() {
			t.Fatalf("expected to read another batch (read %d)", i+1)
		}

		batch := scanner.Batch()
		if batch.Size != expectedBatch.Size {
			t.Fatalf("expected batch size to be %d but was %d", expectedBatch.Size, batch.Size)
		}
		if batch.Checksum != expectedBatch.Checksum {
			t.Fatalf("expected batch checksum to be %d but was %d", expectedBatch.Checksum, batch.Checksum)
		}
		if batch.Messages != expectedBatch.Messages {
			t.Fatalf("expected batch message count to be %d but was %d", expectedBatch.Messages, batch.Messages)
		}

		if !bytes.Equal(batch.Bytes(), expectedBatch.Bytes()) {
			t.Fatalf("expected:\n\t%q\nbut got:\n\t%q", expectedBatch.Bytes(), batch.Bytes())
		}
	}

	if scanner.Scan() {
		t.Fatalf("didn't expect another batch")
	}
}

func testRead(t *testing.T, conf *config.Config, fixtureName string) {
	testReadBatch(t, conf, fixtureName, NewBatch(conf))
}

func testReadBatch(t *testing.T, conf *config.Config, fixtureName string, batch *Batch) {
	fixture := testhelper.LoadFixture(fixtureName)
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
