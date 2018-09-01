package protocol

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/testhelper"
)

func TestWriteBatch(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	testWriteBatch(t, conf, "batch.small", []string{
		"hi", "hallo", "sup",
	})
}

func TestWriteBatchSmallest(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	testWriteBatch(t, conf, "batch.smallest", []string{"0"})
}

func TestWriteBatchMedium(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
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
	conf := testhelper.DefaultConfig(testing.Verbose())
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
	conf := testhelper.DefaultConfig(testing.Verbose())
	batch := NewBatch(conf)
	testReadBatch(t, conf, "batch.small", batch)
	batch.Reset()
	testReadBatch(t, conf, "batch.small", batch)
}

func TestReadBatchSmallest(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	testRead(t, conf, "batch.smallest")
}

func TestReadBatchMedium(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	fixture := testhelper.LoadFixture("batch.medium")
	if conf.MaxBatchSize < len(fixture) {
		conf.MaxBatchSize = len(fixture) * 2
	}
	testRead(t, conf, "batch.medium")
}

func TestReadBatchLarge(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	conf.MaxBatchSize = 1024 * 1024
	conf.PartitionSize = 1024 * 1024 * 10
	testRead(t, conf, "batch.large")
}

func TestReadBatchTooLarge(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
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
	conf := testhelper.DefaultConfig(testing.Verbose())
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

func TestBatchWriteErrors(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	batch := NewBatch(conf)
	fixture := testhelper.LoadFixture("batch.small")

	b := &bytes.Buffer{}
	b.Write(fixture)
	br := bufio.NewReader(b)
	if _, err := batch.ReadFrom(br); err != nil {
		t.Fatalf("unexpected error reading batch: %v", err)
	}

	w := testhelper.NewFailingWriter()

	var errs []error
	for i := 0; i < 50; i++ {
		_, err := batch.WriteTo(w)
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) == 0 {
		t.Fatal("expected errors but got none")
	}

	// if at least one valid batch was written, that's something i guess. this
	// isn't a great check :P
	if !strings.Contains(w.String(), string(fixture)) {
		t.Fatal(w.String(), "\ndidn't contain a single valid batch")
	}
}

func TestBatchReadErrors(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	batch := NewBatch(conf)
	fixture := testhelper.LoadFixture("batch.small")
	r := testhelper.NewFailingReader(fixture)

	var errs []error
	br := bufio.NewReaderSize(r, 4)
	for i := 0; i < 100; i++ {
		batch.Reset()
		_, err := batch.ReadFrom(br)
		fmt.Println(i, err)
		if err != nil {
			if err != io.EOF {
				errs = append(errs, err)
			}
			br.Reset(r)
		} else if i > 90 {
			break
		}
	}

	if len(errs) == 0 {
		t.Fatal("expected errors but got none")
	}

	if batch.Size != 37 {
		t.Fatal("expected batch size 37 but got:", batch.Size)
	}
	if batch.Messages != 3 {
		t.Fatal("expected 3 messages but got:", batch.Messages)
	}
}

var invalidBatches = map[string][]byte{
	// "valid":  []byte("BATCH 30 default 1362320750 3\r\nMSG 1\r\nA\r\nMSG 1\r\nB\r\nMSG 1\r\nC\r\n"),
	// "space before newline":  []byte("BATCH 30 default 1362320750 3 \r\nMSG 1\r\nA\r\nMSG 1\r\nB\r\nMSG 1\r\nC\r\n"),
	"bad crc":               []byte("BATCH 30 default 1362320751 3\r\nMSG 1\r\nA\r\nMSG 1\r\nB\r\nMSG 1\r\nC\r\n"),
	"bad crc2":              []byte("BATCH 30 default 1362320750 3\r\nMSG 1\r\na\r\nMSG 1\r\nB\r\nMSG 1\r\nC\r\n"),
	"small body length":     []byte("BATCH 30 default 1362320750 3\r\nMSG 1\r\n\r\nMSG 1\r\nB\r\nMSG 1\r\nC\r\n"),
	"no final newline":      []byte("BATCH 30 default 1362320750 3\r\nMSG 1\r\n\r\nMSG 1\r\nB\r\nMSG 1\r\nC"),
	"no final newline2":     []byte("BATCH 30 default 1362320750 3\r\nMSG 1\r\n\r\nMSG 1\r\nB\r\nMSG 1\r\nC\r"),
	"no final newline3":     []byte("BATCH 30 default 1362320750 3\r\nMSG 1\r\n\r\nMSG 1\r\nB\r\nMSG 1\r\nC\n"),
	"wrong size":            []byte("BATCH 29 default 1362320750 3\r\nMSG 1\r\nA\r\nMSG 1\r\nB\r\nMSG 1\r\nC\r\n"),
	"invalid size":          []byte("BATCH a default 1362320750 3\r\nMSG 1\r\nA\r\nMSG 1\r\nB\r\nMSG 1\r\nC\r\n"),
	"invalid size2":         []byte("BATCH cool default 1362320750 3\r\nMSG 1\r\nA\r\nMSG 1\r\nB\r\nMSG 1\r\nC\r\n"),
	"wrong command":         []byte("BOTCH 30 default 1362320750 3\r\nMSG 1\r\nA\r\nMSG 1\r\nB\r\nMSG 1\r\nC\r\n"),
	"wrong msg size":        []byte("BATCH 30 default 1362320750 3\r\nMSG 1\r\nA\r\nMSG 2\r\nB\r\nMSG 1\r\nC\r\n"),
	"invalid num messages":  []byte("BATCH 30 default 1362320750 a\r\nMSG 1\r\nA\r\nMSG 1\r\nB\r\nMSG 1\r\nC\r\n"),
	"invalid num messages2": []byte("BATCH 30 default 1362320750 cool\r\nMSG 1\r\nA\r\nMSG 1\r\nB\r\nMSG 1\r\nC\r\n"),
	"invalid crc":           []byte("BATCH 30 default dang 3\r\nMSG 1\r\nA\r\nMSG 1\r\nB\r\nMSG 1\r\nC\r\n"),
	"missing data":          []byte("BATCH 30 default 1362320750 3\r\n"),
}

func TestBatchInvalid(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	batch := NewBatch(conf)

	for name, b := range invalidBatches {
		t.Run(name, func(t *testing.T) {
			batch.Reset()
			_, err := batch.ReadFrom(bufio.NewReader(bytes.NewBuffer(b)))
			verr := batch.Validate()
			if err == nil && verr == nil {
				t.Fatalf("%s case: batch should not have been valid\n%q\n", name, b)
			}

			batch.Reset()
			req := NewRequestConfig(conf)
			_, err = req.ReadFrom(bufio.NewReader(bytes.NewBuffer(b)))
			_, rerr := batch.FromRequest(req)
			if err == nil && rerr == nil {
				t.Fatalf("%s case: batch should not have been a valid request\n%q\n", name, b)
			}
		})
	}
}

func TestBatchWriteTooLarge(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	conf.MaxBatchSize = 10
	batch := NewBatch(conf)
	batch.Append([]byte("AAAAAAAAAAAA"))
	b := &bytes.Buffer{}

	if _, err := batch.WriteTo(b); err == nil {
		t.Fatal("expected err but got none")
	}
}

func TestBatchReadTooLarge(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	batch := NewBatch(conf)
	fixture := testhelper.LoadFixture("batch.small")

	if _, err := batch.ReadFrom(bufio.NewReader(bytes.NewBuffer(fixture))); err != nil {
		t.Fatal(err)
	}

	conf.MaxBatchSize = 10
	if err := batch.Validate(); err == nil {
		t.Fatal("expected error but got none")
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

	if err := batch.Validate(); err != nil {
		t.Fatalf("validation error: %+v", err)
	}

	b = &bytes.Buffer{}
	if _, err := batch.WriteTo(b); err != nil {
		t.Fatalf("unexpected error writing batch: %v", err)
	}

	actual := b.Bytes()
	testhelper.CheckGoldenFile(fixtureName, actual, testhelper.Golden)
}
