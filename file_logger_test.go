package logd

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

func getTempdir() string {
	dir, err := ioutil.TempDir("", "__log")
	if err != nil {
		panic(fmt.Sprintf("Failed to get tempdir: %+v", err))
	}
	return dir
}

func setupFileLogger(t *testing.T) (*Config, Logger, func()) {
	config := testConfig(nil)
	config.IndexCursorSize = 10
	config.PartitionSize = 2048
	tmpdir := getTempdir()
	tmplog := path.Join(tmpdir, "__log")
	config.LogFile = tmplog

	var logger Logger
	logger = newFileLogger(config)
	config.Logger = newFileLogger(config)

	if err := logger.(logManager).Setup(); err != nil {
		t.Fatalf("error setting up: %+v", err)
	}

	return config, logger, func() {
		os.RemoveAll(tmpdir)

		if err := logger.(logManager).Shutdown(); err != nil {
			t.Fatalf("error shutting down: %+v", err)
		}
	}
}

func getLogOutput(config *Config, l io.Reader) []byte {
	b, err := ioutil.ReadAll(l)
	if err != nil {
		panic(err)
	}

	// var b []byte
	// scanner := newLogScanner(config, l)
	// for scanner.Scan() {
	// 	msg := scanner.Msg()
	// 	b = append(b, msg.bytes()...)
	// }
	return b
}

func TestFileLoggerCreate(t *testing.T) {
	_, _, teardown := setupFileLogger(t)
	defer teardown()
}

func TestFileLoggerWrite(t *testing.T) {
	config, logger, teardown := setupFileLogger(t)
	defer teardown()

	logger.Write([]byte("really cool message\n"))
	logger.Write([]byte("another really cool message\n"))
	logger.Write([]byte("yet another really cool message\n"))
	logger.Write([]byte("finally a last really cool message\n"))

	b, err := ioutil.ReadFile(config.LogFile + ".0")
	if err != nil {
		t.Fatalf("failed reading log: %+v", err)
	}
	checkGoldenFile(t, "file_logger_write", b, golden)
}

func TestFileLoggerReadSeek(t *testing.T) {
	config, logger, teardown := setupFileLogger(t)
	defer teardown()

	fileWriteLog(t, logger, 1, "one")
	fileWriteLog(t, logger, 2, "two")
	fileWriteLog(t, logger, 3, "three")
	fileWriteLog(t, logger, 4, "four")

	if head, err := logger.Head(); err != nil {
		t.Fatalf("failed getting head of log: %+v", err)
	} else {
		if head != 4 {
			t.Fatalf("expected head to be 4 but was %d", head)
		}
	}

	if err := logger.SeekToID(2); err != nil {
		t.Fatalf("unexpected error seeking: %+v", err)
	}

	out := getLogOutput(config, logger)
	checkGoldenFile(t, "file_logger_read_seek", out, golden)
}

func TestFileLoggerIndex(t *testing.T) {
	config, logger, teardown := setupFileLogger(t)
	defer teardown()

	for i := 1; i < 501; i++ {
		n := i
		if n > 2 {
			n /= 2
		}
		if n > 250 {
			n /= 2
		}
		fileWriteLog(t, logger, uint64(i), repeat("word", n))
	}

	out, _ := ioutil.ReadFile(config.indexFileName())
	checkGoldenFile(t, "file_logger_index", out, golden)
}

func repeat(s string, n int) string {
	b := bytes.Buffer{}
	for i := 0; i < n; i++ {
		b.WriteString(s)
		b.WriteString(" ")
	}
	return b.String()
}

func fileWriteLog(t *testing.T, logger Logger, id uint64, body string) (int, error) {
	logger.SetID(id)
	n, err := logger.Write(NewMessage(id, []byte(body)).logBytes())
	if err != nil {
		t.Fatalf("unexpectedly failed to write to log: %+v", err)
		return n, err
	}
	return n, err
}
