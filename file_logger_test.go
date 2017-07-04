package logd

import (
	"fmt"
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

func getLogOutput(config *Config, l Logger) []byte {
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

	logger.Write(NewMessage(1, []byte("one")).bytes())
	logger.Write(NewMessage(2, []byte("two")).bytes())
	logger.Write(NewMessage(3, []byte("three")).bytes())
	logger.Write(NewMessage(4, []byte("four")).bytes())

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
	fmt.Printf("%q\n", out)

	// checkGoldenFile(t, "file_logger_read_seek", b, golden)
}
