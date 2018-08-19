package events

import (
	"bytes"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/testhelper"
)

type startupTCParts map[uint64][]byte
type startupTCTopics map[string]startupTCParts

type startupTestCase struct {
	conf           *config.Config
	topics         startupTCTopics
	expectedTopics startupTCTopics
	err            error
}

func TestStartup(t *testing.T) {
	fixture := testhelper.LoadFixture("batch.small")
	var testCases = map[string]startupTestCase{
		"valid": {
			conf: testhelper.DefaultTestConfig(testing.Verbose()),
			topics: startupTCTopics{
				"default": startupTCParts{0: fixture},
			},
			expectedTopics: startupTCTopics{
				"default": startupTCParts{0: fixture},
			},
		},
		"empty": {
			conf: testhelper.DefaultTestConfig(testing.Verbose()),
			topics: startupTCTopics{
				"default": startupTCParts{0: []byte{}},
			},
			expectedTopics: startupTCTopics{
				"default": startupTCParts{0: []byte{}},
			},
		},
		"trailing zero": {
			conf: testhelper.DefaultTestConfig(testing.Verbose()),
			topics: startupTCTopics{
				"default": startupTCParts{0: append(fixture, '0')},
			},
			expectedTopics: startupTCTopics{
				"default": startupTCParts{0: fixture},
			},
		},
		"leading zero": {
			conf: testhelper.DefaultTestConfig(testing.Verbose()),
			topics: startupTCTopics{
				"default": startupTCParts{0: append([]byte{0}, fixture...)},
			},
			expectedTopics: startupTCTopics{
				"default": startupTCParts{0: []byte{}},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			writePartitionFiles(tc.conf, tc.topics)
			q := newEventQ(tc.conf)
			t.Logf("starting event queue with config: %+v", tc.conf)

			err := q.GoStart()
			if err != tc.err {
				t.Fatalf("expected err %v but got %+v", tc.err, err)
			}

			checkPartitionFiles(t, tc.conf, tc.expectedTopics)

			err = q.Stop()
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func writePartitionFiles(conf *config.Config, topics startupTCTopics) {
	for topic, parts := range topics {
		for off, b := range parts {
			if err := os.MkdirAll(path.Join(conf.WorkDir, topic), 0755); err != nil {
				panic(err)
			}
			p := partitionFullPath(conf, topic, off)
			f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				panic(err)
			}
			defer f.Close()

			if _, err := f.Write(b); err != nil {
				panic(err)
			}
			f.Close()
		}
	}
}

func checkPartitionFiles(t *testing.T, conf *config.Config, topics startupTCTopics) {
	if topics == nil {
		return
	}

	for topic, parts := range topics {
		for off, b := range parts {
			p := partitionFullPath(conf, topic, off)
			if _, err := os.Stat(p); err != nil {
				t.Fatal(err)
			}

			f, err := os.Open(p)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			if b == nil {
				t.Fatalf("expected no file for partition %d", off)
			}

			buf := make([]byte, len(b))
			if _, err := io.ReadFull(f, buf); err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(b, buf) {
				t.Fatalf("expected partition %s/%d to be:\n\n\t%q\n\nbut was:\n\n\t%q", topic, off, b, buf)
			}
			f.Close()
		}
	}
}

func partitionPath(conf *config.Config, topic string, off uint64) string {
	_, prefix := filepath.Split(conf.WorkDir)
	return path.Join(prefix, topic, strconv.FormatUint(off, 10)+".log")
}

func partitionFullPath(conf *config.Config, topic string, off uint64) string {
	dir, _ := filepath.Split(conf.WorkDir)
	return filepath.Join(dir, partitionPath(conf, topic, off))
}
