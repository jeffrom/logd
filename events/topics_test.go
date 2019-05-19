package events

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"

	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/testhelper"
)

func TestTopics(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	q := NewHandlers(conf)
	doStartHandler(t, q)
	defer doShutdownHandler(t, q)

	names := []string{"cool", "sup", "oknice"}
	msgs := [][]byte{[]byte("aaa"), []byte("bbb"), []byte("ccc")}
	for i, name := range names {
		msg := msgs[i]
		b := protocol.NewBatch(conf)
		b.SetTopic([]byte(name))
		b.Append(msg)

		buf := &bytes.Buffer{}
		if _, err := b.WriteTo(buf); err != nil {
			t.Fatal(err)
		}

		cr := pushBatch(t, q, buf.Bytes())
		if cr.Offset() != 0 {
			t.Fatalf("expect 0 offset but got %d", cr.Offset())
		}
		if err := cr.Error(); err != nil {
			t.Fatal(err)
		}

		readResp := pushReadTopic(t, q, name, 0, 1)
		if !bytes.HasPrefix(readResp, []byte("OK 0 1\r\n")) {
			t.Fatalf("expected 'OK 0 1\r\n' prefix but got: %q", readResp)
		}
		readResp = bytes.TrimPrefix(readResp, []byte("OK 0 1\r\n"))
		b.Reset()
		if _, err := b.ReadFrom(bufio.NewReader(bytes.NewReader(readResp))); err != nil {
			t.Fatal(err)
		}

		if b.Messages != 1 {
			t.Fatalf("expected 1 message batch but got %d", b.Messages)
		}
		expect := []byte(fmt.Sprintf("MSG %d\r\n%s\r\n", len(msg), msg))
		if !bytes.Equal(b.MessageBytes(), expect) {
			t.Fatalf("expected message %q but got %q", expect, b.MessageBytes())
		}
	}
}

func TestTopicConfigMaxTopics(t *testing.T) {
	// new topics not allowed > the limit

	conf := testhelper.DefaultConfig(testing.Verbose())
	conf.MaxTopics = 1
	q := NewHandlers(conf)
	doStartHandler(t, q)
	defer doShutdownHandler(t, q)

	b := protocol.NewBatch(conf)
	b.SetTopic([]byte("default"))
	b.Append([]byte("woooo"))
	buf := &bytes.Buffer{}
	if _, err := b.WriteTo(buf); err != nil {
		t.Fatal(err)
	}

	cr := pushBatch(t, q, buf.Bytes())
	if err := cr.Error(); err != nil {
		t.Fatal(err)
	}

	b = protocol.NewBatch(conf)
	b.SetTopic([]byte("toomanytopics"))
	b.Append([]byte("woooo"))
	buf = &bytes.Buffer{}
	if _, err := b.WriteTo(buf); err != nil {
		t.Fatal(err)
	}

	cr = pushBatch(t, q, buf.Bytes())
	if err := cr.Error(); err != protocol.ErrMaxTopics {
		t.Fatal("expected max topic error but got:", err)
	}
}

func TestTopicConfigTopicWhitelist(t *testing.T) {
	// new topics not allowed if not in whitelist
	// infinite new topics if whitelist is empty
	conf := testhelper.DefaultConfig(testing.Verbose())
	conf.TopicWhitelist = []string{"allowedtopic"}
	q := NewHandlers(conf)
	doStartHandler(t, q)
	defer doShutdownHandler(t, q)

	b := protocol.NewBatch(conf)
	b.SetTopic([]byte("allowedtopic"))
	b.Append([]byte("woooo"))
	buf := &bytes.Buffer{}
	if _, err := b.WriteTo(buf); err != nil {
		t.Fatal(err)
	}

	cr := pushBatch(t, q, buf.Bytes())
	if err := cr.Error(); err != nil {
		t.Fatal(err)
	}

	b = protocol.NewBatch(conf)
	b.SetTopic([]byte("notallowed"))
	b.Append([]byte("woooo"))
	buf = &bytes.Buffer{}
	if _, err := b.WriteTo(buf); err != nil {
		t.Fatal(err)
	}

	cr = pushBatch(t, q, buf.Bytes())
	if err := cr.Error(); err != protocol.ErrTopicNotAllowed {
		t.Fatal("expected topic not allowed but got:", err)
	}
}
