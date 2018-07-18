package logger

import (
	"fmt"
	"testing"

	"github.com/jeffrom/logd/testhelper"
)

func TestTopics(t *testing.T) {
	topics := NewTopics(testhelper.TestConfig(testing.Verbose()))
	t.Logf("starting config: %+v", topics.conf)
	if err := topics.Setup(); err != nil {
		t.Fatal(err)
	}

	l, err := topics.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(l) != 0 {
		t.Fatalf("expected 0 length topics list but got %v", l)
	}

	names := []string{"cool", "sup", "hi"}
	for i, name := range names {
		if err := topics.Create(name); err != nil {
			t.Fatal(err)
		}

		l, err := topics.List()
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println(l)
		if len(l) < i+1 {
			t.Fatalf("expected > %d partitions, but got %v", i, l)
		}
	}

	for i, name := range names {
		if err := topics.Remove(name); err != nil {
			t.Fatal(err)
		}

		l, err := topics.List()
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println(l, len(names)-(i+1))
		if len(l) > len(names)-(i+1) {
			t.Fatalf("expected > %d partitions, but got %v", i, l)
		}
	}
}
