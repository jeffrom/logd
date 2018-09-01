package protocol

import (
	"bufio"
	"bytes"
	"strings"
	"testing"

	"github.com/jeffrom/logd/testhelper"
)

func TestConfigRequest(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	req := NewRequest(conf)
	cr := NewConfigRequest(conf)
	fixture := []byte("CONFIG\r\n")
	buf := &bytes.Buffer{}

	_, err := req.ReadFrom(bufio.NewReader(bytes.NewBuffer(fixture)))
	if err != nil {
		t.Fatal(err)
	}

	_, err = cr.FromRequest(req)
	if err != nil {
		t.Fatal(err)
	}

	_, err = cr.WriteTo(buf)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(fixture, buf.Bytes()) {
		t.Fatalf("expected:\n\n\t%q\n\nbut got:\n\n\t%q", fixture, buf.Bytes())
	}
}

var invalidConfigRequests = map[string][]byte{
	"no newline":    []byte("CONFIG\r"),
	"no newline2":   []byte("CONFIG"),
	"leading space": []byte(" CONFIG\r\n"),

	// TODO fix
	// "extra args":    []byte("CONFIG sup\r\n"),
	// "space before newline": []byte("CONFIG \r\n"),
}

func TestConfigRequestInvalid(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	cr := NewConfigRequest(conf)

	for name, b := range invalidConfigRequests {
		t.Run(name, func(t *testing.T) {
			cr.Reset()
			req := NewRequest(conf)
			_, err := req.ReadFrom(bufio.NewReader(bytes.NewBuffer(b)))
			_, rerr := cr.FromRequest(req)
			if err == nil && rerr == nil {
				t.Fatalf("%s case: close request should not have been valid\n%q\n", name, b)
			}

			confResp := NewConfigResponse(conf)
			if perr := confResp.Parse(b); perr == nil {
				t.Fatalf("%s case: close request should not have been parsed without error\n%q\n", name, b)
			}
		})
	}
}

func TestConfigRequestWriteErrors(t *testing.T) {
	conf := testhelper.DefaultConfig(testing.Verbose())
	cr := NewConfigRequest(conf)
	fixture := []byte("CONFIG\r\n")

	w := testhelper.NewFailingWriter()

	var errs []error
	for i := 0; i < 50; i++ {
		_, err := cr.WriteTo(w)
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) == 0 {
		t.Fatal("expected errors but got none")
	}

	// if at least one valid CONFIG\r\n was written, that's something i guess. this
	// isn't a great check :P
	if !strings.Contains(w.String(), string(fixture)) {
		t.Fatal(w.String(), "\ndidn't contain a single valid close request")
	}
}
