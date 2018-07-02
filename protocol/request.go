package protocol

import (
	"bufio"
	"bytes"
	"io"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
	"github.com/pkg/errors"
)

// Request represents a single request / response.
type Request struct {
	conf      *config.Config
	Name      CmdType
	responseC chan *Response

	respBuf *closingBuffer

	raw      []byte   // the full request as raw bytes
	read     int64    //
	envelope []byte   // slice of raw pointing to the first line of the request
	args     [][]byte // slices of raw pointing to the requests arguments
	nargs    int      //
	body     []byte   // slice of raw pointing to the body, if it exists
	bodysize int      //
}

// NewRequest returns a new instance of *Request
func NewRequest(conf *config.Config) *Request {
	return &Request{
		conf:      conf,
		raw:       make([]byte, conf.MaxBatchSize),
		responseC: make(chan *Response),
		args:      make([][]byte, maxArgs),
		respBuf:   newClosingBuffer(),
	}
}

// Reset sets the request to its initial values
func (req *Request) Reset() {
	req.Name = 0
	req.read = 0
	req.envelope = nil
	req.nargs = 0
	req.body = nil
	req.bodysize = 0
	req.respBuf.Reset()
}

func (req *Request) String() string {
	return req.Name.String()
}

// Bytes returns the raw byte representation of the request
func (req *Request) Bytes() []byte {
	return req.raw[:req.read]
}

// FullSize returns the total byte size of the request
func (req *Request) FullSize() int {
	return int(req.read)
}

func (req *Request) parseType() ([]byte, error) {
	rest, word, err := parseWord(req.envelope)
	req.Name = cmdNamefromBytes(word)
	return rest, err
}

func (req *Request) parseArg(line []byte) ([]byte, error) {
	rest, word, err := parseWord(line)
	if err != nil {
		return rest, err
	}

	req.args[req.nargs] = word
	req.nargs++
	return rest, nil
}

func (req *Request) hasBody() bool {
	switch req.Name {
	case CmdBatch:
		return true
	}
	return false
}

// if hasBody, the first arg is always the size
func (req *Request) readBody(r *bufio.Reader, pos int64) (int64, error) {
	n, err := asciiToInt(req.args[0])
	if err != nil {
		return 0, err
	}
	req.bodysize = int(n)

	// internal.Debugf(req.conf, "body size: %d bytes (total %d)", req.bodysize, int64(req.bodysize)+pos)
	if int64(req.bodysize)+pos > int64(req.conf.MaxBatchSize) {
		return 0, errTooLarge
	}

	// fmt.Println(pos, req.bodysize, pos+int64(req.bodysize))
	// fmt.Printf("%q\n", req.raw)
	read, err := io.ReadFull(r, req.raw[pos:pos+int64(req.bodysize)])
	if err != nil {
		return int64(read), err
	}

	req.body = req.raw[pos : int(pos)+req.bodysize]
	return int64(read), err
}

// ReadFrom implements io.ReaderFrom
func (req *Request) ReadFrom(r io.Reader) (int64, error) {
	n, err := req.readFromBuf(r.(*bufio.Reader))
	req.read = n
	return n, err
}

// Respond sends a Response over the channel back to the conn goroutine
func (req *Request) Respond(resp *Response) {
	req.responseC <- resp
}

// Responded returns a channel that a response will be passed to. the event
// handler uses this to pass messages to the conn goroutines.
func (req *Request) Responded() chan *Response {
	return req.responseC
}

func (req *Request) readEnvelope(r *bufio.Reader) (int64, error) {
	total, _, raw, err := readLineFromBuf(r)
	copy(req.raw, raw)
	req.envelope = req.raw[:total]
	return total, err
}

func (req *Request) readFromBuf(r *bufio.Reader) (int64, error) {
	total, err := req.readEnvelope(r)
	if err != nil {
		return total, err
	}

	line, err := req.parseType()
	if err != nil {
		return total, err
	}

	if req.Name == 0 { // unknown command
		return total, ErrInvalid
	}

	expectedArgs, ok := argLens[req.Name]
	if !ok {
		return total, errors.Errorf("type %v has no specified length", req.Name)
	}

	for i := 0; i < expectedArgs; i++ {
		line, err = req.parseArg(line)
		if err != nil {
			return total, err
		}
	}

	// internal.Debugf(req.conf, "read envelope: %d bytes", total)
	if req.hasBody() {
		n, berr := req.readBody(r, total)
		total += n
		if berr != nil && berr != io.EOF {
			return total, berr
		}
	}

	return total, err
}

// WriteResponse is used by the event loop to write a single response. Should
// be used for all commands except READ. It can be used for the READ response
// envelope.
func (req *Request) WriteResponse(resp *Response, cr *ClientResponse) (int64, error) {
	n, err := cr.WriteTo(req.respBuf)
	internal.LogError(resp.AddReader(req.respBuf))
	return n, err
}

type closingBuffer struct {
	*bytes.Buffer
}

func newClosingBuffer() *closingBuffer {
	return &closingBuffer{
		Buffer: &bytes.Buffer{},
	}
}

func (c *closingBuffer) Close() error {
	return nil
}
