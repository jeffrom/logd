package protocol

import (
	"bufio"
	"bytes"
	"io"
	"strconv"
	"time"

	"github.com/jeffrom/logd/config"
)

var bhostport = []byte("Hostport: ")
var btimeout = []byte("Timeout: ")
var bidletimeout = []byte("IdleTimeout: ")
var bmaxbatchsize = []byte("MaxBatchSize: ")

// ConfigResponse is a representation of the server-side config which is
// intended as a client multi ok response.
type ConfigResponse struct {
	conf     *config.Config
	size     int
	b        *bytes.Buffer
	cached   bool
	readConf *config.Config
}

func NewConfigResponse(conf *config.Config) *ConfigResponse {
	return &ConfigResponse{
		conf:     conf,
		b:        &bytes.Buffer{},
		readConf: config.New(),
	}
}

// Reset sets the ConfigResponse to its initial values
func (cr *ConfigResponse) Reset() {
	cr.cached = false
	cr.size = 0
	cr.b.Reset()

	cr.readConf.Host = ""
	cr.readConf.Timeout = 0
	cr.readConf.IdleTimeout = 0
	cr.readConf.MaxBatchSize = 0
}

// MultiResponse returns a server-side MOK response body
func (cr *ConfigResponse) MultiResponse() []byte {
	if cr.cached {
		return cr.b.Bytes()
	}

	_, err := cr.WriteTo(cr.b)
	if err != nil {
		cr.b.Reset()
		return nil
	}

	cr.cached = true
	return cr.b.Bytes()
}

// WriteTo implements io.WriterTo interface.
func (cr *ConfigResponse) WriteTo(w io.Writer) (int64, error) {
	var total int64

	n, err := w.Write(bhostport)
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write([]byte(cr.conf.Host))
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(bnewLine)
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(btimeout)
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write([]byte(cr.conf.Timeout.String()))
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(bnewLine)
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(bidletimeout)
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write([]byte(cr.conf.IdleTimeout.String()))
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(bnewLine)
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(bmaxbatchsize)
	total += int64(n)
	if err != nil {
		return total, err
	}

	// TODO don't need to alloc here
	n, err = w.Write([]byte(strconv.Itoa(cr.conf.MaxBatchSize)))
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(bnewLine)
	total += int64(n)
	if err != nil {
		return total, err
	}

	return total, nil
}

// Parse reads and returns a config struct from a byte slice
func (cr *ConfigResponse) Parse(b []byte) error {
	// TODO do this more efficiently
	if _, err := cr.readFromBuf(bufio.NewReader(bytes.NewBuffer(b))); err != nil {
		return err
	}

	return nil
}

// ReadFrom implements io.ReaderFrom interface.
func (cr *ConfigResponse) ReadFrom(r io.Reader) (int64, error) {
	return cr.readFromBuf(r.(*bufio.Reader))
}

func (cr *ConfigResponse) readFromBuf(r *bufio.Reader) (int64, error) {
	var total int64

	// TODO 4 shouldn't be a magic number. should be the total number of config
	// fields.
	for i := 0; i < 4; i++ {
		kb, err := r.ReadSlice(' ')
		total += int64(len(kb))
		if err != nil {
			return total, err
		}

		n, vb, _, err := readLineFromBuf(r)
		total += int64(n)
		if err != nil {
			return total, err
		}

		switch string(kb) {
		case "Hostport: ":
			cr.readConf.Host = string(vb)
		case "Timeout: ":
			dur, err := time.ParseDuration(string(vb))
			if err != nil {
				return total, err
			}
			cr.readConf.Timeout = dur
		case "IdleTimeout: ":
			dur, err := time.ParseDuration(string(vb))
			if err != nil {
				return total, err
			}
			cr.readConf.IdleTimeout = dur
		case "MaxBatchSize: ":
			batchSize, err := strconv.Atoi(string(vb))
			if err != nil {
				return total, err
			}
			cr.readConf.MaxBatchSize = batchSize
		default:
			return total, errInvalidProtocolLine
		}
	}

	return total, nil
}

// Config returns the most recently read config.
func (cr *ConfigResponse) Config() *config.Config {
	return cr.readConf
}
