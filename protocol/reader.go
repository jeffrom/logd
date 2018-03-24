package protocol

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
	"github.com/pkg/errors"
)

// ProtocolReader reads commands and responses
// turns a []byte of socket data into a *response or *ReadResponse
type ProtocolReader struct {
	config *config.Config
	Br     *bufio.Reader
}

// NewProtocolReader readers an instance of a protocol reader
func NewProtocolReader(config *config.Config) *ProtocolReader {
	return &ProtocolReader{
		config: config,
		Br:     bufio.NewReaderSize(bytes.NewReader([]byte("")), 1024*8),
	}
}

func (pr *ProtocolReader) ReadCommand(r io.Reader) (*Command, error) {
	pr.Br.Reset(r)

	line, err := ReadLine(pr.Br)
	if err != nil {
		return nil, err
	}
	internal.Debugf(pr.config, "read(raw): %q", line)

	parts := bytes.SplitN(line, []byte(" "), 2)
	if len(parts) != 2 {
		return nil, errors.New("Badly formatted command")
	}

	name := cmdNamefromBytes(parts[0])
	numArgs, err := strconv.ParseInt(string(parts[1]), 10, 16)
	if err != nil {
		return nil, err
	}

	var args [][]byte
	// TODO read args efficiently
	for i := 0; i < int(numArgs); i++ {
		line, err = ReadLine(pr.Br)
		if err != nil {
			return nil, err
		}
		internal.Debugf(pr.config, "read arg(raw): %q", internal.Prettybuf(line))

		parts = bytes.SplitN(line, []byte(" "), 2)
		if len(parts) != 2 {
			return nil, errors.New("Badly formatted argument")
		}

		_, err := ParseNumber(parts[0])
		if err != nil {
			return nil, errors.New("Badly formatted argument length")
		}

		arg := parts[1]

		args = append(args, arg)
	}

	return NewCommand(pr.config, name, args...), nil
}

// ReadResponse reads a READ command response from an io.Reader
func (pr *ProtocolReader) ReadResponse(r io.Reader) (*Response, error) {
	pr.Br.Reset(r)
	line, err := ReadLine(pr.Br)
	if err != nil {
		return nil, err
	}

	parts := bytes.SplitN(line, []byte(" "), 2)
	var resp *Response
	if bytes.Equal(parts[0], []byte("OK")) {
		resp = NewResponse(pr.config, RespOK)
		if len(parts) > 1 {
			var n uint64

			subParts := bytes.SplitN(parts[1], []byte(" "), 2)

			_, err := fmt.Sscanf(string(subParts[0]), "%d", &n)
			if err != nil {
				return nil, errors.Wrap(err, "failed to parse response id or body length")
			}

			if len(subParts) > 1 { // this is an OK string response. ie from STATS

				rest := bytes.Join(subParts[1:], []byte(" "))
				rest = append(rest, []byte("\r\n")...)
				if uint64(len(rest)) < n {
					restBytes := make([]byte, n-uint64(len(rest)))
					if _, err := io.ReadFull(pr.Br, restBytes); err != nil {
						return nil, errors.Wrap(err, "failed to read OK body")
					}

					rest = append(rest, restBytes...)
				}

				resp.Body = rest
				if n != uint64(len(rest)) {
					return resp, errors.Wrap(errInvalidBodyLength, "failed to read OK body")
				}
			} else { // just want the ID
				resp.ID = n
			}
		}
	} else if bytes.Equal(parts[0], []byte("+EOF")) {
		resp = NewResponse(pr.config, RespEOF)
	} else if bytes.Equal(parts[0], []byte("ERR")) {
		var arg []byte
		if len(parts) > 1 {
			arg = parts[1]
		}
		resp = NewErrResponse(pr.config, arg)
	} else if bytes.Equal(parts[0], []byte("ERR_CLIENT")) {
		resp = NewClientErrResponse(pr.config, parts[1])
	} else {
		internal.Debugf(pr.config, "invalid response: %q", line)
		return nil, errors.New("Invalid response")
	}

	return resp, nil
}
