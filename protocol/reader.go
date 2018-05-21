package protocol

import (
	"bufio"
	"bytes"
	"io"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
	"github.com/pkg/errors"
)

// Reader reads commands and responses
// turns a []byte of socket data into a *response or *ReadResponse
type Reader struct {
	config *config.Config
	Br     *bufio.Reader
}

// NewReader readers an instance of a protocol reader
func NewReader(config *config.Config) *Reader {
	return &Reader{
		config: config,
		Br:     bufio.NewReaderSize(bytes.NewReader([]byte("")), 1024*8),
	}
}

// Reset resets all state and sets the Reader to read from rdr
func (r *Reader) Reset(rdr io.Reader) {
	r.Br.Reset(rdr)
}

// CommandFrom reads a Command from an io.Reader
func (r *Reader) CommandFrom(rdr io.Reader) (int, *Command, error) {
	r.Br.Reset(rdr)

	_, line, err := ReadLine(r.Br)
	if err != nil {
		return 0, nil, err
	}
	internal.Debugf(r.config, "read(raw): %q", line)

	line, cmdBytes, err := parseWord(line)
	if err != nil {
		return 0, nil, err
	}

	name := cmdNamefromBytes(cmdBytes)
	_, numArgs, err := parseInt(line)
	// fmt.Printf("readInt: %d %q %+v\n", numArgs, line, err)
	if err != nil {
		return 0, nil, err
	}

	if name == CmdBatch {
		batch := NewBatch(r.config)
		bline, word, berr := parseWord(line)
		if err != nil {
			return 0, nil, berr
		}

		var n uint64
		n, berr = asciiToUint(word[:len(word)-1])
		if berr != nil {
			return 0, nil, berr
		}
		batch.NumMessages = int(n)

		_, word, berr = parseWord(bline)
		if berr != nil {
			return 0, nil, berr
		}

		n, berr = asciiToUint(word[:len(word)-2])
		if berr != nil {
			return 0, nil, berr
		}
		batch.Size = n

		_, berr = batch.readData(r.Br)
		if berr != nil {
			return 0, nil, berr
		}

		cmd := NewCommand(r.config, name)
		cmd.Batch = batch
		return 0, cmd, nil
	}

	args := make([][]byte, int(numArgs))
	for i := 0; i < int(numArgs); i++ {
		_, line, err = ReadLine(r.Br)
		if err != nil {
			return 0, nil, err
		}
		internal.Debugf(r.config, "read arg(raw): %q", internal.Prettybuf(line))

		var arglen uint64
		line, arglen, err = parseUint(line)
		if err != nil {
			return 0, nil, errors.New("Badly formatted argument length")
		}

		arg := trimNewline(line)

		if arglen != uint64(len(arg)) {
			return 0, nil, errors.New("length did not match argument body")
		}

		args[i] = arg
	}

	return 0, NewCommand(r.config, name, args...), nil
}

// ResponseFrom reads a READ command response from an io.Reader
func (r *Reader) ResponseFrom(rdr io.Reader) (*Response, error) {
	r.Br.Reset(rdr)
	_, line, err := ReadLine(r.Br)
	if err != nil {
		return nil, err
	}

	parts := bytes.SplitN(line, []byte(" "), 2)
	var resp *Response
	if bytes.Equal(parts[0], []byte("OK")) {
		resp = NewResponse(r.config, RespOK)
		if len(parts) > 1 {
			var n uint64

			subParts := bytes.SplitN(parts[1], []byte(" "), 2)

			n, err := asciiToUint(subParts[0])
			if err != nil {
				return nil, errors.Wrap(err, "failed to parse response id or body length")
			}

			if len(subParts) > 1 { // this is an OK string response. ie from STATS

				rest := bytes.Join(subParts[1:], []byte(" "))
				rest = append(rest, []byte("\r\n")...)
				if uint64(len(rest)) < n {
					restBytes := make([]byte, n-uint64(len(rest)))
					if _, err := io.ReadFull(r.Br, restBytes); err != nil {
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
		resp = NewResponse(r.config, RespEOF)
	} else if bytes.Equal(parts[0], []byte("ERR")) {
		var arg []byte
		if len(parts) > 1 {
			arg = parts[1]
		}
		resp = NewErrResponse(r.config, arg)
	} else if bytes.Equal(parts[0], []byte("ERR_CLIENT")) {
		resp = NewClientErrResponse(r.config, parts[1])
	} else {
		internal.Debugf(r.config, "invalid response: %q", line)
		return nil, errors.New("Invalid response")
	}

	return resp, nil
}
