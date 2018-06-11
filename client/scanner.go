package client

import (
	"fmt"
	"io"
	"time"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/protocol"
)

// Scanner is similar to protocol.Scanner, but when reading forever, it issues
// additional READ commands when all previous messages have been read
type Scanner struct {
	*protocol.Scanner
	config *config.Config
	client *Client

	err error
}

// newScanner returns a new client scanner
func newScanner(conf *config.Config, client *Client) *Scanner {
	return &Scanner{config: conf, client: client}
}

func (s *Scanner) fromScanResponse() (*Scanner, error) {
	c := s.client

	deadline := time.Now().Add(c.readTimeout)
	if err := c.Conn.SetReadDeadline(deadline); err != nil {
		return nil, err
	}

	resp, err := c.pr.ResponseFrom(c.Conn)
	if c.handleErr(err) != nil {
		return nil, err
	}

	if resp.Failed() {
		return nil, fmt.Errorf("%s %s", resp.Status, resp.Body)
	}

	internal.Debugf(s.config, "initial scan response: %s", resp.Status)

	if err := c.Conn.SetReadDeadline(time.Now().Add(c.readTimeout)); err != nil {
		return nil, err
	}
	s.Scanner = protocol.NewScanner(s.config, c.pr.Br)
	return s, nil
}

// Scan reads messages. when the server's response has finished, it will issue
// another READ command.
func (s *Scanner) Scan() bool {
	c := s.client
	prev := s.Scanner.Message()

	if err := c.Conn.SetReadDeadline(time.Now().Add(c.readTimeout)); err != nil {
		s.err = err
		return false
	}
	res := s.Scanner.Scan()
	if err := s.Scanner.Error(); res == false && s.config.ReadForever &&
		err == io.EOF {

		start := s.config.StartID
		if prev != nil {
			start = prev.ID + 1
		}

		for {
			pscanner, err := s.client.continueRead(start, s.config.ClientBatchSize)
			s.err = err
			if err != nil {
				if err == errNotFound {
					time.Sleep(time.Duration(s.config.ClientWaitInterval) * time.Millisecond)
					continue
				} else {
					return false
				}
			}

			s.Scanner = pscanner
			break
		}

		return s.Scanner.Scan()
	}

	return res
}

func (s *Scanner) Error() error {
	if s.err != nil && s.err != io.EOF {
		return s.err
	}
	err := s.Scanner.Error()
	if err != nil && err != io.EOF {
		return err
	}
	return nil
}
