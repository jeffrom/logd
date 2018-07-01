package protocol

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
)

var errUnknownCmdType = errors.New("unknown command type")

// CmdType is the type for logd commands.
type CmdType uint8

const (
	_ CmdType = iota

	// CmdMessage is a message command type.
	CmdMessage

	// CmdBatch is a batch command type.
	CmdBatch

	// CmdRead is a read command type.
	CmdRead

	// CmdReadV2 is the new read request type
	CmdReadV2

	// CmdTail is a read command type. While similar to Read, in the case where
	// the caller supplies an id lower than the tail of the log, Tail starts
	// from the beginning of the log.
	CmdTail

	// CmdTailV2 is similar to READV2, except it always starts from the
	// beginning of the log.
	CmdTailV2

	// CmdHead is a head command type.
	CmdHead

	// CmdStats returns some internal stats.
	CmdStats

	// CmdStatsV2 returns some internal stats.
	CmdStatsV2

	// CmdPing is a ping command type.
	CmdPing

	// CmdClose is a close command type.
	CmdClose

	// CmdSleep is a sleep command type.
	CmdSleep

	// CmdShutdown is a shutdown command type.
	CmdShutdown
)

func (cmd *CmdType) String() string {
	switch *cmd {
	case CmdMessage:
		return "MSG"
	case CmdBatch:
		return "BATCH"
	case CmdRead:
		return "READ"
	case CmdReadV2:
		return "READV2"
	case CmdTail:
		return "TAIL"
	case CmdTailV2:
		return "TAILV2"
	case CmdHead:
		return "HEAD"
	case CmdStats:
		return "STATS"
	case CmdStatsV2:
		return "STATSV2"
	case CmdPing:
		return "PING"
	case CmdClose:
		return "CLOSE"
	case CmdSleep:
		return "SLEEP"
	case CmdShutdown:
		return "SHUTDOWN"
	}
	return fmt.Sprintf("<unknown_command %q>", *cmd)
}

// Bytes returns a byte representation of a command
func (cmd *CmdType) Bytes() []byte {
	switch *cmd {
	case CmdMessage:
		return []byte("MSG")
	case CmdBatch:
		return []byte("BATCH")
	case CmdRead:
		return []byte("READ")
	case CmdReadV2:
		return []byte("READV2")
	case CmdTail:
		return []byte("TAIL")
	case CmdTailV2:
		return []byte("TAILV2")
	case CmdHead:
		return []byte("HEAD")
	case CmdStats:
		return []byte("STATS")
	case CmdStatsV2:
		return []byte("STATSV2")
	case CmdPing:
		return []byte("PING")
	case CmdClose:
		return []byte("CLOSE")
	case CmdSleep:
		return []byte("SLEEP")
	case CmdShutdown:
		return []byte("SHUTDOWN")
	}
	return []byte(fmt.Sprintf("<unknown_command %q>", *cmd))
}

func cmdNamefromBytes(b []byte) CmdType {
	if bytes.Equal(b, []byte("MSG")) {
		return CmdMessage
	}
	if bytes.Equal(b, []byte("BATCH")) {
		return CmdBatch
	}
	if bytes.Equal(b, []byte("READ")) {
		return CmdRead
	}
	if bytes.Equal(b, []byte("READV2")) {
		return CmdReadV2
	}
	if bytes.Equal(b, []byte("TAIL")) {
		return CmdTail
	}
	if bytes.Equal(b, []byte("TAILV2")) {
		return CmdTailV2
	}
	if bytes.Equal(b, []byte("HEAD")) {
		return CmdHead
	}
	if bytes.Equal(b, []byte("STATS")) {
		return CmdStats
	}
	if bytes.Equal(b, []byte("STATSV2")) {
		return CmdStatsV2
	}
	if bytes.Equal(b, []byte("PING")) {
		return CmdPing
	}
	if bytes.Equal(b, []byte("CLOSE")) {
		return CmdClose
	}
	if bytes.Equal(b, []byte("SLEEP")) {
		return CmdSleep
	}
	if bytes.Equal(b, []byte("SHUTDOWN")) {
		return CmdShutdown
	}
	return 0
}

var argLens = map[CmdType]int{
	CmdMessage:  2,
	CmdBatch:    3,
	CmdRead:     2,
	CmdReadV2:   2,
	CmdTail:     2,
	CmdTailV2:   1,
	CmdHead:     0,
	CmdStats:    0,
	CmdStatsV2:  0,
	CmdPing:     0,
	CmdClose:    0,
	CmdSleep:    1,
	CmdShutdown: 0,
}

const maxArgs = 3

// Command is an input received by a caller
type Command struct {
	config *config.Config
	ConnID string
	Name   CmdType
	Args   [][]byte
	RespC  chan *Response
	Batch  *Batch

	ready chan struct{}
	Wake  chan struct{}

	digitbuf [32]byte
}

// NewCommand returns a new instance of a command type
func NewCommand(conf *config.Config, name CmdType, args ...[]byte) *Command {
	c := &Command{
		config: conf,
		Name:   name,
		Args:   args,
		RespC:  make(chan *Response),
		ready:  make(chan struct{}),
		Wake:   make(chan struct{}),
	}
	return c
}

// NewCloseCommand returns a close command struct
func NewCloseCommand(conf *config.Config, respC chan *Response) *Command {
	return &Command{
		config: conf,
		Name:   CmdClose,
		RespC:  respC,
	}
}

func (cmd *Command) String() string {
	return fmt.Sprintf("%s/%d", cmd.Name.String(), len(cmd.Args))
}

// Bytes returns a byte representation of the command
func (cmd *Command) Bytes() []byte {
	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("%s %d\r\n", cmd.Name.String(), len(cmd.Args)))
	for _, arg := range cmd.Args {
		buf.WriteString(strconv.FormatInt(int64(len(arg)), 10))
		buf.WriteByte(' ')
		buf.Write(arg)
		buf.WriteString("\r\n")
	}
	return buf.Bytes()
}

// WriteTo implements io.WriterTo. Intended for writing to a net.Conn
func (cmd *Command) WriteTo(w io.Writer) (int64, error) {
	var total int64
	n, err := w.Write(cmd.Name.Bytes())
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write([]byte(" "))
	total += int64(n)
	if err != nil {
		return total, err
	}

	l := uintToASCII(uint64(len(cmd.Args)), &cmd.digitbuf)
	n, err = w.Write(cmd.digitbuf[l:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write([]byte("\r\n"))
	total += int64(n)
	if err != nil {
		return total, err
	}

	for _, arg := range cmd.Args {
		l = uintToASCII(uint64(len(arg)), &cmd.digitbuf)
		n, err = w.Write(cmd.digitbuf[l:])
		total += int64(n)
		if err != nil {
			return total, err
		}

		n, err = w.Write([]byte(" "))
		total += int64(n)
		if err != nil {
			return total, err
		}

		n, err = w.Write(arg)
		total += int64(n)
		if err != nil {
			return total, err
		}

		n, err = w.Write([]byte("\r\n"))
		total += int64(n)
		if err != nil {
			return total, err
		}
	}

	return total, nil
}

// Respond sends a command's response back over the command's channel
func (cmd *Command) Respond(resp *Response) {
	internal.Debugf(cmd.config, "<-response: %q", resp)
	select {
	case cmd.RespC <- resp:
	}

	close(cmd.RespC)
	// cmd.RespC = nil
}

// SignalReady signals that the command is ready for read response bytes
func (cmd *Command) SignalReady() {
	internal.Debugf(cmd.config, "signalling command %s readiness", cmd)
	cmd.ready <- struct{}{}
}

// WaitForReady blocks until the ready signal is received
func (cmd *Command) WaitForReady() {
	internal.Debugf(cmd.config, "waiting for command %s to be ready", cmd)
	select {
	case <-cmd.ready:
		// case <-time.After(100 * time.Millisecond):
		// 	log.Printf("timed out waiting for read command to be ready")
		// 	return
	}
	internal.Debugf(cmd.config, "command %s ready", cmd)
}

// CancelSleep wakes a sleep command
func (cmd *Command) CancelSleep() {
	cmd.Wake <- struct{}{}
}

// IsRead returns true if the command is a read command
func (cmd *Command) IsRead() bool {
	return cmd.Name == CmdRead || cmd.Name == CmdTail
}

// ParseNumber parses a uint64 from bytes
func ParseNumber(b []byte) (uint64, error) {
	return strconv.ParseUint(string(b), 10, 64)
}
