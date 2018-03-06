package protocol

import (
	"bytes"
	"errors"
	"fmt"
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

	// CmdRead is a read command type.
	CmdRead

	// CmdHead is a head command type.
	CmdHead

	// CmdStats returns some internal stats.
	CmdStats

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
	case CmdRead:
		return "READ"
	case CmdHead:
		return "HEAD"
	case CmdStats:
		return "STATS"
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

func cmdNamefromBytes(b []byte) CmdType {
	if bytes.Equal(b, []byte("MSG")) {
		return CmdMessage
	}
	if bytes.Equal(b, []byte("READ")) {
		return CmdRead
	}
	if bytes.Equal(b, []byte("HEAD")) {
		return CmdHead
	}
	if bytes.Equal(b, []byte("STATS")) {
		return CmdStats
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

// Command is an input received by a caller
type Command struct {
	config *config.Config
	ConnID string
	Name   CmdType
	Args   [][]byte
	RespC  chan *Response

	ready chan struct{}
	Done  chan struct{}
	Wake  chan struct{}
}

// NewCommand returns a new instance of a command type
func NewCommand(conf *config.Config, name CmdType, args ...[]byte) *Command {
	c := &Command{
		config: conf,
		Name:   name,
		Args:   args,
		RespC:  make(chan *Response, 1),
		ready:  make(chan struct{}, 1),
		Done:   make(chan struct{}),
		Wake:   make(chan struct{}),
	}
	return c
}

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

func (cmd *Command) Respond(resp *Response) {
	internal.Debugf(cmd.config, "<-response: %q", resp)
	select {
	case cmd.RespC <- resp:
	}

	close(cmd.RespC)
	// cmd.RespC = nil
}

func (cmd *Command) Finish() {
	if cmd.Done == nil {
		return
	}
	select {
	case cmd.Done <- struct{}{}:
		internal.Debugf(cmd.config, "cmd %s <-Done", cmd)
	default:
		internal.Debugf(cmd.config, "tried but failed to finish command %s", cmd)
	}
}

func (cmd *Command) SignalReady() {
	internal.Debugf(cmd.config, "signalling command %s readiness", cmd)
	cmd.ready <- struct{}{}
}

func (cmd *Command) WaitForReady() {
	internal.Debugf(cmd.config, "waiting for command %s to be ready", cmd)
	select {
	case <-cmd.ready:
		// case <-time.After(100 * time.Millisecond):
		// 	log.Printf("timed out waiting for read command to be ready")
		// 	return
	}

}

func (cmd *Command) CancelSleep() {
	cmd.Wake <- struct{}{}
}

// ParseNumber parses a uint64 from bytes
func ParseNumber(b []byte) (uint64, error) {
	return strconv.ParseUint(string(b), 10, 64)
}
