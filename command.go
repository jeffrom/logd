package logd

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
)

var errUnknownCmdType = errors.New("unknown command type")

// CmdType is the type for logd commands.
type CmdType uint8

const (
	_ CmdType = iota

	// CmdMessage is a message command type.
	CmdMessage

	// CmdRawMessage is a message command type. Used for replication.
	CmdRawMessage

	// CmdReplicate initiates a replication session on the connection.
	CmdReplicate

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
	case CmdReplicate:
		return "REPLICATE"
	case CmdRawMessage:
		return "RAWMSG"
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
	if bytes.Equal(b, []byte("RAWMSG")) {
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
	if bytes.Equal(b, []byte("SHUTDOWN")) {
		return CmdShutdown
	}
	if bytes.Equal(b, []byte("SLEEP")) {
		return CmdSleep
	}
	return 0
}

// Command is an input received by a caller
type Command struct {
	config *Config
	connID UUID
	name   CmdType
	args   [][]byte
	respC  chan *Response

	ready chan struct{}
	done  chan struct{}
	wake  chan struct{}
}

// NewCommand returns a new instance of a command type
func NewCommand(config *Config, name CmdType, args ...[]byte) *Command {
	c := &Command{
		config: config,
		name:   name,
		args:   args,
		respC:  make(chan *Response),
		ready:  make(chan struct{}),
		done:   make(chan struct{}),
		wake:   make(chan struct{}),
	}
	return c
}

func newCloseCommand(config *Config, respC chan *Response) *Command {
	return &Command{
		config: config,
		name:   CmdClose,
		respC:  respC,
	}
}

func (cmd *Command) String() string {
	return fmt.Sprintf("%s/%d", cmd.name.String(), len(cmd.args))
}

// Bytes returns a byte representation of the command
func (cmd *Command) Bytes() []byte {
	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("%s %d\r\n", cmd.name.String(), len(cmd.args)))
	for _, arg := range cmd.args {
		buf.WriteString(strconv.FormatInt(int64(len(arg)), 10))
		buf.WriteByte(' ')
		buf.Write(arg)
		buf.WriteString("\r\n")
	}
	return buf.Bytes()
}

func (cmd *Command) respond(resp *Response) {
	debugf(cmd.config, "<-response: %q", resp)
	cmd.respC <- resp

	close(cmd.respC)
	// cmd.respC = nil
}

func (cmd *Command) finish() {
	select {
	case cmd.done <- struct{}{}:
		debugf(cmd.config, "cmd <-done")
	default:
		debugf(cmd.config, "tried but failed to finish command %s", cmd)
	}
}

func (cmd *Command) signalReady() {
	debugf(cmd.config, "signalling command %s readiness", cmd)
	cmd.ready <- struct{}{}
}

func (cmd *Command) waitForReady() {
	debugf(cmd.config, "waiting for command %s to be ready", cmd)
	select {
	case <-cmd.ready:
		// case <-time.After(100 * time.Millisecond):
		// 	log.Printf("timed out waiting for read command to be ready")
		// 	return
	}

}

func (cmd *Command) cancelSleep() {
	cmd.wake <- struct{}{}
}

func parseNumber(b []byte) (uint64, error) {
	return strconv.ParseUint(string(b), 10, 64)
}
