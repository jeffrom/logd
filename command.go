package logd

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
)

var errUnknownCmdType = errors.New("unknown command type")

type cmdType uint8

const (
	_ cmdType = iota

	cmdMsg

	cmdRead
	cmdHead

	cmdPing
	cmdClose
	cmdSleep
	cmdShutdown
)

func (cmd *cmdType) String() string {
	switch *cmd {
	case cmdMsg:
		return "MSG"
	case cmdRead:
		return "READ"
	case cmdHead:
		return "HEAD"
	case cmdPing:
		return "PING"
	case cmdClose:
		return "CLOSE"
	case cmdSleep:
		return "SLEEP"
	case cmdShutdown:
		return "SHUTDOWN"
	}
	return fmt.Sprintf("<unknown_command %q>", *cmd)
}

// command is an input received by a caller
type command struct {
	name  cmdType
	args  [][]byte
	respC chan *response
	done  chan struct{}
	wake  chan struct{}
}

func newCommand(name cmdType, args ...[]byte) *command {
	c := &command{
		name:  name,
		args:  args,
		respC: make(chan *response, 0),
		done:  make(chan struct{}),
		wake:  make(chan struct{}),
	}
	return c
}

func newCloseCommand(respC chan *response) *command {
	return &command{
		name:  cmdClose,
		respC: respC,
	}
}

func (cmd *command) String() string {
	return fmt.Sprintf("%s/%d", cmd.name.String(), len(cmd.args))
}

func (cmd *command) Bytes() []byte {
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

func (cmd *command) respond(resp *response) {
	cmd.respC <- resp
}

func (cmd *command) finish() {
	if cmd.done != nil {
		select {
		case cmd.done <- struct{}{}:
		default:
		}
		close(cmd.respC)
		close(cmd.done)
	}
}

func (cmd *command) cancelSleep() {
	cmd.wake <- struct{}{}
}

func cmdNamefromBytes(b []byte) cmdType {
	if bytes.Equal(b, []byte("MSG")) {
		return cmdMsg
	}
	if bytes.Equal(b, []byte("READ")) {
		return cmdRead
	}
	if bytes.Equal(b, []byte("HEAD")) {
		return cmdHead
	}
	if bytes.Equal(b, []byte("PING")) {
		return cmdPing
	}
	if bytes.Equal(b, []byte("CLOSE")) {
		return cmdClose
	}
	if bytes.Equal(b, []byte("SHUTDOWN")) {
		return cmdShutdown
	}
	if bytes.Equal(b, []byte("SLEEP")) {
		return cmdSleep
	}
	return 0
}
