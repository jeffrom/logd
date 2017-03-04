package logd

import "fmt"

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
	// case cmdSleep:
	// 	return "SLEEP"
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
}

func newCommand(name cmdType, args ...[]byte) *command {
	c := &command{
		name:  name,
		args:  args,
		respC: make(chan *response, 0),
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
