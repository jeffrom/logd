package protocol

import (
	"bytes"
	"errors"
	"fmt"
)

const maxArgs = 4

var errUnknownCmdType = errors.New("unknown command type")

// CmdType is the type for logd commands.
type CmdType uint8

const (
	_ CmdType = iota

	// CmdBatch is a batch command type.
	CmdBatch

	// CmdRead is the new read request type
	CmdRead

	// CmdTail is similar to READ, except it always starts from the
	// beginning of the log.
	CmdTail

	// CmdStats returns some internal stats.
	CmdStats

	// CmdClose is a close command type.
	CmdClose

	// CmdShutdown is a shutdown command type.
	// CmdShutdown
)

func (cmd *CmdType) String() string {
	switch *cmd {
	case CmdBatch:
		return "BATCH"
	case CmdRead:
		return "READ"
	case CmdTail:
		return "TAIL"
	case CmdStats:
		return "STATS"
	case CmdClose:
		return "CLOSE"
		// case CmdShutdown:
		// 	return "SHUTDOWN"
	}
	return fmt.Sprintf("<unknown_command %q>", *cmd)
}

// Bytes returns a byte representation of a command
func (cmd *CmdType) Bytes() []byte {
	switch *cmd {
	case CmdBatch:
		return []byte("BATCH")
	case CmdRead:
		return []byte("READ")
	case CmdTail:
		return []byte("TAIL")
	case CmdStats:
		return []byte("STATS")
	case CmdClose:
		return []byte("CLOSE")
		// case CmdShutdown:
		// 	return []byte("SHUTDOWN")
	}
	return []byte(fmt.Sprintf("<unknown_command %q>", *cmd))
}

func cmdNamefromBytes(b []byte) CmdType {
	if bytes.Equal(b, []byte("BATCH")) {
		return CmdBatch
	}
	if bytes.Equal(b, []byte("READ")) {
		return CmdRead
	}
	if bytes.Equal(b, []byte("TAIL")) {
		return CmdTail
	}
	if bytes.Equal(b, []byte("STATS")) {
		return CmdStats
	}
	if bytes.Equal(b, []byte("CLOSE")) {
		return CmdClose
	}
	// if bytes.Equal(b, []byte("SHUTDOWN")) {
	// 	return CmdShutdown
	// }
	return 0
}

var argLens = map[CmdType]int{
	CmdBatch: 4,
	CmdRead:  3,
	CmdTail:  2,
	CmdStats: 0,
	CmdClose: 0,
	// CmdShutdown: 0,
}
