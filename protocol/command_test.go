package protocol

import (
	"bytes"
	"strings"
	"testing"
)

func TestCommand(t *testing.T) {
	cmds := []string{"BATCH", "READ", "TAIL", "STATS", "CLOSE"}

	for _, s := range cmds {
		b := []byte(s)
		cmd := cmdNamefromBytes(b)
		if cmd.String() != s {
			t.Fatal(cmd, "!=", s)
		}
		if !bytes.Equal(cmd.Bytes(), b) {
			t.Fatal(cmd, "!=", s)
		}
	}

	badcmd := cmdNamefromBytes([]byte("THIS DOESNT EXIST LOLOLOL"))
	if !strings.Contains(badcmd.String(), "unknown_command") {
		t.Fatal(badcmd, "wasn't unknown")
	}
	if !bytes.Contains(badcmd.Bytes(), []byte("unknown_command")) {
		t.Fatal(badcmd, "wasn't unknown")
	}
}
