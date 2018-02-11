package protocol

import (
	"flag"
	"testing"

	"github.com/jeffrom/logd/testhelper"
)

func init() {
	// each test module must define this flag and pass its value to the
	// testhelper module.
	flag.BoolVar(&testhelper.Golden, "golden", false, "write the golden file for this module")
	flag.Parse()
}

func TestUnknownCommand(t *testing.T) {
	// remove this if it becomes a problem. this test is dumb
	unknownCmd := NewCommand(testhelper.DefaultTestConfig(testing.Verbose()), 100)
	s := unknownCmd.String()
	if s != "<unknown_command 'd'>/0" {
		t.Fatalf("improperly formatted command string: %s", s)
	}
}
