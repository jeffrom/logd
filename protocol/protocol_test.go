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

func TestAsciiToInt(t *testing.T) {

}

func TestIntToASCII(t *testing.T) {

}
