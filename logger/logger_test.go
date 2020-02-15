package logger

import (
	"flag"
	"os"
	"testing"

	"github.com/jeffrom/logd/testhelper"
)

func TestMain(m *testing.M) {
	// each test module must define this flag and pass its value to the
	// testhelper module.
	flag.BoolVar(&testhelper.Golden, "golden", false, "write the golden file for this module")
	flag.Parse()
	os.Exit(m.Run())
}
