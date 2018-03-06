package app_test

import (
	"os"
	"testing"

	"github.com/jeffrom/logd/testhelper"
)

func TestMain(m *testing.M) {
	res := m.Run()

	f, err := os.Open(testhelper.ArtifactFile)
	if err != nil {
		panic("failed to open artifact file")
	}
	defer f.Close()

	// only clean up test artifacts if the tests pass
	if res == 0 {
		if err := testhelper.CleanupSuite(); err != nil {
			panic(err)
		}
	}

	os.Exit(res)
}
