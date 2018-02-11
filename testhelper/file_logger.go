package testhelper

import (
	"fmt"
	"io"
	"io/ioutil"
	"path"

	"github.com/jeffrom/logd/config"
)

const LogDirPrefix = "__logd-testdata__"

// TmpLog creates a temporary directory for testing purposes
func TmpLog() string {
	tmpdir := getTempdir()
	return path.Join(tmpdir, "__log")
}

func getTempdir() string {
	dir, err := ioutil.TempDir("", LogDirPrefix)
	if err != nil {
		panic(fmt.Sprintf("Failed to get tempdir: %+v", err))
	}

	//
	// debugging
	//

	// fmt.Println(dir)
	// ls, err := ioutil.ReadDir(dir)
	// if err != nil {
	// 	panic(err)
	// }
	// for _, f := range ls {
	// 	fmt.Println(f.Name())
	// }

	//
	//
	//

	return dir
}

func GetLogOutput(config *config.Config, l io.Reader) []byte {
	b, err := ioutil.ReadAll(l)
	if err != nil {
		panic(err)
	}
	return b
}
