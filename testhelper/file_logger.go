package testhelper

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/jeffrom/logd/config"
)

const LogDirPrefix = "logd-testdata"

var ArtifactFile string

func init() {
	tmpfile, err := ioutil.TempFile("", "logd-artifacts.log")
	if err != nil {
		panic(err)
	}
	defer tmpfile.Close()

	ArtifactFile = tmpfile.Name()
}

// TmpLog creates a temporary directory for testing purposes
func TmpLog() string {
	tmpdir := getTempdir()
	return path.Join(tmpdir, "logs")
}

func getTempdir() string {
	dir, err := ioutil.TempDir("", LogDirPrefix)
	if err != nil {
		panic(fmt.Sprintf("Failed to get tempdir: %+v", err))
	}

	f, err := os.OpenFile(ArtifactFile, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		panic("failed to open artifact log for writing")
	}
	defer f.Close()

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		panic(err)
	}
	defer func() {
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN); err != nil {
			panic("failed to write to artifact log")
		}
	}()

	if _, err := f.WriteString(fmt.Sprintf("%s\n", dir)); err != nil {
		panic("failed to write to artifact log")
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

func CleanupSuite() error {
	f, err := os.Open(ArtifactFile)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fname := scanner.Text()

		if err := os.RemoveAll(fname); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	if err := os.Remove(ArtifactFile); err != nil {
		return err
	}
	return nil
}

func GetLogOutput(config *config.Config, l io.Reader) []byte {
	b, err := ioutil.ReadAll(l)
	if err != nil {
		panic(err)
	}
	return b
}

func ListPartitions(conf *config.Config) []string {
	matches, err := filepath.Glob(conf.WorkDir + ".*")
	if err != nil {
		panic(err)
	}
	var files []string
	for _, match := range matches {
		if strings.HasSuffix(match, ".index") {
			continue
		}
		files = append(files, match)
	}
	return files
}
