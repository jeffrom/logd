package logd

import (
	"bytes"
	"flag"
	"io/ioutil"
	"runtime/debug"
	"testing"
)

var golden bool

func init() {
	flag.BoolVar(&golden, "golden", false, "write the golden file")
	flag.Parse()
}

func defaultTestConfig() *Config {
	return &Config{
		Verbose:       testing.Verbose(),
		Logger:        newMemLogger(),
		ServerTimeout: 500,
		ClientTimeout: 500,
	}
}

func checkGoldenFile(t *testing.T, filename string, b []byte, golden bool) {
	goldenFile := "testdata/" + filename + ".golden"
	goldenActual := "testdata/" + filename + ".actual.golden"

	if golden {
		t.Logf("Writing golden file to %s", goldenFile)
		ioutil.WriteFile(goldenFile, b, 0644)
		return
	}

	expected, err := ioutil.ReadFile(goldenFile)
	checkError(t, err)
	if !bytes.Equal(b, expected) {
		ioutil.WriteFile(goldenActual, b, 0644)
		t.Fatalf("Golden files didn't match: wrote output to %s", goldenActual)
	}
}

func checkError(t *testing.T, err error) {
	if err != nil {
		t.Logf("%s", debug.Stack())
		t.Fatalf("Unexpected error %v", err)
	}
}
