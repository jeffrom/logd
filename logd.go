package logd

import (
	"expvar"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"time"
)

var counts = expvar.NewMap("logd")

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}

func debugf(config *ServerConfig, s string, args ...interface{}) {
	if !config.Verbose {
		return
	}

	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "???"
		line = 0
	}

	parts := strings.Split(file, "/")
	file = parts[len(parts)-1]

	s = "%s %s " + s + "\n"
	linearg := fmt.Sprintf("%s:%d:", file, line)
	args = append([]interface{}{time.Now().Format("2006/01/02 15:04:05"), linearg}, args...)
	fmt.Fprintf(os.Stderr, s, args...)
}
