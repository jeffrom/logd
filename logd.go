package logd

import (
	"expvar"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/trace"
	"strings"
	"time"
)

var counts = expvar.NewMap("logd")

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.SetOutput(os.Stdout)
}

func getFileLine(distance int) (string, int) {
	_, file, line, ok := runtime.Caller(1 + distance)
	if !ok {
		file = "???"
		line = 0
	}

	parts := strings.Split(file, "/")
	file = parts[len(parts)-1]

	return file, line
}

func stdlog(distance int, s string, args ...interface{}) {
	file, line := getFileLine(distance)

	s = "%s %s " + s + "\n"
	linearg := fmt.Sprintf("%s:%d:", file, line)
	args = append([]interface{}{time.Now().Format("2006/01/02 15:04:05.000"), linearg}, args...)
	fmt.Fprintf(os.Stdout, s, args...)
}

func debugf(config *Config, s string, args ...interface{}) {
	if !config.Verbose {
		return
	}

	stdlog(2, s, args...)
}

func logf(s string, args ...interface{}) {
	stdlog(2, s, args...)
}

func doTrace() func() {
	f, err := os.Create("trace.out")
	panicOnError(err)
	trace.Start(f)
	return trace.Stop
}
