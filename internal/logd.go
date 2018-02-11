package internal

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/trace"
	"strings"
	"time"

	"github.com/jeffrom/logd/config"
)

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

// Debugf prints a debug log message to stdout
func Debugf(conf *config.Config, s string, args ...interface{}) {
	if !conf.Verbose {
		return
	}

	stdlog(2, s, args...)
}

// Logf logs to stdout
func Logf(s string, args ...interface{}) {
	stdlog(2, s, args...)
}

func doTrace() func() {
	f, err := os.Create("trace.out")
	PanicOnError(err)
	trace.Start(f)
	return trace.Stop
}

// PanicOnError panics if an error is passed.
func PanicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

// Prettybuf returns a human readable representation of a buffer that fits more
// or less on a log line
func Prettybuf(bufs ...[]byte) []byte {
	var flat []byte
	limit := 100
	for _, b := range bufs {
		flat = append(flat, b...)
	}
	if len(flat) > limit {
		// flat = flat[:limit-5] + []byte("...") + flat[limit-2:]
		var final []byte
		final = append(final, flat[:limit-5]...)
		final = append(final, []byte("...")...)
		final = append(final, flat[len(flat)-2:]...)
		return final
	}
	return flat
}
