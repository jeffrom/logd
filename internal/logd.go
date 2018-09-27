package internal

import (
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/trace"
	"strings"
	"time"

	"github.com/jeffrom/logd/config"
)

// LifecycleManager handles application startup / shutdown for loggers and
// servers.
type LifecycleManager interface {
	Setup() error
	Shutdown() error
}

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

func stdlog(f *os.File, distance int, s string, args ...interface{}) {
	file, line := getFileLine(distance)

	s = "%s %s " + s + "\n"
	linearg := fmt.Sprintf("%s:%d:", file, line)
	args = append([]interface{}{time.Now().Format("2006/01/02 15:04:05.000"), linearg}, args...)
	_, err := fmt.Fprintf(f, s, args...)
	LogError(err)
}

// Debugf prints a debug log message to stdout
func Debugf(conf *config.Config, s string, args ...interface{}) {
	if !conf.Verbose {
		return
	}

	stdlog(os.Stdout, 2, s, args...)
}

// DebugfDepth prints a debug log message to stdout
func DebugfDepth(conf *config.Config, depth int, s string, args ...interface{}) {
	if !conf.Verbose {
		return
	}

	stdlog(os.Stdout, 2+depth, s, args...)
}

// Logf logs to stdout
func Logf(s string, args ...interface{}) {
	stdlog(os.Stdout, 3, s, args...)
}

// LogError logs the error if one occurred
func LogError(err error) {
	if err != nil {
		stdlog(os.Stderr, 2, "error logged and ignored: %+v", err)
	}
}

// LogAndReturnError logs the error if one occurred, then returns the error
func LogAndReturnError(err error) error {
	if err != nil {
		stdlog(os.Stderr, 2, "error logged and ignored: %+v", err)
	}
	return err
}

// IgnoreError logs the error if one occurred
func IgnoreError(verbose bool, err error) {
	if verbose && err != nil {
		stdlog(os.Stderr, 2, "error ignored: %+v", err)
	}
}

// DiscardError does nothing with an error. Used for linting really
func DiscardError(err error) {

}

func doTrace() func() {
	f, err := os.Create("trace.out")
	PanicOnError(err)
	LogError(trace.Start(f))
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

// CloseAll closes all supplied closers, returns the first error, and logs all
// errors.
func CloseAll(c []io.Closer) error {
	var firstErr error

	for _, cl := range c {
		if cl == nil {
			continue
		}
		if err := cl.Close(); err != nil {
			log.Printf("error closing %v: %+v", cl, err)
			if firstErr != nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// CopyBytes returns a copy of p
func CopyBytes(p []byte) []byte {
	b := make([]byte, len(p))
	copy(b, p)
	return b
}
