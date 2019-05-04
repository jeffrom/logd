package internal

import (
	"io"
	"log"
)

type readLogger struct {
	prefix string
	r      io.Reader
	depth  int
}

func (l *readLogger) Read(p []byte) (int, error) {
	n, err := l.r.Read(p)
	truncated := n
	var suff string
	if n > 300 {
		truncated = 300
		suff = "..."
	}
	file, line := getFileLine(l.depth)
	if err != nil {
		log.Printf("%s %s:%d (%d) %q%s: %v", l.prefix, file, line, len(p), p[0:truncated], suff, err)
	} else {
		log.Printf("%s %s:%d (%d) %q%s", l.prefix, file, line, len(p), p[0:truncated], suff)
	}
	return n, err
}

// NewReadLogger returns a reader that behaves like r except
// that it logs (using log.Printf) each read to standard error,
// printing the prefix and the data read.
func NewReadLogger(prefix string, r io.Reader, depth int) io.Reader {
	return &readLogger{prefix, r, depth}
}
