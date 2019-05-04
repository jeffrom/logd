// Package logd contains logd client functionality.
//
// Client implements the logd socket protocol. For application use, Writer and
// Scanner provide APIs for writing to and reading from the log server.
//
// Writer implements io.Writer, batching input and flushing to the server,
// either when the configured batch size is reached, or at a configured time
// interval.
//
// Scanner has a similar API to bufio.Scanner. It can read either
// from the beginning of the log, or from an offset, keeping track of the
// current batch offset and message delta as well as calling for more batches
// as necessary.
//
package logd
