package logd

import (
	"bufio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
)

const defaultChunkSize int64 = 1024 * 1024

// takes a startID and a conn and writes the log directly into the conn
// message format is:
// <chunksize> <crc>\r\n
// <chunk>\r\n
type logReplicaServer struct {
	conn *net.TCPConn
}

func newLogReplicaServer(c *net.TCPConn) *logReplicaServer {
	return &logReplicaServer{conn: c}
}

func (r *logReplicaServer) readFromID(id uint64) (int64, error) {
	return 0, nil
}

var table = crc32.MakeTable(crc32.Koopman)

func (r *logReplicaServer) sendChunks(rdr io.Reader, size int64) (int64, error) {
	var total int64
	for total < size {
		chunkSize := defaultChunkSize
		if size-total < defaultChunkSize {
			chunkSize = size - total
		}

		// TODO don't be so lazy about allocating ^_^
		b := make([]byte, chunkSize)
		_, err := io.ReadFull(io.LimitReader(rdr, chunkSize), b)
		if err != nil {
			return total, err
		}

		checksum := crc32.Checksum(b, table)

		// _, err = rdr.Seek(0, io.SeekStart)
		// if err != nil {
		// 	return total, err
		// }

		_, err = fmt.Fprintf(r.conn, "%d %d\r\n", chunkSize, checksum)
		if err != nil {
			return total, err
		}

		// does sendfile, probably wont in this case though since this isn't
		// actually a file.
		// wroten, err := r.conn.ReadFrom(io.LimitReader(rdr, chunkSize))
		// total += wroten
		// if err != nil {
		// 	// TODO try to send an error code back to the client?
		// 	return total, err
		// }

		n, err := fmt.Fprint(r.conn, string(b))
		total += int64(n)
		if err != nil {
			return total, err
		}

		_, err = r.conn.Write([]byte("\r\n"))
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

type logReplicaScanner struct {
	err error
	f   io.Reader
	br  *bufio.Reader
	buf []byte
}

func newLogReplicaScanner(r io.Reader) *logReplicaScanner {
	return &logReplicaScanner{f: r, br: bufio.NewReader(r)}
}

func (s *logReplicaScanner) Scan() bool {
	s.err = nil

	line, err := readLine(s.br)
	if err != nil {
		s.err = err
		return false
	}

	var chunkSize uint64
	var checksum uint32
	_, err = fmt.Sscanf(string(line), "%d %d", &chunkSize, &checksum)
	if err != nil {
		s.err = err
		return false
	}

	buf := make([]byte, chunkSize+2)
	_, err = io.ReadFull(s.br, buf)
	if err != nil {
		s.err = err
		return false
	}

	chunk := buf[:len(buf)-2]
	calculatedChecksum := crc32.Checksum(chunk, table)
	if checksum != calculatedChecksum {
		s.err = errors.New("crc32 mismatch")
		return false
	}

	s.buf = chunk
	return true
}

func (s *logReplicaScanner) Err() error {
	return s.err
}

func (s *logReplicaScanner) Buffer() []byte {
	return s.buf
}

// Replica client. receives a chunk of log messages and writes them directly
// into the log. For now just uses the read protocol.
type Replica struct {
	Client
}

func newReplica(client Client) *Replica {
	return &Replica{Client: client}
}
