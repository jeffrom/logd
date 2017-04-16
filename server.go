package logd

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// Server is an interface for interaction with clients
// type Server interface {
// 	Respond(cmd *Command, resp *Response) error
// 	Send(sub *Subscription, msg *Message) error
// }

// SocketServer handles socket connections
type SocketServer struct {
	config *Config

	addr string
	ln   net.Listener
	mu   sync.Mutex

	readyC chan struct{}
	conns  map[*conn]bool
	connMu sync.Mutex
	connIn chan *conn

	readTimeout  time.Duration
	writeTimeout time.Duration

	stopC     chan struct{}
	shutdownC chan struct{}

	q *eventQ

	stats    *stats
	statlock sync.RWMutex
}

// NewServer will return a new instance of a log server
func NewServer(addr string, config *Config) *SocketServer {
	q := newEventQ(config)
	q.start()

	return &SocketServer{
		config:       config,
		addr:         addr,
		readyC:       make(chan struct{}),
		conns:        make(map[*conn]bool),
		connIn:       make(chan *conn, 0),
		readTimeout:  time.Duration(500 * time.Millisecond),
		writeTimeout: time.Duration(500 * time.Millisecond),
		stopC:        make(chan struct{}),
		shutdownC:    make(chan struct{}),
		q:            q,
		stats:        &stats{},
	}
}

// ListenAndServe starts serving requests
func (s *SocketServer) ListenAndServe() error {
	return s.listenAndServe(false)
}

func (s *SocketServer) listenAndServe(wait bool) error {
	var outerErr error

	s.mu.Lock()
	s.ln, outerErr = net.Listen("tcp", s.addr)
	s.mu.Unlock()
	if outerErr != nil {
		return outerErr
	}

	s.statlock.Lock()
	s.stats.startedAt = time.Now().UTC()
	s.statlock.Unlock()

	log.Printf("Serving at %s", s.ln.Addr())
	if wait {
		s.readyC <- struct{}{}
	}

	go s.accept()

	for {
		select {
		case <-s.stopC:
			log.Printf("Shutting down server at %s", s.ln.Addr())
			return s.shutdown()
		case conn := <-s.connIn:
			go s.handleClient(conn)
		}
	}
}

// Respond satisfies Server interface
func (s *SocketServer) Respond(cmd *Command, resp *Response) error {
	return nil
}

// Send satisfies Server interface
func (s *SocketServer) Send(sub *Subscription, msg *Message) error {
	return nil
}

// ready signals that the application is ready to serve on this host:port
func (s *SocketServer) ready() {
	<-s.readyC
}

func (s *SocketServer) accept() {
	for {
		rawConn, err := s.ln.Accept()
		if err != nil {
			break
		}

		debugf(s.config, "accept: %s", rawConn.RemoteAddr())

		conn := newConn(rawConn, s.config)
		s.addConn(conn)

		s.connIn <- conn
	}
}

func (s *SocketServer) goServe() {
	go func() {
		if err := s.listenAndServe(true); err != nil {
			log.Printf("error serving: %v", err)
		}
	}()
	s.ready()
}

func (s *SocketServer) shutdown() error {
	defer func() {
		s.shutdownC <- struct{}{}
	}()
	err := s.q.stop()

	s.connMu.Lock()
	for conn := range s.conns {
		delete(s.conns, conn)
	}
	s.connMu.Unlock()

	return err
}

func (s *SocketServer) stop() {
	s.stopC <- struct{}{}
	<-s.shutdownC
}

func (s *SocketServer) addConn(conn *conn) {
	s.connMu.Lock()
	s.conns[conn] = true
	s.connMu.Unlock()
}

func (s *SocketServer) removeConn(conn *conn) {
	s.connMu.Lock()
	delete(s.conns, conn)
	s.connMu.Unlock()
}

func (s *SocketServer) handleClient(conn *conn) {
	counts.Add("clients", 1)

	defer func() {
		counts.Add("clients", -1)

		s.removeConn(conn)
		conn.Close()
	}()

	for {
		err := conn.SetReadDeadline(time.Now().Add(s.readTimeout))
		panicOnError(err)

		cmd, err := conn.pr.readCommand()
		if err == io.EOF {
			return
		}
		if err, ok := err.(net.Error); ok && err.Timeout() {
			log.Printf("%s timed out", conn.RemoteAddr())
			return
		}
		panicOnError(err)

		debugf(s.config, "%s<-%s: %s", conn.LocalAddr(), conn.RemoteAddr(), cmd)

		resp, err := s.q.add(cmd)
		panicOnError(err)

		err = conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))
		panicOnError(err)

		respBytes := resp.Bytes()
		if cmd.name == CmdClose {
			debugf(s.config, "close %s", conn.RemoteAddr())
			conn.mu.Lock()
			conn.pw.bw.Write(respBytes)
			conn.pw.bw.Flush()
			conn.mu.Unlock()
			break
		} else {
			conn.mu.Lock()
			_, err = conn.pw.bw.Write(respBytes)
			conn.mu.Unlock()
			if err, ok := err.(net.Error); ok && err.Timeout() {
				debugf(s.config, "%s timed out", conn.RemoteAddr())
				return
			}
			panicOnError(err)
			conn.mu.Lock()
			err = conn.pw.bw.Flush()
			conn.mu.Unlock()
			if err, ok := err.(net.Error); ok && err.Timeout() {
				debugf(s.config, "%s timed out", conn.RemoteAddr())
				return
			}
			panicOnError(err)
		}
		debugf(s.config, "%s->%s: %q", conn.LocalAddr(), conn.RemoteAddr(), respBytes)

		if cmd.name == CmdRead {
			debugf(s.config, "sending log messages to %s", conn.RemoteAddr())
			go s.handleSubscriber(conn.Conn, cmd, resp)
		}
	}
}

func (s *SocketServer) handleSubscriber(c net.Conn, cmd *Command, resp *Response) {
	conn := newConn(c, s.config)
	for {
		select {
		case msg := <-resp.msgC:
			conn.mu.Lock()
			conn.pw.bw.WriteString(fmt.Sprintf("+%d %d %s\r\n", msg.id, len(msg.body), msg.body))
			conn.pw.bw.Flush()
			conn.mu.Unlock()
		case <-cmd.done:
			conn.mu.Lock()
			conn.pw.bw.Write([]byte("+EOF\r\n"))
			conn.pw.bw.Flush()
			conn.mu.Unlock()
			return
		}
	}
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}
