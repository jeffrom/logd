package server

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/events"
	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/protocol"
)

// Server is an interface for interaction with clients
type Server interface {
	Respond(cmd *protocol.Command, resp *protocol.Response) error
	Send(sub *events.Subscription, msg *protocol.Message) error
}

// SocketServer handles socket connections
type SocketServer struct {
	config *config.Config

	addr string
	ln   net.Listener
	mu   sync.Mutex

	conns  map[*Conn]bool
	connMu sync.Mutex
	connIn chan *Conn

	readTimeout  time.Duration
	writeTimeout time.Duration

	readyC       chan struct{}
	stopC        chan struct{}
	shutdownC    chan struct{}
	shuttingDown bool

	q *events.EventQ
}

// NewServer will return a new instance of a log server
func NewServer(addr string, config *config.Config) *SocketServer {
	internal.Debugf(config, "starting options: %s", config)
	q := events.NewEventQ(config)
	if err := q.Start(); err != nil {
		panic(err)
	}

	timeout := time.Duration(config.ServerTimeout) * time.Millisecond
	return &SocketServer{
		config:       config,
		addr:         addr,
		readyC:       make(chan struct{}),
		conns:        make(map[*Conn]bool),
		connIn:       make(chan *Conn, 0),
		readTimeout:  timeout,
		writeTimeout: timeout,
		stopC:        make(chan struct{}),
		shutdownC:    make(chan struct{}),
		q:            q,
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

	log.Printf("Serving at %s", s.ln.Addr())
	if wait {
		select {
		case s.readyC <- struct{}{}:
		}
	}

	go s.accept()

	for {
		select {
		case <-s.stopC:
			log.Printf("Shutting down server at %s", s.ln.Addr())
			s.logConns()
			return s.shutdown()
		case conn := <-s.connIn:
			go s.handleConnection(conn)
		}
	}
}

// Respond satisfies Server interface
// func (s *SocketServer) Respond(cmd *Command, resp *Response) error {
// 	return nil
// }

// Send satisfies Server interface
// func (s *SocketServer) Send(sub *Subscription, msg *Message) error {
// 	return nil
// }

// ready signals that the application is ready to serve on this host:port
func (s *SocketServer) ready() {
	select {
	case <-s.readyC:
	}
}

func (s *SocketServer) isShuttingDown() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.shuttingDown
}

func (s *SocketServer) accept() {
	for {
		if s.isShuttingDown() {
			break
		}

		rawConn, err := s.ln.Accept()
		if err != nil {
			break
		}
		if s.isShuttingDown() {
			log.Printf("Closed new connection from %s because shutting down", rawConn.RemoteAddr())
			rawConn.Close()
			break
		}

		s.q.Stats.Incr("total_connections")
		internal.Debugf(s.config, "accept: %s", rawConn.RemoteAddr())

		conn := newServerConn(rawConn, s.config)
		s.addConn(conn)

		select {
		case s.connIn <- conn:
		}

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

// shutdown shuts down the server
func (s *SocketServer) shutdown() error {
	defer func() {
		select {
		case s.shutdownC <- struct{}{}:
		}
	}()

	s.mu.Lock()
	s.shuttingDown = true
	s.mu.Unlock()

	err := s.q.Stop()

	wg := sync.WaitGroup{}
	s.connMu.Lock()
	for c := range s.conns {
		wg.Add(1)
		go func(c *Conn) {
			defer wg.Done()

			if c.isActive() {
				select {
				case <-c.done:
					internal.Debugf(s.config, "%s closed gracefully", c.RemoteAddr())
				case <-time.After(1 * time.Second): // XXX config timeout
					log.Printf("%s timed out", c.RemoteAddr())
				}
			}

			s.removeConn(c)
		}(c)
	}
	s.connMu.Unlock()
	wg.Wait()

	return err
}

func (s *SocketServer) logConns() {
	s.connMu.Lock()
	defer s.connMu.Unlock()

	var states []string
	for c := range s.conns {
		state := fmt.Sprintf("%s(%s)", c.Conn.RemoteAddr(), c.getState())
		states = append(states, state)
	}

	log.Printf("connection states (%d): %s", len(states), strings.Join(states, ", "))
}

// Stop can be called to shut down the server
func (s *SocketServer) Stop() {
	select {
	case s.stopC <- struct{}{}:
	}

	select {
	case <-s.shutdownC:
	}
}

func (s *SocketServer) addConn(conn *Conn) {
	conn.setState(connStateInactive)
	s.connMu.Lock()
	s.conns[conn] = true
	s.connMu.Unlock()
}

func (s *SocketServer) removeConn(conn *Conn) {
	conn.Close()
	s.connMu.Lock()
	delete(s.conns, conn)
	s.connMu.Unlock()
}

func handleConnErr(config *config.Config, err error, conn *Conn) error {
	if err == nil {
		return nil
	}
	if err == io.EOF {
		internal.Debugf(config, "%s closed the connection", conn.RemoteAddr())
	} else if err, ok := err.(net.Error); ok && err.Timeout() {
		internal.Logf("%s timed out", conn.RemoteAddr())
	} else if err != nil {
		conn.setState(connStateFailed)

		log.Printf("error handling connection: %+v", err)
	}
	return err
}

func (s *SocketServer) handleConnection(conn *Conn) {
	s.q.Stats.Incr("connections")

	defer func() {
		s.q.Stats.Decr("connections")

		s.removeConn(conn)
		conn.close()
	}()

	for {
		if s.isShuttingDown() {
			break
		}

		if err := conn.setWaitForCmdDeadline(); err != nil {
			log.Printf("%s error: %+v", conn.RemoteAddr(), err)
			return
		}

		// wait for a command
		cmd, err := s.readCommand(conn)
		if cerr := handleConnErr(s.config, err, conn); cerr != nil {
			return
		}
		internal.Debugf(s.config, "%s<-%s: %s", conn.LocalAddr(), conn.RemoteAddr(), cmd)

		// just waited for io, so check if we're in shutdown
		if s.isShuttingDown() {
			break
		}

		// push to event queue and wait for a result
		resp, err := s.q.PushCommand(cmd)
		conn.readerC = resp.ReaderC
		s.q.Stats.Incr("total_commands")
		if cerr := handleConnErr(s.config, err, conn); cerr != nil {
			s.q.Stats.Incr("command_errors")
			return
		}

		// now we've waited to hear back from the event queue, so check if we're in shutdown again
		if s.isShuttingDown() {
			break
		}

		if s.handleShutdownRequest(conn, cmd, resp) {
			return
		}

		// send a response
		if werr := conn.setWriteDeadline(); werr != nil {
			s.q.Stats.Incr("connection_errors")
			log.Printf("error setting %s write deadline: %+v", conn.RemoteAddr(), werr)
			return
		}

		respBytes := resp.Bytes()
		n, err := conn.write(respBytes)
		s.q.Stats.Add("total_bytes_written", int64(n))

		if handleConnErr(s.config, err, conn) != nil {
			s.q.Stats.Incr("connection_errors")
			log.Printf("error writing to %s: %+v", conn.RemoteAddr(), err)
			return
		}
		if s.handleClose(conn, cmd, resp) {
			internal.Debugf(s.config, "closing %s", conn.RemoteAddr())
			break
		}

		// handle a single read or go into subscriber state
		s.handleRead(conn, cmd, resp)

		// clean up the request in preparation for the next, or go into
		// subscriber mode
		s.finishCommand(conn, cmd, resp)
	}
}

func (s *SocketServer) readCommand(conn *Conn) (*protocol.Command, error) {
	cmd, err := conn.pr.ReadCommand(conn)
	if cmd != nil && conn != nil {
		cmd.ConnID = conn.id
	}
	return cmd, err
}

func (s *SocketServer) finishCommand(conn *Conn, cmd *protocol.Command, resp *protocol.Response) {
	if cmd.Name == protocol.CmdRead {
		return
	}

	conn.setState(connStateInactive)
}

func (s *SocketServer) handleRead(conn *Conn, cmd *protocol.Command, resp *protocol.Response) {
	if cmd.Name != protocol.CmdRead {
		return
	}

	cmd.SignalReady()
	internal.Debugf(s.config, "reading log messages to %s", conn.RemoteAddr())
	conn.setState(connStateReading)
	// continue the loop to accept additional commands

	if err := conn.SetWriteDeadline(time.Time{}); err != nil {
		log.Printf("error setting write deadline to %s: %+v", conn.RemoteAddr(), err)
		return
	}

	go s.handleSubscriber(conn, cmd, resp)
}

func (s *SocketServer) handleClose(conn *Conn, cmd *protocol.Command, resp *protocol.Response) bool {
	return cmd.Name == protocol.CmdClose
}

func (s *SocketServer) handleShutdownRequest(conn *Conn, cmd *protocol.Command, resp *protocol.Response) bool {
	if cmd.Name == protocol.CmdShutdown && resp.Status == protocol.RespOK {
		s.removeConn(conn)
		s.Stop()
		return true
	}
	return false
}

func (s *SocketServer) handleSubscriber(conn *Conn, cmd *protocol.Command, resp *protocol.Response) {
	s.q.Stats.Incr("subscriptions")
	s.q.Stats.Incr("total_subscriptions")
	defer func() {
		s.q.Stats.Decr("subscriptions")
		conn.close()
	}()

	for {
		select {
		case r := <-resp.ReaderC:
			internal.Debugf(s.config, "%s <-reader: %+v", conn.RemoteAddr(), r)

			if err := s.sendReader(r, conn); err != nil {
				return
			}

		case <-cmd.Done:
			internal.Debugf(s.config, "%s: received <-done", conn.RemoteAddr())
			if err := conn.Flush(); err != nil {
				log.Printf("error flushing connection while closing: %+v", err)
			}
			return
		}
	}
}

func (s *SocketServer) sendReader(r io.Reader, conn *Conn) error {
	n, err := conn.readFrom(r)
	s.q.Stats.Add("total_bytes_written", int64(n))
	if err != nil {
		log.Printf("%s error: %+v", conn.RemoteAddr(), err)
		return err
	}

	// unwrap the os.file, same as go stdlib sendfile optimization logic
	lr, ok := r.(*io.LimitedReader)
	if ok {
		r = lr.R
	}
	f, ok := r.(*os.File)
	if ok {
		internal.Debugf(s.config, "%s: closing %s", conn.RemoteAddr(), f.Name())
		if err := f.Close(); err != nil {
			log.Printf("error closing reader to %s: %+v", conn.RemoteAddr(), err)
			return err
		}
	}
	return nil
}
