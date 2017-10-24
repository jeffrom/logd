package logd

import (
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

// Server is an interface for interaction with clients
type Server interface {
	Respond(cmd *Command, resp *Response) error
	Send(sub *Subscription, msg *Message) error
}

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

	stopC        chan struct{}
	shutdownC    chan struct{}
	shuttingDown bool

	q       *eventQ
	replica *Replica

	stats    *stats
	statlock sync.RWMutex
}

// NewServer will return a new instance of a log server
func NewServer(addr string, config *Config) *SocketServer {
	debugf(config, "starting options: %s", config)
	q := newEventQ(config)
	if err := q.start(); err != nil {
		panic(err)
	}

	timeout := time.Duration(time.Duration(config.ServerTimeout) * time.Millisecond)
	return &SocketServer{
		config:       config,
		addr:         addr,
		readyC:       make(chan struct{}),
		conns:        make(map[*conn]bool),
		connIn:       make(chan *conn, 0),
		readTimeout:  timeout,
		writeTimeout: timeout,
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
			s.logConns()
			return s.shutdown()
		case conn := <-s.connIn:
			go s.handleClient(conn)
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
	<-s.readyC
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

		debugf(s.config, "accept: %s", rawConn.RemoteAddr())

		conn := newServerConn(rawConn, s.config)
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

	s.mu.Lock()
	s.shuttingDown = true
	s.mu.Unlock()

	err := s.q.stop()

	wg := sync.WaitGroup{}
	s.connMu.Lock()
	for c := range s.conns {
		wg.Add(1)
		go func(c *conn) {
			defer wg.Done()

			if c.isActive() {
				select {
				case <-c.done:
					debugf(s.config, "%s closed gracefully", c.RemoteAddr())
				case <-time.After(1 * time.Second): // XXX config timeout
					log.Printf("%s timed out", c.RemoteAddr())
				}
			}

			c.Conn.Close()

			s.connMu.Lock()
			delete(s.conns, c)
			s.connMu.Unlock()
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

func (s *SocketServer) stop() {
	s.stopC <- struct{}{}
	<-s.shutdownC
}

func (s *SocketServer) addConn(conn *conn) {
	conn.setState(connStateInactive)
	s.connMu.Lock()
	s.conns[conn] = true
	s.connMu.Unlock()
}

func (s *SocketServer) removeConn(conn *conn) {
	s.connMu.Lock()
	delete(s.conns, conn)
	s.connMu.Unlock()
}

func (s *SocketServer) handleConnErr(err error, conn *conn) error {
	if err == io.EOF {
		debugf(s.config, "%s closed the connection", conn.RemoteAddr())
	} else if err, ok := err.(net.Error); ok && err.Timeout() {
		stdlog(2, "%s timed out", conn.RemoteAddr())
	} else if err != nil {
		conn.setState(connStateFailed)

		// XXX
		panic(err)
	}
	return err
}

func (s *SocketServer) handleClient(conn *conn) {
	counts.Add("clients", 1)

	defer func() {
		counts.Add("clients", -1)

		s.removeConn(conn)
		conn.close()
	}()

	for {
		if s.isShuttingDown() {
			break
		}

		if conn.getState() != connStateReading {
			err := conn.SetReadDeadline(time.Now().Add(s.readTimeout))
			if cerr := s.handleConnErr(err, conn); cerr != nil {
				return
			}

			conn.setState(connStateActive)
		}
		cmd, err := conn.pr.readCommand()
		if cerr := s.handleConnErr(err, conn); cerr != nil {
			return
		}
		debugf(s.config, "%s<-%s: %s", conn.LocalAddr(), conn.RemoteAddr(), cmd)

		if s.isShuttingDown() {
			break
		}

		resp, err := s.q.pushCommand(cmd)
		if cerr := s.handleConnErr(err, conn); cerr != nil {
			return
		}

		if cmd.name == CmdShutdown && resp.Status == RespOK {
			conn.Close()
			s.connMu.Lock()
			delete(s.conns, conn)
			s.connMu.Unlock()
			s.stop()
			return
		}

		err = conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))
		if cerr := s.handleConnErr(err, conn); cerr != nil {
			return
		}

		respBytes := resp.Bytes()
		if cmd.name == CmdClose {
			debugf(s.config, "close %s", conn.RemoteAddr())
			conn.write(respBytes)
			break
		} else {
			_, err = conn.write(respBytes)
			if cerr := s.handleConnErr(err, conn); cerr != nil {
				return
			}
		}
		// debugf(s.config, "%s->%s: %q", conn.LocalAddr(), conn.RemoteAddr(), respBytes)

		if cmd.name == CmdRead {
			debugf(s.config, "sending log messages to %s", conn.RemoteAddr())
			conn.setState(connStateReading)
			// continue the loop to accept additional commands

			if err := conn.SetWriteDeadline(time.Time{}); err != nil {
				panic(err)
			}
			go s.handleSubscriber(conn, cmd, resp)
		} else {
			conn.setState(connStateInactive)
		}

		if s.isShuttingDown() {
			break
		}
	}
}

func (s *SocketServer) handleSubscriber(conn *conn, cmd *Command, resp *Response) {
	for {
		select {
		case msg := <-resp.msgC:
			// serialized := []byte(fmt.Sprintf("+%d %d %s\r\n", msg.id, len(msg.body), msg.body))
			bytes := append([]byte("+"), msg...)
			conn.write(bytes)
		case <-cmd.done:
			conn.write([]byte("+EOF\r\n"))
			return
		}
	}
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}
