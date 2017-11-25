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

	conns  map[*conn]bool
	connMu sync.Mutex
	connIn chan *conn

	readTimeout  time.Duration
	writeTimeout time.Duration

	readyC       chan struct{}
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

	timeout := time.Duration(config.ServerTimeout) * time.Millisecond
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
	conn.Close()
	s.connMu.Lock()
	delete(s.conns, conn)
	s.connMu.Unlock()
}

func handleConnErr(config *Config, err error, conn *conn) error {
	if err == nil {
		return nil
	}
	if err == io.EOF {
		debugf(config, "%s closed the connection", conn.RemoteAddr())
	} else if err, ok := err.(net.Error); ok && err.Timeout() {
		stdlog(2, "%s timed out", conn.RemoteAddr())
	} else if err != nil {
		conn.setState(connStateFailed)

		log.Printf("error handling connection: %+v", err)
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

		if err := conn.setWaitForCmdDeadline(); err != nil {
			log.Printf("%s error: %+v", conn.RemoteAddr(), err)
			return
		}

		// wait for a command
		cmd, err := conn.pr.readCommand(conn)
		if cerr := handleConnErr(s.config, err, conn); cerr != nil {
			return
		}
		debugf(s.config, "%s<-%s: %s", conn.LocalAddr(), conn.RemoteAddr(), cmd)

		if s.isShuttingDown() {
			break
		}

		// push to event queue and wait for a result
		resp, err := s.q.pushCommand(cmd)
		conn.readerC = resp.readerC
		if cerr := handleConnErr(s.config, err, conn); cerr != nil {
			return
		}

		// for when another connection shut down the server while this one was
		// waiting for a response
		if s.isShuttingDown() {
			break
		}

		if cmd.name == CmdShutdown && resp.Status == RespOK {
			s.removeConn(conn)
			s.stop()
			return
		}

		if err := conn.setWriteDeadline(); err != nil {
			log.Printf("error setting %s write deadline: %+v", conn.RemoteAddr(), err)
			return
		}

		respBytes := resp.Bytes()
		if cmd.name == CmdClose {
			debugf(s.config, "closing %s", conn.RemoteAddr())
			if _, err := conn.write(respBytes); handleConnErr(s.config, err, conn) != nil {
				log.Printf("error closing connection to %s: %+v", conn.RemoteAddr(), err)
				return
			}
			break
		}

		if _, err := conn.write(respBytes); handleConnErr(s.config, err, conn) != nil {
			log.Printf("error writing to %s: %+v", conn.RemoteAddr(), err)
			return
		}
		cmd.signalReady()

		s.finishRequest(conn, cmd, resp)
	}
}

func (s *SocketServer) finishRequest(conn *conn, cmd *Command, resp *Response) {
	if cmd.name == CmdRead {
		debugf(s.config, "reading log messages to %s", conn.RemoteAddr())
		conn.setState(connStateReading)
		// continue the loop to accept additional commands

		if err := conn.SetWriteDeadline(time.Time{}); err != nil {
			log.Printf("error setting write deadline to %s: %+v", conn.RemoteAddr(), err)
			return
		}
		go s.handleSubscriber(conn, cmd, resp)
	} else {
		conn.setState(connStateInactive)
	}
}

func (s *SocketServer) handleSubscriber(conn *conn, cmd *Command, resp *Response) {
	defer conn.close()

	for {
		select {
		case r := <-resp.readerC:
			if _, err := conn.readFrom(r); err != nil {
				log.Printf("%s error: %+v", conn.RemoteAddr(), err)
				return
			}

			if flusher, ok := r.(protocolFlusher); ok && flusher.shouldFlush() {
				if err := conn.flush(); err != nil {
					log.Printf("%s error: %+v", conn.RemoteAddr(), err)
					return
				}
			}
		case <-cmd.done:
			conn.flush()
			return
		}
	}
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}
