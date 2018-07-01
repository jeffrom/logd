package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/events"
	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/protocol"
)

// Socket handles socket connections
type Socket struct {
	config *config.Config

	addr string
	ln   net.Listener
	mu   sync.Mutex

	conns  map[*Conn]bool
	connMu sync.Mutex
	connIn chan *Conn

	disallowedCommands map[protocol.CmdType]bool

	readyC       chan struct{}
	stopC        chan struct{}
	shutdownC    chan struct{}
	shuttingDown bool

	q *events.EventQ
}

// NewSocket will return a new instance of a log server
func NewSocket(addr string, config *config.Config) *Socket {
	log.Printf("starting options: %s", config)
	q := events.NewEventQ(config)
	if err := q.Start(); err != nil {
		panic(err)
	}

	return &Socket{
		config:    config,
		addr:      addr,
		readyC:    make(chan struct{}),
		conns:     make(map[*Conn]bool),
		connIn:    make(chan *Conn, 1000),
		stopC:     make(chan struct{}),
		shutdownC: make(chan struct{}),
		q:         q,
	}
}

// ListenAndServe starts serving requests
func (s *Socket) ListenAndServe() error {
	return s.listenAndServe(false)
}

// ListenAddress returns the listen address of the server.
func (s *Socket) ListenAddress() net.Addr {
	return s.ln.Addr()
}

func (s *Socket) listenAndServe(wait bool) error {
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
			return s.Shutdown()
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
func (s *Socket) ready() {
	<-s.readyC
}

func (s *Socket) isShuttingDown() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.shuttingDown
}

func (s *Socket) accept() {
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
			internal.IgnoreError(rawConn.Close())
			break
		}

		s.q.Stats.Incr("total_connections")
		internal.Debugf(s.config, "accept: %s", rawConn.RemoteAddr())

		conn := newServerConn(rawConn, s.config)
		s.addConn(conn)

		s.connIn <- conn
	}
}

// GoServe starts a server without blocking the current goroutine
func (s *Socket) GoServe() {
	go func() {
		if err := s.listenAndServe(true); err != nil {
			log.Printf("error serving: %v", err)
		}
	}()
	s.ready()
}

// Shutdown implements internal.LifecycleManager, shutting down the server
func (s *Socket) Shutdown() error {
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
					internal.Debugf(s.config, "%s(ACTIVE) closed gracefully", c.RemoteAddr())
				case <-time.After(time.Duration(s.config.GracefulShutdownTimeout) * time.Millisecond):
					log.Printf("%s timed out", c.RemoteAddr())
				}
			} else {
				internal.Debugf(s.config, "%s(%s): closed gracefully", c.RemoteAddr(), c.getState())
			}

			s.removeConn(c)
		}(c)
	}
	s.connMu.Unlock()
	wg.Wait()

	return err
}

// Conns returns a list of current connections. For debugging.
func (s *Socket) Conns() []*Conn {
	s.connMu.Lock()
	defer s.connMu.Unlock()

	conns := make([]*Conn, len(s.conns))
	i := 0
	for conn := range s.conns {
		conns[i] = conn
		i++
	}
	return conns
}

func (s *Socket) logConns() {
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
func (s *Socket) Stop() error {
	select {
	case s.stopC <- struct{}{}:
	}

	select {
	case <-s.shutdownC:
	}

	return nil
}

func (s *Socket) addConn(conn *Conn) {
	conn.setState(connStateInactive)
	s.connMu.Lock()
	s.conns[conn] = true
	s.connMu.Unlock()
}

func (s *Socket) removeConn(conn *Conn) {
	if err := conn.Close(); err != nil {
		internal.Debugf(s.config, "error removing connection: %+v", err)
	}

	s.connMu.Lock()
	delete(s.conns, conn)
	s.connMu.Unlock()
}

func (s *Socket) setDisallowedCommands(cmds map[protocol.CmdType]bool) {
	s.disallowedCommands = cmds
}

func (s *Socket) handleConnection(conn *Conn) {
	s.q.Stats.Incr("connections")

	var (
		ctx    context.Context
		cancel context.CancelFunc
	)

	ctx, cancel = context.WithCancel(context.Background())

	defer func() {
		cancel()
		s.q.Stats.Decr("connections")

		if err := conn.close(); err != nil {
			internal.Debugf(s.config, "error closing connection: %+v", err)
		}
		s.removeConn(conn)
	}()

	for {
		if s.isShuttingDown() {
			internal.Debugf(s.config, "closing connection to %s due to shutdown", conn.RemoteAddr())
			break
		}

		if err := conn.setWaitForCmdDeadline(); err != nil {
			log.Printf("%s error: %+v", conn.RemoteAddr(), err)
			return
		}
		// wait for a new request. continue along if there isn't one. All
		// requests will be handled in this block, and after all commands have
		// been migrated, this will be the whole loop
		internal.Debugf(s.config, "%s: waiting for request", conn.RemoteAddr())
		if req, err := s.waitForRequest(conn); req != nil && err == nil {
			internal.Debugf(s.config, "%s: waited for request", conn.RemoteAddr())
			if _, rerr := req.ReadFrom(conn.br); rerr != nil {
				// conn.Flush()
				log.Printf("%s read error: %+v", conn.RemoteAddr(), rerr)
				return
			}
			internal.Debugf(s.config, "%s: read request %v", conn.RemoteAddr(), req)
			resp, rerr := s.q.PushRequest(ctx, req)
			if rerr != nil {
				internal.IgnoreError(conn.Flush())
				log.Printf("%s error: %+v", conn.RemoteAddr(), rerr)
				return
			}
			internal.Debugf(s.config, "%s: got response: %+v", conn.RemoteAddr(), resp)

			n, rerr := s.sendResponse(conn, resp)
			if rerr != nil {
				internal.IgnoreError(conn.Flush())
				log.Printf("%s response error: %+v", conn.RemoteAddr(), rerr)
				return
			}
			internal.Debugf(s.config, "%s: sent response (%d bytes)", conn.RemoteAddr(), n)

			internal.IgnoreError(conn.Flush())
			continue
			// } else if terr, ok := err.(net.Error); ok && terr.Timeout() {
			// 	log.Printf("%s timeout, so passing through to legacy command handler: %+v", conn.RemoteAddr(), terr)
		} else if err != nil {
			log.Printf("%s wait error: %+v", conn.RemoteAddr(), err)
			return
		}
	}
}

// TODO should this take context and wait for ctx.Done()?
func (s *Socket) waitForRequest(conn *Conn) (*protocol.Request, error) {
	// PING\r\n (6 bytes) is the shortest possible valid request
	_, err := conn.br.Peek(6)
	if err != nil {
		return nil, err
	}
	return protocol.NewRequest(s.config), nil
}

func (s *Socket) sendResponse(conn *Conn, resp *protocol.Response) (int, error) {
	var r io.ReadCloser
	var err error
	var total int
	var readOne bool
	for {
		r, err = resp.ScanReader()
		if err != nil || r == nil {
			break
		}

		readOne = true

		n, serr := conn.readFrom(r)
		internal.IgnoreError(r.Close())
		total += int(n)
		if serr != nil {
			return total, serr
		}
	}

	if !readOne {
		log.Printf("%s: no readers in Response", conn.RemoteAddr())
		return conn.sendDefaultError()
	}
	return total, err
}
