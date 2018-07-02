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
	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/transport"
)

// Socket handles socket connections
type Socket struct {
	conf *config.Config

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

	q transport.QPusher
}

// NewSocket will return a new instance of a log server
func NewSocket(addr string, conf *config.Config) *Socket {
	return &Socket{
		conf:      conf,
		addr:      addr,
		readyC:    make(chan struct{}),
		conns:     make(map[*Conn]bool),
		connIn:    make(chan *Conn, 1000),
		stopC:     make(chan struct{}),
		shutdownC: make(chan struct{}),
	}
}

// ListenAndServe starts serving requests
func (s *Socket) ListenAndServe() error {
	return s.listenAndServe(false)
}

// ListenAddr returns the listen address of the server.
func (s *Socket) ListenAddr() net.Addr {
	return s.ln.Addr()
}

// SetQPusher implements transport.Server
func (s *Socket) SetQPusher(q transport.QPusher) {
	s.q = q
}

func (s *Socket) listenAndServe(wait bool) error {
	var outerErr error

	if s.ln == nil {
		s.mu.Lock()
		s.ln, outerErr = net.Listen("tcp", s.addr)
		s.mu.Unlock()
		if outerErr != nil {
			return outerErr
		}
	}

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
			return s.Shutdown()
		case conn := <-s.connIn:
			go s.handleConnection(conn)
		}
	}
}

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
			internal.LogError(rawConn.Close())
			break
		}

		// s.q.Stats.Incr("total_connections")
		internal.Debugf(s.conf, "accept: %s", rawConn.RemoteAddr())

		conn := newServerConn(rawConn, s.conf)
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
		s.shutdownC <- struct{}{}
		log.Print("shutdown complete")
	}()

	s.mu.Lock()
	s.shuttingDown = true
	s.mu.Unlock()

	var err error

	wg := sync.WaitGroup{}
	s.connMu.Lock()
	for c := range s.conns {
		wg.Add(1)
		go func(c *Conn) {
			defer wg.Done()

			if c.isActive() {
				select {
				case <-c.done:
					internal.Debugf(s.conf, "%s(ACTIVE) closed gracefully", c.RemoteAddr())
				case <-time.After(time.Duration(s.conf.GracefulShutdownTimeout) * time.Millisecond):
					log.Printf("%s timed out", c.RemoteAddr())
				}
			} else {
				internal.Debugf(s.conf, "%s(%s): closed gracefully", c.RemoteAddr(), c.getState())
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
	s.stopC <- struct{}{}

	select {
	case <-s.shutdownC:
	case <-time.After(time.Duration(s.conf.GracefulShutdownTimeout) * time.Millisecond):
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
		internal.Debugf(s.conf, "error removing connection: %+v", err)
	}

	s.connMu.Lock()
	delete(s.conns, conn)
	s.connMu.Unlock()
}

func (s *Socket) setDisallowedCommands(cmds map[protocol.CmdType]bool) {
	s.disallowedCommands = cmds
}

func (s *Socket) handleConnection(conn *Conn) {
	// s.q.Stats.Incr("connections")

	var (
		ctx    context.Context
		cancel context.CancelFunc
	)

	ctx, cancel = context.WithCancel(context.Background())

	defer func() {
		cancel()
		// s.q.Stats.Decr("connections")

		if err := conn.close(); err != nil {
			internal.Debugf(s.conf, "error closing connection: %+v", err)
		}
		s.removeConn(conn)
	}()

	for {
		if s.isShuttingDown() {
			internal.Debugf(s.conf, "closing connection to %s due to shutdown", conn.RemoteAddr())
			break
		}

		if err := conn.setWaitForCmdDeadline(); err != nil {
			log.Printf("%s error: %+v", conn.RemoteAddr(), err)
			return
		}
		// wait for a new request. continue along if there isn't one. All
		// requests will be handled in this block, and after all commands have
		// been migrated, this will be the whole loop
		internal.Debugf(s.conf, "%s: waiting for request", conn.RemoteAddr())
		if req, err := s.waitForRequest(conn); req != nil && err == nil {
			internal.Debugf(s.conf, "%s: waited for request", conn.RemoteAddr())
			if _, rerr := req.ReadFrom(conn.br); rerr != nil {
				// conn.Flush()
				log.Printf("%s read error: %+v", conn.RemoteAddr(), rerr)
				return
			}
			internal.Debugf(s.conf, "%s: read request %v", conn.RemoteAddr(), req)
			resp, rerr := s.q.PushRequest(ctx, req)
			if rerr != nil {
				internal.LogError(conn.Flush())
				log.Printf("%s error: %+v", conn.RemoteAddr(), rerr)
				return
			}
			internal.Debugf(s.conf, "%s: got response: %+v", conn.RemoteAddr(), resp)

			n, rerr := s.sendResponse(conn, resp)
			if rerr != nil {
				internal.LogError(conn.Flush())
				log.Printf("%s response error: %+v", conn.RemoteAddr(), rerr)
				return
			}
			internal.Debugf(s.conf, "%s: sent response (%d bytes)", conn.RemoteAddr(), n)

			internal.LogError(conn.Flush())

			if req.Name == protocol.CmdClose {
				internal.Debugf(s.conf, "%s: closing", conn.RemoteAddr())
				return
			}
			continue
		} else if err != nil {
			if err != io.EOF {
				log.Printf("%s wait error: %+v", conn.RemoteAddr(), err)
			}
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
	return protocol.NewRequest(s.conf), nil
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
		internal.LogError(r.Close())
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
