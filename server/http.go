package server

import (
	"context"
	"expvar"
	"log"
	"net"
	"net/http"
	"net/http/pprof"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/transport"
)

// Http implements transport.Server interface.
type Http struct {
	conf *config.Config
	ln   net.Listener
	mux  *http.ServeMux
	srv  *http.Server
	h    transport.RequestHandler
}

// NewHttp returns a new instance of *Http.
func NewHttp(conf *config.Config) *Http {
	mux := http.NewServeMux()
	s := &Http{
		conf: conf,
		mux:  mux,
		srv: &http.Server{
			Handler: mux,
		},
	}
	s.setupHandlers()
	return s
}

// GoServe implements transport.Server interface.
func (s *Http) GoServe() {
	go func() {
		listener, err := net.Listen("tcp", s.conf.HttpHost)
		if err != nil {
			panic(err)
		}
		s.ln = listener

		log.Printf("Serving at %s", s.ln.Addr())
		if err := s.srv.Serve(s.ln); err != nil {
			// panic(err)
		}
	}()
}

func (s *Http) setupHandlers() {
	s.mux.HandleFunc("/debug/pprof/", pprof.Index)
	s.mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	s.mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	s.mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	s.mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	s.mux.Handle("/debug/vars", expvar.Handler())

	s.mux.Handle("/log", &logHandler{conf: s.conf, h: s.h})
}

// Stop implements transport.Server interface.
func (s *Http) Stop() error {
	log.Printf("Shutting down server at %s", s.ln.Addr())
	// TODO use a ctx with timeout to do a graceful shutdown
	return s.srv.Shutdown(context.Background())
}

// ListenAddr implements transport.Server interface.
func (s *Http) ListenAddr() net.Addr {
	return s.ln.Addr()
}

func (s *Http) SetHandler(h transport.RequestHandler) {
	s.h = h
}
