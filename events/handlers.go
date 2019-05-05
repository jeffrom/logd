package events

import (
	"context"
	"log"
	"sync"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/server"
	"github.com/jeffrom/logd/transport"
)

var blockingReqs = map[protocol.CmdType]bool{
	protocol.CmdBatch: true,
	protocol.CmdRead:  true,
	protocol.CmdTail:  true,
}

// Handlers is a map of event queues, one for each topic as well as one for
// non-blocking requests.
type Handlers struct {
	conf      *config.Config
	h         map[string]*eventQ
	mu        sync.Mutex // for h
	asyncQ    *eventQ
	topics    *topics
	whitelist map[string]bool
	servers   []transport.Server
	shutdownC chan error
}

// NewHandlers returns a new instance of *Handlers.
func NewHandlers(conf *config.Config) *Handlers {
	log.Printf("starting options: %+v", conf)

	h := &Handlers{
		conf:      conf,
		h:         make(map[string]*eventQ),
		asyncQ:    newEventQ(conf),
		topics:    newTopics(conf),
		whitelist: make(map[string]bool),
		servers:   []transport.Server{},
		shutdownC: make(chan error, 1),
	}

	for _, tname := range conf.TopicWhitelist {
		h.whitelist[tname] = true
	}

	if conf.Host != "" {
		h.Register(server.NewSocket(conf.Host, conf))
	}

	if conf.HttpHost != "" {
		h.Register(server.NewHttp(conf))
	}

	return h
}

// Register adds a server to the event queue. The queue should be stopped when
// Register is called.
func (h *Handlers) Register(server transport.Server) {
	server.SetHandler(h)
	h.servers = append(h.servers, server)
}

// GoStart begins handling messages
func (h *Handlers) GoStart() error {
	h.drainShutdownC()
	if err := h.topics.Setup(); err != nil {
		return err
	}

	if err := h.asyncQ.GoStart(); err != nil {
		return err
	}

	h.mu.Lock()
	for name, topic := range h.topics.m {
		q := newEventQ(h.conf)
		q.setTopic(topic)
		if err := q.GoStart(); err != nil {
			h.mu.Unlock()
			return err
		}
		h.h[name] = q
	}
	h.mu.Unlock()

	for _, server := range h.servers {
		server.GoServe()
	}
	return nil
}

func (h *Handlers) drainShutdownC() {
	for {
		select {
		case <-h.shutdownC:
		default:
			return
		}
	}
}

func (h *Handlers) Start() error {
	if err := h.GoStart(); err != nil {
		return err
	}

	select {
	case err := <-h.shutdownC:
		if err != nil {
			return err
		}
	}
	return nil
}

// PushRequest implements transport.RequestHandler.
func (h *Handlers) PushRequest(ctx context.Context, req *protocol.Request) (*protocol.Response, error) {
	if ok, _ := blockingReqs[req.Name]; ok {
		return h.pushBlockingRequest(ctx, req)
	}
	return h.asyncQ.PushRequest(ctx, req)
}

func (h *Handlers) pushBlockingRequest(ctx context.Context, req *protocol.Request) (*protocol.Response, error) {
	name := req.Topic()
	if name == "" {
		return h.asyncQ.PushRequest(ctx, req)
	}

	h.mu.Lock()
	q, ok := h.h[name]
	h.mu.Unlock()
	if ok {
		// if req.Name == protocol.CmdRead || req.Name == protocol.CmdTail {
		// 	resp, err := q.handleRequest(req)
		// 	if err == protocol.ErrNotFound {
		// 		err = nil
		// 	}
		// 	return resp, err
		// }
		return q.PushRequest(ctx, req)
	}

	// create a new topic if there isn't already one
	if req.Name == protocol.CmdBatch {
		// make sure we only create one new topic so we don't lose messages or
		// do extra work.
		h.mu.Lock()
		if q, ok := h.h[name]; ok {
			h.mu.Unlock()
			return q.PushRequest(ctx, req)
		}

		if err := h.addTopicAllowed(name); err != nil {
			resp, werr := errResponse(h.conf, req, req.Response, err)
			if werr != nil && werr != err {
				h.mu.Unlock()
				return resp, werr
			}
			h.mu.Unlock()
			return resp, nil
		}

		q := newEventQ(h.conf)
		topic, err := h.topics.add(name)
		if err != nil {
			h.mu.Unlock()
			return nil, err
		}
		q.setTopic(topic)
		if err := q.GoStart(); err != nil {
			h.mu.Unlock()
			return nil, err
		}

		h.h[name] = q
		h.mu.Unlock()
		return q.PushRequest(ctx, req)
	}
	return h.asyncQ.PushRequest(ctx, req)
}

func (h *Handlers) addTopicAllowed(name string) error {
	if h.conf.MaxTopics > 0 && len(h.h) >= h.conf.MaxTopics {
		return protocol.ErrMaxTopics
	}
	if len(h.whitelist) == 0 {
		return nil
	}
	if ok, _ := h.whitelist[name]; !ok {
		return protocol.ErrTopicNotAllowed
	}
	return nil
}

func (h *Handlers) Stop() error {
	defer func() {
		h.shutdownC <- nil
	}()
	internal.Debugf(h.conf, "shutting down")
	var firstErr error

	for _, server := range h.servers {
		if serr := internal.LogAndReturnError(server.Stop()); serr != nil {
			if firstErr == nil {
				firstErr = serr
			}
			log.Printf("shutdown error: %+v", serr)
		}
	}

	if err := internal.LogAndReturnError(h.asyncQ.Stop()); err != nil {
		if firstErr == nil {
			firstErr = err
		}
	}

	for _, q := range h.h {
		if err := internal.LogAndReturnError(q.Stop()); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	if err := internal.LogAndReturnError(h.topics.Shutdown()); err != nil {
		if firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
