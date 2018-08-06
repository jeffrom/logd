package transport

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/protocol"
)

type handlerFunc func(*protocol.Request) *protocol.Response

// MockRequestHandler implements a mock that implements RequestHandler interface
type MockRequestHandler struct {
	conf       *config.Config
	in         chan *protocol.Request
	pending    chan handlerFunc
	response   handlerFunc
	responsemu sync.Mutex
	npending   int32
	stopC      chan struct{}
	done       chan error
	failC      chan error
}

// NewMockRequestHandler returns a new instance of *MockRequestHandler
func NewMockRequestHandler(conf *config.Config) *MockRequestHandler {
	rh := &MockRequestHandler{
		conf:    conf,
		in:      make(chan *protocol.Request, 1000),
		stopC:   make(chan struct{}),
		pending: make(chan handlerFunc, 1000),
		done:    make(chan error),
		failC:   make(chan error, 1),
	}

	go rh.loop()

	return rh
}

func (rh *MockRequestHandler) loop() {
	for {
		select {
		case <-rh.stopC:
			// log.Println("<-stopC")
			go rh.shutdown()
			return
		case req := <-rh.in:
			// log.Println("<-in", req)
			rh.responsemu.Lock()
			if rh.response != nil {
				req.Respond(rh.response(req))
				rh.responsemu.Unlock()
				continue
			}
			rh.responsemu.Unlock()

			select {
			case cb := <-rh.pending:
				// log.Println("<-pending")
				req.Respond(cb(req))
				atomic.AddInt32(&rh.npending, -1)
			case <-time.After(100 * time.Millisecond):
				// log.Println("<-timeout")
				rh.failC <- errors.New("no response set after 100ms")
				return
			}
		}
	}
}

func (rh *MockRequestHandler) shutdown() {
	for i := 0; i < 8 && rh.npending > 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}

	err := rh.getErr()
	rh.done <- err
}

func (rh *MockRequestHandler) Stop() error {
	select {
	case rh.stopC <- struct{}{}:
	case <-time.After(100 * time.Millisecond):
		return errors.New("stop timed out")
	}

	select {
	case err := <-rh.done:
		return err
	case <-time.After(100 * time.Millisecond):
		return errors.New("stop timed out")
	}
	return rh.getErr()
}

func (rh *MockRequestHandler) getErr() error {
	select {
	case err := <-rh.failC:
		return err
	default:
		return nil
	}
}

func (rh *MockRequestHandler) setErr(err error) {
	if preverr := rh.getErr(); preverr != nil {
		log.Printf("replacing error: %+v", preverr)
	}
	rh.failC <- err
}

func (rh *MockRequestHandler) Expect(cb handlerFunc) {
	rh.pending <- cb
	atomic.AddInt32(&rh.npending, 1)
}

func (rh *MockRequestHandler) Respond(cb handlerFunc) {
	rh.responsemu.Lock()
	rh.response = cb
	rh.responsemu.Unlock()
}

// PushRequest implements RequestHandler interface
func (rh *MockRequestHandler) PushRequest(ctx context.Context, req *protocol.Request) (*protocol.Response, error) {
	select {
	case rh.in <- req:
	case <-ctx.Done():
		return nil, errors.New("request cancelled")
	}

	select {
	case resp := <-req.Responded():
		if resp == nil {
			return protocol.NewResponse(config.Default), errors.New("no response")
		}
		return resp, nil
	case <-ctx.Done():
		return nil, errors.New("request cancelled")
	}
	return nil, nil
}
