package transport

import (
	"context"
	"errors"
	"log"
	"sync/atomic"
	"time"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/protocol"
)

// MockRequestHandler implements a mock that implements RequestHandler interface
type MockRequestHandler struct {
	in       chan *protocol.Request
	pending  chan func(*protocol.Request) *protocol.Response
	npending int32
	stopC    chan struct{}
	done     chan error
	failC    chan error
}

// NewMockRequestHandler returns a new instance of *MockRequestHandler
func NewMockRequestHandler() *MockRequestHandler {
	rh := &MockRequestHandler{
		in:      make(chan *protocol.Request, 1000),
		stopC:   make(chan struct{}),
		pending: make(chan func(*protocol.Request) *protocol.Response, 1000),
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
			go rh.shutdown()
			return
		case req := <-rh.in:
			select {
			case cb := <-rh.pending:
				req.Respond(cb(req))
				atomic.AddInt32(&rh.npending, -1)
			case <-time.After(100 * time.Millisecond):
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
	rh.stopC <- struct{}{}

	select {
	case err := <-rh.done:
		return err
	case <-time.After(100 * time.Millisecond):
		return errors.New("timed out")
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

func (rh *MockRequestHandler) Expect(cb func(req *protocol.Request) *protocol.Response) {
	rh.pending <- cb
	atomic.AddInt32(&rh.npending, 1)
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
