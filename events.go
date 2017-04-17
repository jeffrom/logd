package logd

import (
	"fmt"
	"log"
	"strconv"
	"time"
)

// this file contains the core logic of the program. Commands come from the
// various inputs. They are handled and a response is given. For example, a
// message is received, it is written to a backend, and a log id is returned to
// the caller. Or, a tail command is received, and the caller receives a log
// stream.

// TODO use an array of send close <- struct{}{} functions to run on shutdown
// instead of doing each one manually

// eventQ manages the receiving, processing, and responding to events.
type eventQ struct {
	config        *Config
	in            chan *Command
	close         chan struct{}
	subscriptions map[chan *Response]*Subscription
	log           Logger
	// srv           Server
}

func newEventQ(config *Config) *eventQ {
	q := &eventQ{
		config:        config,
		in:            make(chan *Command, 0),
		close:         make(chan struct{}),
		subscriptions: make(map[chan *Response]*Subscription),
		log:           config.Logger,
		// srv:           config.Server,
	}
	return q
}

func (q *eventQ) start() error {
	go q.loop()
	return nil
}

func (q *eventQ) loop() {
	for {
		select {
		case cmd := <-q.in:
			debugf(q.config, "event: %s", cmd)

			switch cmd.name {
			case CmdMessage:
				q.handleMsg(cmd)
			case CmdRead:
				q.handleRead(cmd)
			case CmdHead:
				q.handleHead(cmd)
			case CmdPing:
				q.handlePing(cmd)
			case CmdClose:
				q.handleClose(cmd)
			case CmdSleep:
				q.handleSleep(cmd)
			case CmdShutdown:
				if err := q.handleShutdown(cmd); err != nil {
					cmd.respC <- newResponse(respErr)
				} else {
					cmd.respC <- newResponse(respOK)
					close(q.close)
					close(q.in)
				}
				return
			default:
				cmd.respC <- newResponse(respErr)
			}
		case <-q.close:
			return
		}
	}
}

func (q *eventQ) stop() error {
	select {
	case q.close <- struct{}{}:
	case <-time.After(500 * time.Millisecond):
		log.Printf("event queue failed to stop properly")
	}
	return nil
}

func (q *eventQ) handleMsg(cmd *Command) {
	var id uint64
	for _, msg := range cmd.args {
		_, currID, err := q.log.WriteMessage(msg)
		if err != nil {
			cmd.respond(newResponse(respErr))
			return
		}
		id = currID
	}

	resp := newResponse(respOK)
	resp.id = id
	cmd.respond(resp)

	for _, sub := range q.subscriptions {
		go func(sub *Subscription) {
			for _, msg := range cmd.args {
				sub.send(NewMessage(id, msg))
			}
		}(sub)
	}
}

func (q *eventQ) handleRead(cmd *Command) {
	if len(cmd.args) != 2 {
		cmd.respond(newResponse(respErr))
		return
	}

	startID, err := strconv.ParseUint(string(cmd.args[0]), 10, 64)
	if err != nil {
		cmd.respond(newResponse(respErr))
		return
	}

	limit, err := strconv.ParseUint(string(cmd.args[1]), 10, 64)
	if err != nil {
		cmd.respond(newResponse(respErr))
		return
	}

	resp := newResponse(respOK)
	resp.msgC = make(chan *Message, 0)
	cmd.respond(resp)

	err = q.log.ReadFromID(resp.msgC, startID, int(limit))
	panicOnError(err)

	if limit == 0 { // read forever
		q.subscriptions[cmd.respC] = newSubscription(resp.msgC, cmd.done)
	} else {
		cmd.finish()
	}
}

func (q *eventQ) handleHead(cmd *Command) {
	if id, err := q.log.Head(); err != nil {
		cmd.respond(newResponse(respErr))
	} else {
		resp := newResponse(respOK)
		resp.id = id
		cmd.respond(resp)
	}
}

func (q *eventQ) handlePing(cmd *Command) {
	cmd.respC <- newResponse(respOK)
}

func (q *eventQ) handleClose(cmd *Command) {
	if sub, ok := q.subscriptions[cmd.respC]; ok {
		sub.finish()
	}

	delete(q.subscriptions, cmd.respC)
	cmd.respond(newResponse(respOK))
	// cmd.finish()
}

func (q *eventQ) handleSleep(cmd *Command) {
	var msecs int
	_, err := fmt.Sscanf(string(cmd.args[0]), "%d", &msecs)
	panicOnError(err)

	select {
	case <-time.After(time.Duration(msecs) * time.Millisecond):
	case <-cmd.wake:
	}

	cmd.respond(newResponse(respOK))
}

func (q *eventQ) handleShutdown(cmd *Command) error {
	// check if shutdown command is allowed and wait to finish any outstanding
	// work here
	return nil
}

func (q *eventQ) pushCommand(cmd *Command) (*Response, error) {
	q.in <- cmd
	resp := <-cmd.respC
	return resp, nil
}
