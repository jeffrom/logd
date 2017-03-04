package logd

import (
	"strconv"
)

// this file contains the core logic of the program. Commands come from the
// various inputs. They are handled and a response is given. For example, a
// message is received, it is written to a backend, and a log id is returned to
// the caller. Or, a tail command is received, and the caller receives a log
// stream.

// eventQ manages the receiving, processing, and responding to events.
type eventQ struct {
	config        *Config
	in            chan *command
	close         chan struct{}
	subscriptions map[chan *response]*subscription
	log           Logger
}

func newEventQ(config *Config) *eventQ {
	q := &eventQ{
		config:        config,
		in:            make(chan *command, 0),
		close:         make(chan struct{}, 0),
		subscriptions: make(map[chan *response]*subscription),
		log:           config.Logger,
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
			case cmdMsg:
				q.handleMsg(cmd)
			case cmdRead:
				q.handleRead(cmd)
			case cmdHead:
				q.handleHead(cmd)
			case cmdPing:
				q.handlePing(cmd)
			case cmdClose:
				q.handleClose(cmd)
			// case cmdSleep:
			// 	q.handleSleep(cmd)
			case cmdShutdown:
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
	q.close <- struct{}{}
	close(q.close)
	close(q.in)
	return nil
}

func (q *eventQ) handleMsg(cmd *command) {
	var id uint64
	for _, msg := range cmd.args {
		_, currID, err := q.log.WriteMessage(msg)
		if err != nil {
			cmd.respC <- newResponse(respErr)
			return
		}
		id = currID
	}

	resp := newResponse(respOK)
	resp.id = id
	cmd.respC <- resp

	for _, subs := range q.subscriptions {
		go func(subs *subscription) {
			for _, msg := range cmd.args {
				subs.send(newMessage(id, msg))
			}
		}(subs)
	}
}

func (q *eventQ) handleRead(cmd *command) {
	if len(cmd.args) != 2 {
		cmd.respC <- newResponse(respErr)
		return
	}

	startID, err := strconv.ParseUint(string(cmd.args[0]), 10, 64)
	if err != nil {
		cmd.respC <- newResponse(respErr)
		return
	}

	limit, err := strconv.ParseUint(string(cmd.args[1]), 10, 64)
	if err != nil {
		cmd.respC <- newResponse(respErr)
		return
	}

	resp := newResponse(respOK)
	resp.msgC = make(chan *message, 0)
	cmd.respC <- resp

	q.log.ReadFromID(resp.msgC, startID, int(limit))

	if limit == 0 { // read forever
		q.subscriptions[cmd.respC] = newSubscription(resp.msgC)
	}
}

func (q *eventQ) handleHead(cmd *command) {
	if id, err := q.log.Head(); err != nil {
		cmd.respC <- newResponse(respErr)
	} else {
		resp := newResponse(respOK)
		resp.id = id
		cmd.respC <- resp
	}
}

func (q *eventQ) handlePing(cmd *command) {
	cmd.respC <- newResponse(respOK)
}

func (q *eventQ) handleClose(cmd *command) {
	delete(q.subscriptions, cmd.respC)
	cmd.respC <- newResponse(respOK)
}

// func (q *eventQ) handleSleep(cmd *command) {
// }

func (q *eventQ) handleShutdown(cmd *command) error {
	// check if shutdown command is allowed and wait to finish any outstanding
	// work here
	return nil
}

func (q *eventQ) add(cmd *command) (*response, error) {
	q.in <- cmd
	resp := <-cmd.respC
	return resp, nil
}
