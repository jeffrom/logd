package logd

// this file contains the core logic of the program. Commands come from the
// various inputs. They are handled and a response is given. For example, a
// message is received, it is written to a backend, and a log id is returned to
// the caller. Or, a tail command is received, and the caller receives a log
// stream.

// eventQ manages the receiving, processing, and responding to events.
type eventQ struct {
	config *Config
	in     chan *command
	close  chan struct{}
	log    Logger
}

func newEventQ(config *Config) *eventQ {
	q := &eventQ{
		config: config,
		in:     make(chan *command, 0),
		close:  make(chan struct{}, 0),
		log:    config.Logger,
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
			case cmdSleep:
				q.handleSleep(cmd)
			case cmdShutdown:
				if err := q.handleShutdown(cmd); err != nil {
					cmd.respC <- newResponse(respErr)
				} else {
					cmd.respC <- newResponse(respOK)
					close(q.close)
					close(q.in)
					return
				}
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
		if _, currID, err := q.log.WriteMessage(msg); err != nil {
			cmd.respC <- newResponse(respErr)
			break
		} else {
			id = currID
		}
	}

	resp := newResponse(respOK)
	resp.id = id
	cmd.respC <- resp
}

func (q *eventQ) handleRead(cmd *command) {

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

}

func (q *eventQ) handleSleep(cmd *command) {

}

func (q *eventQ) handleShutdown(cmd *command) error {
	return nil
}

func (q *eventQ) add(cmd *command) (*response, error) {
	q.in <- cmd
	resp := <-cmd.respC
	return resp, nil
}
