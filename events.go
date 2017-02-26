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
}

func newEventQ(config *Config) *eventQ {
	q := &eventQ{
		config: config,
		in:     make(chan *command, 0),
		close:  make(chan struct{}, 0),
	}
	return q
}

func (q *eventQ) start() error {
	go q.doStart()
	return nil
}

func (q *eventQ) doStart() {
	for {
		select {
		case cmd := <-q.in:
			debugf(q.config, "event: %s", cmd)

			switch cmd.name {
			case cmdMsg:
			case cmdRead:
			case cmdHead:
			case cmdPing:
				cmd.resp <- newResponse(respOK)
			case cmdClose:
			case cmdSleep:
			case cmdShutdown:
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

func (q *eventQ) add(cmd *command) (*response, error) {
	q.in <- cmd
	resp := <-cmd.resp
	return resp, nil
}
