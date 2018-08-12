package events

import (
	"context"
	stderrors "errors"
	"io"
	"log"
	"time"

	"github.com/pkg/errors"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/server"
	"github.com/jeffrom/logd/transport"
)

// this file contains the core logic of the program. Commands come from the
// various inputs. They are handled and a response is given. For example, a
// message is received, it is written to a backend, and a log id is returned to
// the caller.

var errInvalidFormat = stderrors.New("Invalid command format")

const defaultTopic = "default"

// EventQ manages the receiving, processing, and responding to events.
type EventQ struct {
	conf         *config.Config
	in           chan *protocol.Request
	stopC        chan error
	shutdownC    chan error
	topics       *topics
	partArgBuf   *partitionArgList
	batchScanner *protocol.BatchScanner
	servers      []transport.Server
	Stats        *internal.Stats
	tmpBatch     *protocol.Batch
}

// NewEventQ creates a new instance of an EventQ
func NewEventQ(conf *config.Config) *EventQ {
	log.Printf("starting options: %+v", conf)

	q := &EventQ{
		conf:         conf,
		Stats:        internal.NewStats(),
		in:           make(chan *protocol.Request, 1000),
		stopC:        make(chan error),
		shutdownC:    make(chan error, 1),
		topics:       newTopics(conf),
		partArgBuf:   newPartitionArgList(conf), // partition arguments buffer
		batchScanner: protocol.NewBatchScanner(conf, nil),
		servers:      []transport.Server{},
		tmpBatch:     protocol.NewBatch(conf),
	}

	if conf.Hostport != "" {
		q.Register(server.NewSocket(conf.Hostport, conf))
	}

	return q
}

// Register adds a server to the event queue. The queue should be stopped when
// Register is called.
func (q *EventQ) Register(server transport.Server) {
	server.SetQPusher(q)
	q.servers = append(q.servers, server)
}

// GoStart begins handling messages
func (q *EventQ) GoStart() error {
	if err := q.topics.Setup(); err != nil {
		return err
	}

	go q.loop()

	for _, server := range q.servers {
		server.GoServe()
	}
	return nil
}

// Start begins handling messages, blocking until the application is closed
func (q *EventQ) Start() error {
	if err := q.GoStart(); err != nil {
		return err
	}

	select {
	case err := <-q.shutdownC:
		if err != nil {
			return err
		}
	}
	return nil
}

func (q *EventQ) drainShutdownC() {
	for {
		select {
		case <-q.shutdownC:
		default:
			return
		}
	}
}

func (q *EventQ) loop() { // nolint: gocyclo
	q.drainShutdownC()
	defer func() {
		q.shutdownC <- nil
	}()

	for {
		internal.Debugf(q.conf, "waiting for event")

		select {
		// new flow for handling requests passed in from servers
		case req := <-q.in:
			var resp *protocol.Response
			var err error
			internal.Debugf(q.conf, "request: %s", &req.Name)

			switch req.Name {
			case protocol.CmdBatch:
				resp, err = q.handleBatch(req)
			case protocol.CmdRead:
				resp, err = q.handleRead(req)
			case protocol.CmdTail:
				resp, err = q.handleTail(req)
			case protocol.CmdStats:
				resp, err = q.handleStats(req)
			case protocol.CmdClose:
				resp, err = q.handleClose(req)
			default:
				log.Printf("unhandled request type passed: %v", req.Name)
				resp, err = protocol.NewResponseErr(q.conf, req, protocol.ErrInvalid)
			}

			if err != nil && err != protocol.ErrNotFound {
				log.Printf("error handling %s request: %+v", &req.Name, err)
			}
			req.Respond(resp)

		case <-q.stopC:
			return
		}
	}
}

// Stop halts the event queue
func (q *EventQ) Stop() error {
	var err error
	for _, server := range q.servers {
		if serr := server.Stop(); serr != nil {
			if err == nil {
				err = serr
			}
			log.Printf("shutdown error: %+v", serr)
		}
	}

	select {
	case q.stopC <- err:
	case <-time.After(500 * time.Millisecond):
		log.Printf("event queue failed to stop properly")
	}

	return nil
}

func (q *EventQ) handleBatch(req *protocol.Request) (*protocol.Response, error) {
	resp := protocol.NewResponse(q.conf)
	q.tmpBatch.Reset()
	batch, err := q.tmpBatch.FromRequest(req)
	if err != nil {
		return errResponse(q.conf, req, resp, err)
	}

	topic, err := q.topics.get(batch.Topic())
	if err != nil {
		return errResponse(q.conf, req, resp, err)
	}

	// set next write partition if needed
	if topic.parts.shouldRotate(req.FullSize()) {
		nextStartOffset := topic.parts.nextOffset()
		if sperr := topic.logw.SetPartition(nextStartOffset); sperr != nil {
			return errResponse(q.conf, req, resp, sperr)
		}
	}
	// write the log
	_, err = topic.logw.Write(req.Bytes())
	if err != nil {
		return errResponse(q.conf, req, resp, err)
	}

	// update log state
	respOffset := topic.parts.nextOffset()
	if aerr := topic.parts.addBatch(batch, req.FullSize()); aerr != nil {
		return errResponse(q.conf, req, resp, aerr)
	}

	// respond
	cr := protocol.NewClientBatchResponse(q.conf, respOffset, 1)
	_, err = req.WriteResponse(resp, cr)
	if err != nil {
		return errResponse(q.conf, req, resp, err)
	}

	return resp, nil
}

func (q *EventQ) handleRead(req *protocol.Request) (*protocol.Response, error) {
	resp := protocol.NewResponse(q.conf)
	readreq, err := protocol.NewRead(q.conf).FromRequest(req)
	if err != nil {
		return errResponse(q.conf, req, resp, err)
	}

	topic, err := q.topics.get(readreq.Topic())
	if err != nil {
		return errResponse(q.conf, req, resp, err)
	}

	partArgs, err := q.gatherReadArgs(topic, readreq.Offset, readreq.Messages)
	if err != nil {
		// fmt.Println("gatherReadArgs error:", err)

		// TODO test this. When the offset pointing to the very end of the file
		// is requested (which happens often when reading forever), we get
		// io.ErrUnexpectedEOF
		if err == io.ErrUnexpectedEOF {
			return errResponse(q.conf, req, resp, protocol.ErrNotFound)
		}
		return errResponse(q.conf, req, resp, err)
	}

	// respond OK
	cr := protocol.NewClientBatchResponse(q.conf, readreq.Offset, partArgs.nbatches)
	_, err = req.WriteResponse(resp, cr)
	if err != nil {
		return errResponse(q.conf, req, resp, err)
	}

	// respond with the batch(es)
	for i := 0; i < partArgs.nparts; i++ {
		args := partArgs.parts[i]
		p, gerr := topic.parts.logp.Get(args.offset, args.delta, args.limit)
		if gerr != nil {
			return errResponse(q.conf, req, resp, gerr)
		}

		if aerr := resp.AddReader(p); aerr != nil {
			return errResponse(q.conf, req, resp, aerr)
		}
	}

	return resp, nil
}

func (q *EventQ) handleTail(req *protocol.Request) (*protocol.Response, error) {
	resp := protocol.NewResponse(q.conf)
	tailreq, err := protocol.NewTail(q.conf).FromRequest(req)
	if err != nil {
		return errResponse(q.conf, req, resp, err)
	}

	topic, err := q.topics.get(tailreq.Topic())
	if err != nil {
		return errResponse(q.conf, req, resp, err)
	}

	firstPart := topic.parts.parts[0]
	if firstPart.size <= 0 {
		return errResponse(q.conf, req, resp, protocol.ErrNotFound)
	}
	off := firstPart.startOffset

	partArgs, err := q.gatherReadArgs(topic, off, tailreq.Messages)
	if err != nil {
		return errResponse(q.conf, req, resp, err)
	}

	// respond OK
	cr := protocol.NewClientBatchResponse(q.conf, off, partArgs.nbatches)
	_, err = req.WriteResponse(resp, cr)
	if err != nil {
		return errResponse(q.conf, req, resp, err)
	}

	// respond with the batch(es)
	for i := 0; i < partArgs.nparts; i++ {
		args := partArgs.parts[i]
		p, gerr := topic.parts.logp.Get(args.offset, args.delta, args.limit)
		if gerr != nil {
			return errResponse(q.conf, req, resp, gerr)
		}

		if aerr := resp.AddReader(p); aerr != nil {
			return errResponse(q.conf, req, resp, aerr)
		}
	}
	return resp, nil
}

func (q *EventQ) handleStats(req *protocol.Request) (*protocol.Response, error) {
	resp := protocol.NewResponse(q.conf)
	cr := protocol.NewClientMultiResponse(q.conf, q.Stats.Bytes())
	_, err := req.WriteResponse(resp, cr)
	if err != nil {
		return errResponse(q.conf, req, resp, err)
	}
	return resp, nil
}

func (q *EventQ) handleClose(req *protocol.Request) (*protocol.Response, error) {
	resp := protocol.NewResponse(q.conf)
	cr := protocol.NewClientOKResponse(q.conf)
	_, err := req.WriteResponse(resp, cr)
	if err != nil {
		return errResponse(q.conf, req, resp, err)
	}
	return resp, nil
}

func (q *EventQ) gatherReadArgs(topic *topic, offset uint64, messages int) (*partitionArgList, error) {
	soff, delta, err := topic.parts.lookup(offset)
	// fmt.Printf("%v\ngatherReadArgs: offset: %d, partition: %d, delta: %d, err: %v\n", topic.parts, offset, soff, delta, err)
	if err != nil {
		return nil, err
	}

	q.partArgBuf.reset()
	scanner := q.batchScanner
	n := 0
	currstart := soff
Loop:
	for n < messages {
		p, gerr := topic.parts.logp.Get(currstart, delta, 0)
		if gerr != nil {
			// if we've successfully read anything, we've read the last
			// partition by now
			if q.partArgBuf.nparts > 0 {
				// fmt.Println("all done", q.partArgBuf.nparts)
				return q.partArgBuf, nil
			}
			return nil, gerr
		}
		defer p.Close()

		scanner.Reset(p)
		for scanner.Scan() {
			q.partArgBuf.nbatches++
			b := scanner.Batch()
			n += b.Messages
			if n >= messages {
				q.partArgBuf.add(currstart, delta, scanner.Scanned())
				// fmt.Println("scanned enough", currstart, q.partArgBuf.parts[:q.partArgBuf.nparts])
				break Loop
			}
		}
		// fmt.Println("finished part", currstart, q.partArgBuf.parts[:q.partArgBuf.nparts])

		serr := scanner.Error()
		// if we've read a partition and and we haven't read any messages, it's
		// an error. probably an incorrect offset near the end of the partition
		if serr == io.EOF && n > 0 {
			q.partArgBuf.add(currstart, delta, p.Size()-delta)
			currstart = p.Offset() + uint64(p.Size())
			delta = 0
			// fmt.Println("next part", currstart, q.partArgBuf.parts[:q.partArgBuf.nparts])
		} else if serr == io.EOF {
			return nil, io.ErrUnexpectedEOF
		} else if serr != nil {
			return nil, serr
		}
	}

	return q.partArgBuf, nil
}

// handleShutdown handles a shutdown request
func (q *EventQ) handleShutdown() error {
	// check if shutdown command is allowed and wait to finish any outstanding
	// work here
	// TODO try all shutdowns or give up after the first error?

	if err := q.topics.Shutdown(); err != nil {
		return err
	}
	return nil
}

// PushRequest adds a request event to the queue, and waits for a response.
// Called by server conn goroutines.
func (q *EventQ) PushRequest(ctx context.Context, req *protocol.Request) (*protocol.Response, error) {
	select {
	case q.in <- req:
	case <-ctx.Done():
		internal.Debugf(q.conf, "request %s cancelled", req)
		return nil, errors.New("request cancelled")
	}

	select {
	case resp := <-req.Responded():
		return resp, nil
	case <-ctx.Done():
		internal.Debugf(q.conf, "request %s cancelled while waiting for a response", req)
		return nil, errors.New("request cancelled")
	}
}

func errResponse(conf *config.Config, req *protocol.Request, resp *protocol.Response, err error) (*protocol.Response, error) {
	clientResp := protocol.NewClientErrResponse(conf, err)
	if _, werr := req.WriteResponse(resp, clientResp); werr != nil {
		return resp, werr
	}
	return resp, err
}
