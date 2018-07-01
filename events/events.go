package events

import (
	"context"
	"io"
	"log"
	"time"

	"github.com/pkg/errors"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/logger"
	"github.com/jeffrom/logd/protocol"
	"github.com/jeffrom/logd/server"
	"github.com/jeffrom/logd/transport"
)

// this file contains the core logic of the program. Commands come from the
// various inputs. They are handled and a response is given. For example, a
// message is received, it is written to a backend, and a log id is returned to
// the caller.

//
// abstract error types
//

// EventQ manages the receiving, processing, and responding to events.
type EventQ struct {
	conf         *config.Config
	in           chan *protocol.Request
	stopC        chan error
	shutdownC    chan error
	parts        *partitions
	partArgBuf   *partitionArgList
	logw         logger.LogWriter
	batchScanner *protocol.BatchScanner
	servers      []transport.Server
	Stats        *internal.Stats
}

// NewEventQ creates a new instance of an EventQ
func NewEventQ(conf *config.Config) *EventQ {
	log.Printf("starting options: %s", conf)

	logp := logger.NewPartitions(conf)
	q := &EventQ{
		conf:         conf,
		Stats:        internal.NewStats(),
		in:           make(chan *protocol.Request, 1000),
		stopC:        make(chan error),
		shutdownC:    make(chan error),
		parts:        newPartitions(conf, logp), // partition state manager
		partArgBuf:   newPartitionArgList(conf), // partition arguments buffer
		logw:         logger.NewWriter(conf),
		batchScanner: protocol.NewBatchScanner(conf, nil),
		servers:      []transport.Server{},
	}

	if conf.Hostport != "" {
		q.servers = append(q.servers, server.NewSocket(conf.Hostport, conf))
	}

	return q
}

// GoStart begins handling messages
func (q *EventQ) GoStart() error {
	// TODO refactor socket to register LifecycleManagers with events so events
	// can control shutdown order of loggers AND servers
	if lc, ok := q.parts.logp.(internal.LifecycleManager); ok {
		if err := lc.Setup(); err != nil {
			return err
		}
	}
	if err := q.setupPartitions(); err != nil {
		return err
	}

	if lc, ok := q.logw.(internal.LifecycleManager); ok {
		if err := lc.Setup(); err != nil {
			return err
		}
	}

	go q.loop()

	for _, server := range q.servers {
		server.SetQPusher(q)
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

func (q *EventQ) setupPartitions() error {
	q.parts.reset()
	parts, err := q.parts.logp.List()
	if err != nil {
		return err
	}

	for _, part := range parts {
		if err := q.parts.add(part.Offset(), part.Size()); err != nil {
			return err
		}
	}

	if len(parts) == 0 {
		if err := q.parts.add(0, 0); err != nil {
			return err
		}
	}

	head := q.parts.head
	if serr := q.logw.SetPartition(head.startOffset); serr != nil {
		return serr
	}

	log.Printf("Starting at %d (partition %d, delta %d)", q.parts.headOffset(), head.startOffset, head.size)
	return nil
}

func (q *EventQ) loop() { // nolint: gocyclo
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
			default:
				log.Printf("unhandled request type passed: %v", req.Name)
				continue
			}

			if err != nil {
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

	// select {
	// case err := <-q.shutdownC:
	// 	return err
	// case <-time.After(time.Duration(q.conf.GracefulShutdownTimeout) * time.Millisecond):
	// 	log.Printf("event queue failed to shutdown properly")
	// 	return errors.New("failed to shut down")
	// }
}

func (q *EventQ) handleBatch(req *protocol.Request) (*protocol.Response, error) {
	resp := protocol.NewResponse(q.conf)
	batch, err := protocol.NewBatch(q.conf).FromRequest(req)
	if err != nil {
		return errResponse(q.conf, req, resp, err)
	}

	// set next write partition if needed
	if q.parts.shouldRotate(req.FullSize()) {
		nextStartOffset := q.parts.nextOffset()
		if sperr := q.logw.SetPartition(nextStartOffset); sperr != nil {
			return errResponse(q.conf, req, resp, sperr)
		}
	}
	// write the log
	_, err = q.logw.Write(req.Bytes())
	if err != nil {
		return errResponse(q.conf, req, resp, err)
	}

	// update log state
	respOffset := q.parts.nextOffset()
	if aerr := q.parts.addBatch(batch, req.FullSize()); aerr != nil {
		return errResponse(q.conf, req, resp, aerr)
	}

	// respond
	cr := protocol.NewClientBatchResponse(q.conf, respOffset)
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

	partArgs, err := q.gatherReadArgs(readreq.Offset, readreq.Messages)
	if err != nil {
		// fmt.Println("gatherReadArgs error:", err)
		return errResponse(q.conf, req, resp, err)
	}

	// respond OK <offset>\r\n
	cr := protocol.NewClientBatchResponse(q.conf, readreq.Offset)
	_, err = req.WriteResponse(resp, cr)
	if err != nil {
		return errResponse(q.conf, req, resp, err)
	}

	// respond with the batch(es)
	for i := 0; i < partArgs.nparts; i++ {
		args := partArgs.parts[i]
		p, gerr := q.parts.logp.Get(args.offset, args.delta, args.limit)
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

	firstPart := q.parts.parts[0]
	if firstPart.size <= 0 {
		return errResponse(q.conf, req, resp, protocol.ErrNotFound)
	}
	off := firstPart.startOffset

	partArgs, err := q.gatherReadArgs(off, tailreq.Messages)
	if err != nil {
		return errResponse(q.conf, req, resp, err)
	}

	// respond OK <offset>\r\n
	cr := protocol.NewClientBatchResponse(q.conf, off)
	_, err = req.WriteResponse(resp, cr)
	if err != nil {
		return errResponse(q.conf, req, resp, err)
	}

	// respond with the batch(es)
	for i := 0; i < partArgs.nparts; i++ {
		args := partArgs.parts[i]
		p, gerr := q.parts.logp.Get(args.offset, args.delta, args.limit)
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

func (q *EventQ) gatherReadArgs(offset uint64, messages int) (*partitionArgList, error) {
	soff, delta, err := q.parts.lookup(offset)
	// fmt.Printf("%v\ngatherReadArgs: offset: %d, partition: %d, delta: %d, err: %v\n", q.parts, offset, soff, delta, err)
	if err != nil {
		return nil, err
	}

	q.partArgBuf.reset()
	scanner := q.batchScanner
	n := 0
	currstart := soff
Loop:
	for n < messages {
		p, gerr := q.parts.logp.Get(currstart, delta, 0)
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

var errInvalidFormat = errors.New("Invalid command format")

// func (q *EventQ) parseBatch(cmd *protocol.Command) error {
// 	if cmd.Batch == nil {
// 		return errors.New("missing batch")
// 	}
// 	return nil
// }

// func (q *EventQ) handleClose(cmd *protocol.Command) {
// 	if len(cmd.Args) != 0 {
// 		cmd.Respond(protocol.NewClientErrResponse(q.conf, protocol.ErrRespInvalid))
// 		return
// 	}

// 	cmd.Respond(protocol.NewResponse(q.conf, protocol.RespOK))
// }

// handleShutdown handles a shutdown request
func (q *EventQ) handleShutdown() error {
	// check if shutdown command is allowed and wait to finish any outstanding
	// work here
	// TODO try all shutdowns or give up after the first error?

	if lc, ok := q.logw.(internal.LifecycleManager); ok {
		if err := lc.Shutdown(); err != nil {
			return err
		}
	}
	if lc, ok := q.parts.logp.(internal.LifecycleManager); ok {
		if err := lc.Shutdown(); err != nil {
			return err
		}
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
