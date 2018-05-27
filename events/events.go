package events

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/pkg/errors"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/logger"
	"github.com/jeffrom/logd/protocol"
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
	currID       uint64
	in           chan *protocol.Command
	requestIn    chan *protocol.Request
	close        chan struct{}
	log          logger.Logger
	parts        *partitions
	partArgBuf   *partitionArgList
	logw         logger.LogWriterV2
	batchScanner *protocol.BatchScanner
	Stats        *internal.Stats
}

// NewEventQ creates a new instance of an EventQ
func NewEventQ(conf *config.Config) *EventQ {
	logp := logger.NewPartitions(conf)
	q := &EventQ{
		conf:         conf,
		Stats:        internal.NewStats(),
		in:           make(chan *protocol.Command, 1000),
		requestIn:    make(chan *protocol.Request, 1000),
		close:        make(chan struct{}),
		log:          logger.New(conf),          // old logger
		parts:        newPartitions(conf, logp), // partition state manager
		partArgBuf:   newPartitionArgList(conf), // partition arguments buffer
		logw:         logger.NewWriter(conf),    // new logger.Writer
		batchScanner: protocol.NewBatchScanner(conf, nil),
	}

	return q
}

// Start begins handling messages
func (q *EventQ) Start() error {
	if manager, ok := q.log.(logger.LogManager); ok {
		if err := manager.Setup(); err != nil {
			panic(err)
		}
	}

	head, err := q.log.Head()
	if err != nil {
		return err
	}
	q.currID = head + 1

	// V2 setup
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
	return nil
}

func (q *EventQ) setupPartitions() error {
	parts, err := q.parts.logp.List()
	if err != nil {
		return err
	}

	for _, part := range parts {
		q.parts.add(part.Offset(), part.Size())
	}

	if len(parts) == 0 {
		q.parts.add(0, 0)
	}

	head := q.parts.head
	if serr := q.logw.SetPartition(head.startOffset); serr != nil {
		return serr
	}

	log.Printf("Starting at %d (partition %d, delta %d)", q.parts.headOffset(), head.startOffset, head.size)
	return nil
}

func (q *EventQ) loop() {
	for {
		internal.Debugf(q.conf, "waiting for event")

		select {
		// new flow for handling requests passed in from servers
		case req := <-q.requestIn:
			var resp *protocol.ResponseV2
			var err error
			internal.Debugf(q.conf, "request: %s", &req.Name)

			switch req.Name {
			case protocol.CmdBatch:
				resp, err = q.handleBatch(req)
			case protocol.CmdReadV2:
				resp, err = q.handleReadV2(req)
			default:
				log.Printf("unhandled request type passed: %v", req.Name)
				continue
			}

			if err != nil {
				log.Printf("error handling %s request: %+v", &req.Name, err)
			}
			req.Respond(resp)

		case cmd := <-q.in:
			internal.Debugf(q.conf, "event: %s(%q)", cmd, cmd.Args)

			switch cmd.Name {
			case protocol.CmdMessage:
				q.handleMsg(cmd)
			case protocol.CmdRead:
				q.handleRead(cmd)
			case protocol.CmdTail:
				q.handleTail(cmd)
			case protocol.CmdHead:
				q.handleHead(cmd)
			case protocol.CmdStats:
				q.handleStats(cmd)
			case protocol.CmdPing:
				q.handlePing(cmd)
			case protocol.CmdClose:
				q.handleClose(cmd)
			case protocol.CmdSleep:
				q.handleSleep(cmd)
			case protocol.CmdShutdown:
				if err := q.HandleShutdown(cmd); err != nil {
					cmd.Respond(protocol.NewResponse(q.conf, protocol.RespErr))
				} else {
					cmd.Respond(protocol.NewResponse(q.conf, protocol.RespOK))
					// close(q.close)
					// close(q.in)
				}
			default:
				cmd.Respond(protocol.NewResponse(q.conf, protocol.RespErr))
			}
		case <-q.close:
			return
		}
	}
}

// Stop halts the event queue
func (q *EventQ) Stop() error {
	select {
	case q.close <- struct{}{}:
	case <-time.After(500 * time.Millisecond):
		log.Printf("event queue failed to stop properly")
	}
	return nil
}

func (q *EventQ) handleMsg(cmd *protocol.Command) {
	// TODO make the messages bytes once and reuse
	var msgs [][]byte
	id := q.currID - 1

	if len(cmd.Args) == 0 {
		cmd.Respond(protocol.NewClientErrResponse(q.conf, protocol.ErrRespNoArguments))
		return
	}

	pw := protocol.NewWriter()

	// TODO if any messages are invalid, throw out the whole bunch
	for _, msg := range cmd.Args {
		if len(msg) == 0 {
			cmd.Respond(protocol.NewClientErrResponse(q.conf, protocol.ErrRespEmptyMessage))
			return
		}

		id++
		msgb := pw.Message(protocol.NewMessage(id, msg))
		msgs = append(msgs, msgb)

		q.log.SetID(id)
		_, err := q.log.Write(msgb)
		if err != nil {
			log.Printf("Error: %+v", err)
			cmd.Respond(protocol.NewResponse(q.conf, protocol.RespErr))
			return
		}
	}
	q.currID = id + 1

	q.Stats.Incr("total_writes")

	resp := protocol.NewResponse(q.conf, protocol.RespOK)
	resp.ID = id
	cmd.Respond(resp)
}

func (q *EventQ) handleBatch(req *protocol.Request) (*protocol.ResponseV2, error) {
	resp := protocol.NewResponseV2(q.conf)
	batch, err := protocol.NewBatch(q.conf).FromRequest(req)
	if err != nil {
		return resp, err
	}

	if verr := batch.Validate(); verr != nil {
		return resp, verr
	}

	// rotate to next partition if needed
	if q.parts.shouldRotate(req.FullSize()) {
		nextStartOffset := q.parts.nextOffset()
		if sperr := q.logw.SetPartition(nextStartOffset); sperr != nil {
			return resp, sperr
		}
	}

	// write the log
	_, err = q.logw.Write(req.Bytes())
	if err != nil {
		return resp, err
	}

	// update log state
	respOffset := q.parts.nextOffset()
	q.parts.addBatch(batch, req.FullSize())

	// respond
	clientResp := protocol.NewClientBatchResponse(q.conf, respOffset)
	_, err = req.WriteResponse(resp, clientResp)
	if err != nil {
		return resp, err
	}

	return resp, nil
}

func (q *EventQ) handleReadV2(req *protocol.Request) (*protocol.ResponseV2, error) {
	resp := protocol.NewResponseV2(q.conf)
	readreq, err := protocol.NewRead(q.conf).FromRequest(req)
	if err != nil {
		return resp, err
	}

	if verr := readreq.Validate(); verr != nil {
		return resp, verr
	}

	partArgs, err := q.gatherReadArgs(readreq.Offset, readreq.Messages)
	if err != nil {
		return resp, err
	}

	for i := 0; i < partArgs.nparts; i++ {
		args := partArgs.parts[i]
		p, gerr := q.parts.logp.Get(args.offset, args.delta, args.limit)
		if gerr != nil {
			return resp, gerr
		}

		if aerr := resp.AddReader(p); aerr != nil {
			return resp, aerr
		}
	}

	return resp, nil
}

func (q *EventQ) gatherReadArgs(offset uint64, messages int) (*partitionArgList, error) {
	// check the offset/delta for range start
	// get the offset/limit for range end
	// partitions.Get the relevant partitions and add them to the client response
	soff, delta, err := q.parts.lookup(offset)
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
				return q.partArgBuf, nil
			}
			return nil, gerr
		}

		scanner.Reset(p)
		for scanner.Scan() {
			b := scanner.Batch()
			n += b.Messages
			if n >= messages {
				q.partArgBuf.add(currstart, delta, scanner.Scanned())
				p.Close()
				break Loop
			}
		}

		serr := scanner.Error()
		p.Close()
		if serr == io.EOF {
			q.partArgBuf.add(currstart, delta, p.Size())
			currstart = p.Offset() + uint64(p.Size()+1)
			delta = 0
		} else if serr != nil {
			return nil, serr
		}
	}

	return q.partArgBuf, nil
}

func (q *EventQ) handleRead(cmd *protocol.Command) {
	startID, limit, err := q.parseRead(cmd)
	if err != nil {
		internal.Debugf(q.conf, "invalid: %v", err)
		cmd.Respond(protocol.NewClientErrResponse(q.conf, protocol.ErrRespInvalid))
		return
	}

	head, err := q.log.Head()
	if err != nil {
		log.Printf("error getting log head: %+v", err)
		cmd.Respond(protocol.NewErrResponse(q.conf, protocol.ErrRespServer))
		return
	}

	if startID > head {
		cmd.Respond(protocol.NewClientErrResponse(q.conf, protocol.ErrRespNotFound))
		return
	}

	end := startID + limit
	if limit == 0 {
		end = head
	} else if end > 1 {
		end--
	}

	iterator, err := q.log.Range(startID, end)
	if err != nil {
		if errors.Cause(err) == protocol.ErrNotFound {
			cmd.Respond(protocol.NewErrResponse(q.conf, protocol.ErrRespNotFound))
			internal.Debugf(q.conf, "id %d not found", startID)
		} else {
			cmd.Respond(protocol.NewErrResponse(q.conf, protocol.ErrRespServer))
			log.Printf("failed to handle read command: %+v", err)
		}
		return
	}

	q.Stats.Incr("total_reads")
	q.doRead(cmd, iterator)
}

func (q *EventQ) handleTail(cmd *protocol.Command) {
	startID, limit, err := q.parseRead(cmd)
	if err != nil {
		internal.Debugf(q.conf, "invalid: %v", err)
		cmd.Respond(protocol.NewClientErrResponse(q.conf, protocol.ErrRespInvalid))
		return
	}

	head, err := q.log.Head()
	if err != nil {
		log.Printf("error getting log head: %+v", err)
		cmd.Respond(protocol.NewErrResponse(q.conf, protocol.ErrRespServer))
		return
	}

	if startID > head {
		cmd.Respond(protocol.NewClientErrResponse(q.conf, protocol.ErrRespNotFound))
		return
	}

	end := startID + limit
	if limit == 0 {
		end = head
	}

	iterator, err := q.log.Range(startID, end)
	if err != nil {
		if errors.Cause(err) == protocol.ErrNotFound {
			internal.Debugf(q.conf, "id not found, reading from tail")

			tailID, terr := q.log.Tail()
			if terr != nil {
				log.Printf("failed to get log tail id: %+v", terr)
				cmd.Respond(protocol.NewErrResponse(q.conf, protocol.ErrRespServer))
				return
			}

			if limit != 0 {
				end = tailID + limit
			}
			iterator, err = q.log.Range(tailID, end)
			if err != nil {
				log.Printf("failed to read range from tail: %+v", err)
				cmd.Respond(protocol.NewErrResponse(q.conf, protocol.ErrRespServer))
				return
			}

			q.Stats.Incr("total_reads")
			q.doRead(cmd, iterator)
		} else {
			cmd.Respond(protocol.NewErrResponse(q.conf, []byte("")))
			log.Printf("failed to handle read command: %+v", err)
		}
		return
	}

	q.Stats.Incr("total_reads")
	q.doRead(cmd, iterator)
}

func (q *EventQ) doRead(cmd *protocol.Command, iterator logger.LogRangeIterator) {
	resp := protocol.NewResponse(q.conf, protocol.RespOK)
	cmd.Respond(resp)
	cmd.WaitForReady()

	for iterator.Next() {
		if err := iterator.Error(); err != nil {
			log.Printf("failed to read log range iterator: %+v", err)
			resp.SendEOF()
			return
		}
		q.sendChunk(iterator.LogFile(), resp.ReaderC)
	}

	resp.SendEOF()
}

func (q *EventQ) sendChunk(lf logger.LogReadableFile, readerC chan protocol.ReadPart) {
	size, limit, err := lf.SizeLimit()
	if err != nil {
		log.Printf("failed to get log size/limit: %+v", err)
		return
	}
	buflen := size
	if limit > 0 {
		buflen = limit
	}
	// buflen does not take seek position into account

	f := lf.AsFile()
	readerC <- protocol.NewPartReader(bytes.NewReader([]byte(fmt.Sprintf("+%d\r\n", buflen))))
	readerC <- protocol.NewPartReader(io.LimitReader(f, buflen))

	internal.Debugf(q.conf, "readerC <-%s: %d bytes", f.Name(), buflen)
}

var errInvalidFormat = errors.New("Invalid command format")

// func (q *EventQ) parseBatch(cmd *protocol.Command) error {
// 	if cmd.Batch == nil {
// 		return errors.New("missing batch")
// 	}
// 	return nil
// }

func (q *EventQ) parseRead(cmd *protocol.Command) (uint64, uint64, error) {
	if len(cmd.Args) != 2 {
		// cmd.Respond(protocol.NewResponse(respErr))
		return 0, 0, errInvalidFormat
	}

	startID, err := protocol.ParseNumber(cmd.Args[0])
	if err != nil {
		return 0, 0, err
	}

	limit, err := protocol.ParseNumber(cmd.Args[1])
	if err != nil {
		return 0, 0, err
	}
	return startID, limit, nil
}

func (q *EventQ) handleHead(cmd *protocol.Command) {
	if len(cmd.Args) != 0 {
		cmd.Respond(protocol.NewClientErrResponse(q.conf, protocol.ErrRespInvalid))
		return
	}

	if id, err := q.log.Head(); err != nil {
		cmd.Respond(protocol.NewResponse(q.conf, protocol.RespErr))
	} else {
		resp := protocol.NewResponse(q.conf, protocol.RespOK)
		resp.ID = id
		cmd.Respond(resp)
	}
}

func (q *EventQ) handleStats(cmd *protocol.Command) {
	if len(cmd.Args) != 0 {
		cmd.Respond(protocol.NewClientErrResponse(q.conf, protocol.ErrRespInvalid))
		return
	}

	resp := protocol.NewResponse(q.conf, protocol.RespOK)
	resp.Body = q.Stats.Bytes()

	cmd.Respond(resp)
}

func (q *EventQ) handlePing(cmd *protocol.Command) {
	if len(cmd.Args) != 0 {
		cmd.Respond(protocol.NewClientErrResponse(q.conf, protocol.ErrRespInvalid))
		return
	}

	cmd.Respond(protocol.NewResponse(q.conf, protocol.RespOK))
}

func (q *EventQ) handleClose(cmd *protocol.Command) {
	if len(cmd.Args) != 0 {
		cmd.Respond(protocol.NewClientErrResponse(q.conf, protocol.ErrRespInvalid))
		return
	}

	cmd.Respond(protocol.NewResponse(q.conf, protocol.RespOK))
}

func (q *EventQ) handleSleep(cmd *protocol.Command) {
	if len(cmd.Args) != 1 {
		cmd.Respond(protocol.NewClientErrResponse(q.conf, protocol.ErrRespInvalid))
		return
	}

	var msecs int
	_, err := fmt.Sscanf(string(cmd.Args[0]), "%d", &msecs)
	if err != nil {
		cmd.Respond(protocol.NewClientErrResponse(q.conf, protocol.ErrRespInvalid))
		return
	}

	select {
	case <-time.After(time.Duration(msecs) * time.Millisecond):
	case <-cmd.Wake:
	}

	cmd.Respond(protocol.NewResponse(q.conf, protocol.RespOK))
}

// HandleShutdown handles a shutdown request
func (q *EventQ) HandleShutdown(cmd *protocol.Command) error {
	// check if shutdown command is allowed and wait to finish any outstanding
	// work here
	if manager, ok := q.log.(logger.LogManager); ok {
		if err := manager.Shutdown(); err != nil {
			return err
		}
	}

	// V2
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

// PushCommand adds an event to the queue. Called by socket connection goroutines.
func (q *EventQ) PushCommand(ctx context.Context, cmd *protocol.Command) (*protocol.Response, error) {
	select {
	case q.in <- cmd:
	case <-ctx.Done():
		internal.Debugf(q.conf, "command %s cancelled", cmd)
		return nil, errors.New("command cancelled")
	}

	select {
	case resp := <-cmd.RespC:
		return resp, nil
	}
}

// PushRequest adds a request event to the queue, and waits for a response.
// Called by server conn goroutines.
func (q *EventQ) PushRequest(ctx context.Context, req *protocol.Request) (*protocol.ResponseV2, error) {
	select {
	case q.requestIn <- req:
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

// Subscription is used to tail logs
type Subscription struct {
	config  *config.Config
	readerC chan io.Reader
}

func newSubscription(config *config.Config, readerC chan io.Reader) *Subscription {
	return &Subscription{
		config:  config,
		readerC: readerC,
	}
}

func (subs *Subscription) send(msg []byte) {
	// fmt.Printf("<-bytes %q (subscription)\n", prettybuf(msg))
	subs.readerC <- bytes.NewReader(msg)
}
