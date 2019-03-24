package events

import (
	"bufio"
	"io"
	"log"
	"sync"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/logger"
	"github.com/jeffrom/logd/protocol"
)

var partitionArgListPool = sync.Pool{
	New: func() interface{} {
		return &partitionArgList{}
	},
}

// topics manages the topic filesystem for the event queues.
type topics struct {
	conf    *config.Config
	manager logger.TopicManager
	m       map[string]*topic
	mu      sync.Mutex // for m
}

func newTopics(conf *config.Config) *topics {
	return &topics{
		conf:    conf,
		manager: logger.NewTopics(conf),
		m:       make(map[string]*topic),
	}
}

func (t *topics) reset() {
	t.mu.Lock()
	t.m = make(map[string]*topic)
	t.mu.Unlock()
}

// Setup implements internal.LifecycleManager
func (t *topics) Setup() error {
	if m, ok := t.manager.(internal.LifecycleManager); ok {
		if err := m.Setup(); err != nil {
			return err
		}
	}

	if _, err := t.add("default"); err != nil {
		return err
	}

	topics, err := t.manager.List()
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	errC := make(chan error, len(topics))
	for _, topic := range topics {
		if topic == "default" {
			continue
		}

		wg.Add(1)
		go func(topic string) {
			defer wg.Done()
			if _, err := t.add(topic); err != nil {
				errC <- err
			}
		}(topic)
	}
	// fmt.Println(t.m)

	wg.Wait()

	// TODO log any additional errors
	select {
	case err := <-errC:
		return err
	default:
	}
	return nil
}

// Shutdown implements LifecycleManager
func (t *topics) Shutdown() error {
	var firstErr error
	t.mu.Lock()
	for _, topic := range t.m {
		err := topic.Shutdown()
		if err != nil && firstErr == nil {
			firstErr = err
		} else if err != nil {
			log.Printf("shutdown: %+v", err)
		}
	}
	t.mu.Unlock()
	t.reset()
	return firstErr
}

func (t *topics) add(name string) (*topic, error) {
	t.mu.Lock()
	topic, ok := t.m[name]
	t.mu.Unlock()
	if !ok {
		log.Printf("initializing topic: %s", name)
		if err := t.manager.Create(name); err != nil {
			return nil, err
		}
		topic = newTopic(t.conf, name)
		if err := topic.Setup(); err != nil {
			return nil, err
		}
		t.mu.Lock()
		t.m[name] = topic
		t.mu.Unlock()
	}
	return topic, nil
}

func (t *topics) get(name string) (*topic, error) {
	return t.add(name)
}

type topic struct {
	conf  *config.Config
	name  string
	parts *partitions
	idx   *queryIndex
	logp  logger.PartitionManager
	logw  logger.LogWriter
	logrp logger.LogRepairer
}

func newTopic(conf *config.Config, name string) *topic {
	logp := logger.NewPartitions(conf, name)

	return &topic{
		conf:  conf,
		name:  name,
		parts: newPartitions(conf, logp),
		idx:   newQueryIndex(conf.WorkDir, name, conf.MaxPartitions),
		logp:  logp,
		logw:  logger.NewWriter(conf, name),
		logrp: logger.NewRepairer(conf, name),
	}
}

func (t *topic) reset() {
	t.parts.reset()
}

// Setup implements internal.LifecycleManager
func (t *topic) Setup() error {
	if w, ok := t.logw.(internal.LifecycleManager); ok {
		if err := w.Setup(); err != nil {
			return err
		}
	}

	if p, ok := t.logp.(internal.LifecycleManager); ok {
		if err := p.Setup(); err != nil {
			return err
		}
	}

	if err := t.setupPartitions(); err != nil {
		return err
	}
	return nil
}

// Shutdown implements internal.LifecycleManager
func (t *topic) Shutdown() error {
	if w, ok := t.logw.(internal.LifecycleManager); ok {
		if err := w.Shutdown(); err != nil {
			return err
		}
	}

	if p, ok := t.logp.(internal.LifecycleManager); ok {
		if err := p.Shutdown(); err != nil {
			return err
		}
	}
	return nil
}

func (t *topic) setupPartitions() error {
	t.parts.reset()
	parts, err := t.parts.logp.List()
	if err != nil {
		return err
	}

	for _, part := range parts {
		if _, err := t.parts.add(part.Offset(), part.Size()); err != nil {
			return err
		}
	}

	if len(parts) == 0 {
		if _, err := t.parts.add(0, 0); err != nil {
			return err
		}
	}

	head := t.parts.head
	if err := t.check(); err != nil {
		return err
	}
	if serr := t.logw.SetPartition(head.startOffset); serr != nil {
		return serr
	}

	// load query index
	for i := 0; i < t.parts.nparts-1; i++ {
		// fmt.Println("load query index at", t.parts.parts[i])
		if err := t.idx.readIndex(t.parts.parts[i].startOffset); err != nil {
			return err
		}
	}

	log.Printf("Topic %s starting at %d (partition %d, delta %d)", t.name, t.parts.headOffset(), head.startOffset, head.size)
	return nil
}

func (t *topic) check() error {
	if t.parts.head.size == 0 {
		return nil
	}
	partOff := t.parts.head.startOffset
	internal.Debugf(t.conf, "checking integrity of partition %s/%d", t.name, partOff)
	r, err := t.logrp.Data(partOff)
	if err != nil {
		return err
	}
	defer r.Close()

	br := bufio.NewReader(r)
	batch := protocol.NewBatch(t.conf)
	var read int64
	var n int64
	for err == nil && read < int64(t.parts.head.size) {
		n, err = batch.ReadFrom(br)
		if err != nil {
			break
		}

		err = batch.Validate()
		if err != nil {
			break
		}
		err = t.Push(partOff+uint64(read), partOff, int(n), batch.Messages)
		if err != nil {
			break
		}

		read += n
	}

	if err == nil || err == io.EOF {
		return nil
	}

	log.Printf("corrupted partition. truncating head partition %d at offset %d. new head offset: %d. old offset: %d. truncated %d bytes", partOff, read, partOff+uint64(read), partOff+uint64(t.parts.head.size), int64(t.parts.head.size)-read)
	if terr := t.logrp.Truncate(partOff, read); terr != nil {
		return terr
	}

	t.parts.head.size = int(read)
	return nil
}

func (t *topic) Query(off uint64, messages int) (*partitionArgList, error) {
	idx := queryIndexOldPool.Get().(*queryIndexOld).initialize(t, t.conf)
	defer queryIndexOldPool.Put(idx)
	return idx.Query(off, messages)
}

func (t *topic) Push(off, part uint64, size, messages int) error {
	return t.idx.Push(off, part, size, messages)
}
