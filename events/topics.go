package events

import (
	"log"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
	"github.com/jeffrom/logd/logger"
)

// topics manages the topics for an event queue.
type topics struct {
	conf    *config.Config
	manager logger.TopicManager
	m       map[string]*topic
}

func newTopics(conf *config.Config) *topics {
	return &topics{
		conf:    conf,
		manager: logger.NewTopics(conf),
		m:       make(map[string]*topic),
	}
}

func (t *topics) reset() {
	t.m = make(map[string]*topic)
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

	for _, topic := range topics {
		// fmt.Println("list", topic)
		if _, ok := t.m[topic]; !ok {
			if _, aerr := t.add(topic); aerr != nil {
				return aerr
			}
		}
	}
	// fmt.Println(t.m)
	return err
}

// Shutdown implements LifecycleManager
func (t *topics) Shutdown() error {
	var firstErr error
	for _, topic := range t.m {
		err := topic.Shutdown()
		if err != nil && firstErr == nil {
			firstErr = err
		} else if err != nil {
			log.Printf("shutdown: %+v", err)
		}
	}
	return firstErr
}

func (t *topics) add(name string) (*topic, error) {
	topic, ok := t.m[name]
	if !ok {
		log.Printf("creating topic: %s", name)
		if err := t.manager.Create(name); err != nil {
			return nil, err
		}
		topic = newTopic(t.conf, name)
		if err := topic.Setup(); err != nil {
			return nil, err
		}
		t.m[name] = topic
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
	logp  logger.PartitionManager
	logw  logger.LogWriter
}

func newTopic(conf *config.Config, name string) *topic {
	logp := logger.NewPartitions(conf, name)
	return &topic{
		conf:  conf,
		name:  name,
		parts: newPartitions(conf, logp),
		logp:  logp,
		logw:  logger.NewWriter(conf, name),
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
		if err := t.parts.add(part.Offset(), part.Size()); err != nil {
			return err
		}
	}

	if len(parts) == 0 {
		if err := t.parts.add(0, 0); err != nil {
			return err
		}
	}

	head := t.parts.head
	if serr := t.logw.SetPartition(head.startOffset); serr != nil {
		return serr
	}
	log.Printf("Topic %s starting at %d (partition %d, delta %d)", t.name, t.parts.headOffset(), head.startOffset, head.size)
	return nil
}
