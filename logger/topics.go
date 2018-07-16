package logger

import (
	"os"
	"path"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/internal"
)

// TopicManager deals with the listing of topics, which typically will exist as
// directories in the filesystem.
// TODO it should probably create/remove them as well
type TopicManager interface {
	List() ([]string, error)
	Create(topic string) error
	Remove(topic string) error
}

// Topics implements TopicManager
type Topics struct {
	conf    *config.Config
	workdir *os.File
}

// NewTopics returns a new instance of Topics
func NewTopics(conf *config.Config) *Topics {
	return &Topics{
		conf: conf,
	}
}

// List implements TopicManager
func (t *Topics) List() ([]string, error) {
	internal.Debugf(t.conf, "listing %s", t.workdir.Name())
	if err := t.reopenWorkDir(); err != nil {
		return nil, err
	}
	files, err := t.workdir.Readdir(0)
	if err != nil {
		return nil, err
	}

	var res []string
	for _, info := range files {
		if !info.IsDir() {
			continue
		}

		res = append(res, info.Name())
	}
	return res, nil
}

// Create implements TopicManager
func (t *Topics) Create(name string) error {
	p := path.Join(t.conf.WorkDir, name)
	return os.MkdirAll(p, 0700)
}

// Remove implements TopicManager
func (t *Topics) Remove(name string) error {
	p := path.Join(t.conf.WorkDir, name)
	return os.Remove(p)
}

func (t *Topics) reopenWorkDir() error {
	if t.workdir != nil {
		if err := t.workdir.Close(); err != nil {
			return nil
		}
	}

	f, err := os.Open(t.conf.WorkDir)
	if err != nil {
		return err
	}
	t.workdir = f

	return nil
}

// Setup implements internal.LifecycleManager
func (t *Topics) Setup() error {
	// TODO conf for this?
	if err := os.MkdirAll(t.conf.WorkDir, 0700); err != nil {
		return err
	}

	f, err := os.Open(t.conf.WorkDir)
	if err != nil {
		return err
	}

	t.workdir = f
	return nil
}

// Shutdown implements internal.LifecycleManager
func (t *Topics) Shutdown() error {
	if t.workdir != nil {
		return t.workdir.Close()
	}
	return nil
}
