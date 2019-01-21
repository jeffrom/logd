package server

import (
	"sync"

	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/protocol"
)

var batchPool = sync.Pool{
	New: func() interface{} {
		return protocol.NewBatch(nil)
	},
}

func newBatch(conf *config.Config) *protocol.Batch {
	batch := batchPool.Get().(*protocol.Batch).WithConfig(conf)
	batch.Reset()
	return batch
}
func finishBatch(b *protocol.Batch) { batchPool.Put(b) }
