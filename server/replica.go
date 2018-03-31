package server

import (
	"context"
	"io"

	"github.com/jeffrom/logd/client"
	"github.com/jeffrom/logd/config"
	"github.com/jeffrom/logd/protocol"
)

// Replica serves reads to clients and replicates from a Socket
type Replica struct {
	*Socket
	client *client.Client
}

// NewReplica returns a new instance of a replica
func NewReplica(listenaddr string, config *config.Config) *Replica {
	r := &Replica{
		Socket: NewSocket(listenaddr, config),
		// client: client.DialConfig(replicationaddr, config),
	}
	r.setDisallowedCommands(map[protocol.CmdType]bool{
		protocol.CmdMessage: true,
	})
	return r
}

func (r *Replica) setup() error {
	c, err := client.DialConfig(r.config.MasterHostport, r.config)
	if err != nil {
		return err
	}
	r.client = c

	return nil
}

// ListenAndServe starts a replica for serving requests
func (r *Replica) ListenAndServe() error {
	r.Socket.GoServe()
	return r.replicate()
}

func (r *Replica) replicate() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	headResp, err := r.q.PushCommand(ctx, protocol.NewCommand(r.config, protocol.CmdHead))
	if err != nil {
		return err
	}
	r.config.ReadFromTail = true
	scanner, err := r.client.DoRead(headResp.ID, 0)

	for scanner.Scan() {
		msg := scanner.Message()
		resp, rerr := r.q.PushCommand(ctx, protocol.NewCommand(r.config, protocol.CmdMessage, msg.Body))
		if rerr != nil {
			return rerr
		}
		if resp.Status == protocol.RespEOF {
			return io.EOF
		}
	}

	return err
}
