package transport

import (
	"context"
	"net"

	"github.com/jeffrom/logd/protocol"
)

// Server is used by events to start and stop servers
type Server interface {
	GoServe()
	Stop() error
	ListenAddr() net.Addr
	SetQPusher(q QPusher)
}

// QPusher lets a server push requests to the event q
type QPusher interface {
	PushRequest(context.Context, *protocol.Request) (*protocol.Response, error)
}
