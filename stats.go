package logd

import "time"

type stats struct {
	startedAt time.Time
	clients   uint
}
