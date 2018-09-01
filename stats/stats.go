package stats

import (
	"expvar"
	"fmt"
	"net/http"
	"time"

	"github.com/zserge/metric"
)

var (
	TotalConnections  *expvar.Map
	ActiveConnections *expvar.Map
	TotalRequests     *expvar.Map
)

func init() {
	TotalConnections = expvar.NewMap("conns.total")
	ActiveConnections = expvar.NewMap("conns.active")

	TotalRequests = expvar.NewMap("requests.total")

	expvar.Publish("batch.latency", metric.NewHistogram("5m1s", "15m30s", "1h1m"))
	expvar.Publish("read.latency", metric.NewHistogram("5m1s", "15m30s", "1h1m"))

	// go periodicFlush()
	// go tmpSetupHTTP()
}

// Timing updates a histogram with millisecond timing
func Timing(name string, start time.Time) {
	expvar.Get(name).(metric.Metric).Add(float64(time.Since(start).Nanoseconds()) / float64(time.Millisecond))
}

func tmpSetupHTTP() {
	http.Handle("/debug/metrics", metric.Handler(metric.Exposed))
	http.Handle("/debug/expvar", expvar.Handler())
	http.ListenAndServe(":1775", nil)
}

func periodicFlush() {
	for {
		time.Sleep(5 * time.Second)
		expvar.Do(func(kv expvar.KeyValue) {
			if kv.Key == "memstats" {
				return
			}
			fmt.Println(kv.Key, kv.Value.String())
		})
	}
}
