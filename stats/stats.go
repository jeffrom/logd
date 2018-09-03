package stats

import (
	"bytes"
	"expvar"
	"fmt"
	"time"
)

var (
	TotalConnections  *expvar.Int
	ActiveConnections *expvar.Int
	BytesIn           *expvar.Int
	BytesOut          *expvar.Int
	TotalRequests     *expvar.Int
)

func init() {
	TotalConnections = expvar.NewInt("conns.total")
	ActiveConnections = expvar.NewInt("conns.active")

	BytesIn = expvar.NewInt("bytes.in")
	BytesOut = expvar.NewInt("bytes.out")

	TotalRequests = expvar.NewInt("requests.total")
}

// MultiOK returns an MOK response body
func MultiOK() []byte {
	b := &bytes.Buffer{}
	expvar.Do(func(kv expvar.KeyValue) {
		if kv.Key == "memstats" || kv.Key == "cmdline" {
			return
		}
		b.WriteString(kv.Key)
		b.WriteString(": ")
		b.WriteString(kv.Value.String())
		b.WriteString("\r\n")
	})
	return b.Bytes()
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

func PrettyTime(ns float64) string {
	if ns > float64(time.Second*60) {
		return fmt.Sprintf("%.2fm", ns/float64(time.Second*60))
	}
	if ns > float64(time.Second) {
		return fmt.Sprintf("%.2fs", ns/float64(time.Second))
	}
	if ns > float64(time.Millisecond) {
		return fmt.Sprintf("%.2fms", ns/float64(time.Millisecond))
	}
	if ns > float64(time.Microsecond) {
		return fmt.Sprintf("%.2fμ", ns/float64(time.Microsecond))
	}
	return fmt.Sprintf("%.2fns", ns)
}
