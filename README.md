[![Build Status](https://travis-ci.org/jeffrom/logd.svg?branch=master)](https://travis-ci.org/jeffrom/logd)

# logd

high-performance log transport

## overview

logd is a client-server application for storing and retrieving logs. Features
include:

- simple, text-based protocol
- batched message delivery
- lookup messages by offset
- topics
- failed deliveries can be recovered
- configurable write guarantees
- high-performance

logd is pre-alpha. things generally work, but the interfaces are probably still
going to change.

## installation

```sh
go get github.com/jeffrom/logd
```

## usage

### server

```sh
logd
```

### command-line client

`log-cli` can be used to read and write messages.

```sh
# you'll get back some offsets
jeff $ cat some_file | log-cli write
10240
10800
12223

# read from the beginning of the log
jeff $ log-cli read --limit 1

# read from an offset
jeff $ log-cli read --offset 10240
```

### go client library

`Writer` can be used for sending messages, in batches, to the log.

```go
// safe for concurrent calls
w := client.NewWriter(conf, "mytopic").WithStateHandler(myhandler)
log.SetOutput(w)
```

`Scanner` can be used to read messages back from the log.

```go
s, _ := client.DialScanner("myserver:1774")
for s.Scan() {
    msg := s.Message()
    fmt.Println(msg)
}
if err := s.Error(); err != nil {
    panic(err)
}
```

## design

logd is built for simplicity and usability. Batches come via the network, are
validated, and then written without modification directly to partitions
(represented as files) on disk. Topics are just directories which hold
partitions. A text protocol is used and logd can be operated pretty easily
using `telnet` if desired. The client library attempts to follow go
conventions. For example, usage of `client.Scanner` should be predictable to
someone familiar with scanners in the go standard library. The server attempts
to model itself closely to the OS such that tuning OS disk, network, and
scheduling parameters can have significant benefits.

Clients write messages in batches and receive a response containing an offset
in the log the batch was written to within a particular topic. This can be used
later to efficiently read starting at that offset. Each topic is managed by a
goroutine which controls synchronous access to the topic's partitions. There
is another goroutine which handles non-blocking requests such as the `CLOSE`
and `STATS` commands.

The client libraries consist of a socket client, which handles sending
requests, receiving responses, and synchronous retries. Some higher-level
libraries are built on top of this, `Writer` and `Scanner`: `Writer` collects
messages, sending along batches to the server when the current batch reaches a
certain size or a time interval is reached. `Scanner` reads messages back from
the server. The client libraries attempt to provide strategies for common
stream processing requirements, such as persisting log state so scanning can
resume from a checkpoint after failures. `Writer` aggressively returns errors
during failure, and failed batch deliveries can be accessed and retried. The
lower-level client can be used to compose other abstractions, such as more
lenient writers and parallel scanners. Callers can write their own state
handlers which keep track of log offsets.

## performance

The goal is for server to be able to persist all messages when the network
interface is saturated, while clients can send and receive tens of thousands of
reasonably-sized messages per second. The bottleneck for reads and writes
should be kernel space, specifically network and disk IO, not the server
itself. The kernel should be leveraged here to increase performance. For
example, the `sendfile` system call is used when sending batches to client
readers.

Other goals are to minimize hot-path memory allocations and copying of bytes.

Here are some benchmarks on a 12-core, 16gb memory linux machine, using a
loopback device for networking:

```
pkg: github.com/jeffrom/logd/events
BenchmarkBatchFile-12                    1000000              3192 ns/op              70 B/op          1 allocs/op
BenchmarkLifecycle-12                      20000            131053 ns/op           90164 B/op        205 allocs/op
BenchmarkBatchFull-12                    1000000              2852 ns/op              88 B/op          6 allocs/op
BenchmarkBatchFullLarge-12                 20000            149112 ns/op            1086 B/op          8 allocs/op
BenchmarkBatchFullTopics8-12             1000000              3010 ns/op              82 B/op          6 allocs/op
BenchmarkReadHead-12                      200000             10275 ns/op            4398 B/op         25 allocs/op
BenchmarkReadTail-12                      200000             11437 ns/op            4716 B/op         25 allocs/op
BenchmarkReadAll-12                       200000             13197 ns/op            5512 B/op         29 allocs/op

jeff$ log-cli bench --conns 16 --topics 4 --duration 60s
batch size: 65500b, topics: 4, duration: 1m0s, connections: 16

bytes out :             50.56Gb                 863.53Mb/s
batches   :             828.61K                 13.82K/s
timing    :
        min 626.93μ
        p50 626.93μ
        p90 626.93μ
        p95 626.93μ
        p99 21.92ms
        max 1.25s
```

## planned

- clean up API
- multi-language client libraries
- over-the-wire compression
- TLS
- more state handlers (postgres, mysql, redis)
- replication
- automatic failover
