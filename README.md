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
- high-performance

logd is in alpha.

## installation

```sh
go get github.com/jeffrom/logd
```

## usage

### go client library

`Writer` can be used for sending messages, in batches, to the log.

```go
# safe for concurrent calls
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

### command-line tool

`log-cli` can be used to read and write messages.

## performance

The goal is for server to be able to persist all messages when the network
interface is saturated, while clients can send and receive tens of thousands of
messages per second. The bottleneck should be kernel space, specifically
network and disk IO. The kernel should be leveraged here. For example,
`sendfile` is used when sending message batches to client readers.

Other goals are to minimize hot-path memory allocations and copying of bytes.

## planned

- clean up API
- multi-language client libraries
- over-the-wire compression
- TLS
- optional at-least-once delivery guarantee
- replication
- automatic failover
