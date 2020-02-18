# Roadmap

## 0.2.0

- [X] configuration to limit the number of topics
- [X] topic whitelist
- [X] document config

### testing

- [X] eventQ should return the correct number of batches when there is more than one
- [X] scanner offset/delta state with restarts

## 0.3.0

- [ ] writer error handler / reconnect sequencing
- [ ] server-side limit on number of messages returned for reads
- [ ] fix: events test attempts to reconnect to default host:port. it should
      instead try to reconnect to the test server's ephemeral host:port.

### testing

- [ ] scanner state: single message in batch with subsequent messages completed up
      to the next batch, and other such cases

## 0.4.0

- [ ] stabilize socket client reconnection
- [ ] stabilize graceful shutdown
- [ ] Writer, Scanner, FileStatePuller, others get own configs
- [ ] fix(cli): log-cli incorrectly prints "topic is empty" when bad offset
      requested

## 0.5.0: ready for public testing

- [ ] log-forwarder agent / client. accept MSG, build batches, flush to server.
- [ ] stabilize configuration api
- [ ] stabilize client api
- [ ] make uint64 client-facing variables into int64
- [ ] linting

## 0.6.0

- [ ] writer backpressure config
- [ ] test context cancellations
- [ ] refactor / document client -> server -> events error handling
- [ ] configuration validation

### testing

- [ ] device mapper based tests such as flakey device mapper with nonrandom text input

## 0.7.0

- [ ] blocking READ
- [ ] stabilize logd.Backlog

## 0.8.0

- [ ] replication
- [ ] scanner failover


# maybe

<!-- - [ ] XXX writer: on each flush, send a map or array of offset:delta -> message to
      a callback -->

- [ ] a version of client.Writer.Write that accepts a callback which receives
      the message, offset, delta, and error that is called when a flush occurs.
- [ ] make Partitioner an io.ReaderAt
  - this would let us do lookups in parallel, and use mmap for this
    - https://github.com/golang/exp/tree/master/mmap
- [ ] track / limit / reuse concurrent fds in use
- [ ] record READ misses
- [ ] over the wire compression
- [ ] documentation with many use cases, event log, pub sub, replication,
      changing master
- [ ] figure out linting
- [ ] benchmarking suite
  - [x] server/client startup/shutdown
  - [ ] all commands
  - [ ] error handling cases
- [ ] optimize. shoot for 0 allocations and do as little work as possible.
  - where we can use a mutex instead of channels?
  - using preallocated buffer + end position pointer so the buffer doesn't
    need to be cleared
- [ ] some tests that spin up containers to replicate, switch masters, etc
      while under load
- [ ] put delete hooks in a queue, keep track of running delete hooks, make
      part of graceful shutdown
      _ continue on startup when there are still pending delete hooks
      _ log delete hook output to logd stdout

## previously completed

## July-August 2018

- [x] send config to new connections so it can be validated
  - probably better to have client ask for it
- [x] make consistency guarantees configurable. fast by default (at-most-once),
      but also force disk flush before returning success (at-least-once) if
      that's desired.
  - most strict can use `creat(O_SYNC)`, or maybe just flush before
    responding to each command
  - least strict just needs to fsync during shutdown
  - have a flush interval option. this can be implemented by just putting a
    flush command into the queue at an interval. also document how it may be
    better to just change the dirty page cache kernel settings.
- [x] Client graceful reconnect
- [x] topics?
- [x] migrate logd/log-cli to cobra
- [x] failure handling for writer
- [x] scanner should read all pending response batches over the wire before
      beginning to iterate through them
- [x] Repairer to truncate partitions with corrupt data
- [x] remove state management stuff from scanner for now
- [x] idle timeout
- [x] how should client scanner message tracking work?
  - needs to be able to start at any message in the batch
- [x] split each topic into its own event queue which will be a big win as each
      topic would have concurrency against the rest
  - pushrequest would send to the correct `in` channel

## Jan-March 2018

- [x] test concurrent writes -> correct number of messages in the log
- [x] need to check the return value of Close(). May contain errors from
      previous delayed io.
- [x] read from the beginning of the log
- [x] should have a client flag to read from the beginning in case where
      requested id not found
- [x] audit all panics: we should only panic when there's a fatal error.
- [x] correctly read when partition 0 has been deleted
  - [x] reads/writes after startup should work
  - [x] should return a not found error
  - [x] client should exit with not found error by default
- [x] ensure subscription connection is closed when _not_ reading forever
- [x] test suite that runs the same set of tests with different configurations,
      but also supports expected failures in some cases
- [x] graceful shutdown: clients connections should close cleanly if possible
- [x] backpressure in the form of max concurrent connections
  - done as event queue buffer size

## DEC 2017

- [x] same protocol for file storage as network transfer
- [x] use sendfile
- [x] `STATS` command

## OCT 2017

- [x] working client functionality, particularly around reads
- [x] partitioning, including removing old partitions with hooks
  - reading doesn't work near partition boundaries
  - can't read last few messages
- [x] index isn't being written to disk, probably other issues too
- [x] subscribers are removed when their connection closes
- [x] test suite should also include system-level tests with coverage

- [x] make synchronization idiomatic w/ stuff like https://udhos.github.io/golang-concurrency-tricks/
