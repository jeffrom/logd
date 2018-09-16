[modeline]: <> ( vim: set ft=markdown: )

# TODO

- [ ] audit / fix int types, such as batch size (should be int, not uint64)
- [ ] simple replication, scanner failover
- [ ] config validation
- [ ] `testhelper/mock_server.go` has some race condition problems. probably
      has to do with the closing connection stuff
- [ ] verify batches in the connection goroutine to better leverage multiple
      processors for the expensive checksum
  - option to disable checksum verification would also be good
- [ ] verify topics concurrently during startup
- [ ] http server
  - should have a json protocol
  - logd protocol is working
    | curl -X POST -d $'READ default 0 3\r\n' -H 'Content-type: application/logd' http://localhost:1775/log

# maybe later

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

# COMPLETED

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

# OBSOLETE

- [ ] fix sendfile. internal.LogFile needs to be able to return a seeked
      os.File wrapped in an io.LimitReader
- [ ] read from a partition:offset, not just an id
- [ ] add batch to protocol so we can seek quickly around the logfile without an index
- [ ] replication. replicas ACK to master. instrument replica delay
- [ ] protocol.Reader, Writer should not hold their own buffers, should
      implement something similar to io.ReaderFrom and io.WriterTo
  - may want to just have Command, Response, Message have ReadFrom and WriteTo methods
  - then we can add Batch
- [ ] Store head/tail id in index
  - [x] head
  - [ ] tail
- [ ] clear index entries that have been deleted
- [ ] refuse/accept functionality
  - `refuse(_at)` / `accept(_at)` should be able to synchronize switching at
    partition boundaries, as well as ids.
  - probably want to be able to have the server close a connection and tell the
    client where they should try to reconnect?
- [ ] `STATS` is a lot simpler after refactoring response logic. there
      shouldn't be cmd.respC AND resp.readerC. all server response bytes should go
      through one channel.
