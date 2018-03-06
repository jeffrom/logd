[modeline]: <> ( vim: set ft=markdown: )

# TODO

* [ ] correctly read when partition 0 has been deleted
  * [ ] reads/writes after startup should work
  * [X] should return a not found error
  * [X] client should exit with not found error by default
  * [ ] should have a client flag to read from the end in this case
* [ ] read from the beginning of the log
* [X] ensure subscription connection is closed when _not_ reading forever
* [ ] audit all panics: we should only panic when there's a fatal error.
  * mostly return all the way up to main
* [ ] put delete hooks in a queue, keep track of running delete hooks, make
      part of graceful shutdown
      * continue on startup when there are still pending delete hooks
      * log delete hook output to logd stdout
* [ ] need to check the return value of Close(). May contain errors from
      previous delayed io.
* [ ] make synchronization idiomatic w/ stuff like https://udhos.github.io/golang-concurrency-tricks/
* [ ] backpressure in the form of max concurrent connections
* [ ] clear index entries that have been deleted
* [ ] test concurrent writes -> correct number of messages in the log

# later

* [ ] `REFUSE`, `ACCEPT` commands.
* [ ] log file compression
  * probably want to be able to have the server close a connection and tell the
    client where they should try to reconnect?
* [ ] `STATS` is a lot simpler after refactoring response logic. there
      shouldn't be cmd.respC AND resp.readerC. all server response bytes should go
      through one channel.
* [ ] documentation with many use cases, event log, pub sub, replication,
      changing master
* [ ] figure out linting
* [ ] benchmarking suite
  * server/client startup/shutdown
  * all commands
  * error handling cases
* [ ] seeking/reading the log when we don't need to (calling Setup, probably)
* [ ] minimize IO abstractions as much as possible. io.Copy is ideal, probably.
  * would syscall.Fdatasync instead of Flush help? seems likely.
* [ ] evaluate async logic.
* [ ] track / limit / reuse concurrent fds in use
* [ ] optimize. shoot for 0 allocations and do as little work as possible.
  * where we can use a mutex instead of channels?
  * using preallocated buffer + end position pointer so the buffer doesn't
    need to be cleared
* [ ] test suite that runs the same set of tests with different configurations,
      but also supports expected failures in some cases
* [ ] make consistency guarantees configurable. fast by default but also force
      disk flush before returning success if that's desired.
  * most strict can use `creat(O_SYNC)`, or maybe just flush before
    responding to each command
  * least strict just needs to fsync during shutdown
  * have a flush interval option. also document how it may be better to just
    change the dirty page cache kernel settings. this can be implemented by
    just putting a flush command into the queue at an interval.
* [ ] some tests that spin up containers to replicate, switch masters, etc
      while under load
* [ ] refuse/accept functionality
  * `refuse(_at)` / `accept(_at)` should be able to synchronize switching at
    partition boundaries, as well as ids.
* [ ] Read from end of log
  * `MIN` command? since we can delete old partitions
  * `READ -1 0` maybe to read from the tail?
* [ ] record READ misses

# COMPLETED

## DEC 2017

* [x] same protocol for file storage as network transfer
* [x] use sendfile
* [x] `STATS` command

## OCT 2017

* [x] working client functionality, particularly around reads
* [x] partitioning, including removing old partitions with hooks
  * reading doesn't work near partition boundaries
  * can't read last few messages
* [x] index isn't being written to disk, probably other issues too
* [x] subscribers are removed when their connection closes
* [x] test suite should also include system-level tests with coverage
