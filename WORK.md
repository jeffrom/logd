[modeline]: <> ( vim: set ft=markdown: )

# TODO

- [ ] test suite that runs the same set of tests with different configurations,
  but also supports expected failures in some cases
- [ ] large benchmarking suite
    - server/client startup/shutdown
    - all commands
    - error handling cases
- [ ] finish command set, including STATS, REFUSE, ACCEPT
    - `refuse(_at)` / `accept(_at)` should be able to synchronize switching at
      partition boundaries, as well as ids.
- [ ] make consistency guarantees configurable. fast by default but also force
  disk flush before returning success if that's desired.
    - most strict can use `creat(O_SYNC)`, or maybe just flush before
      responding to each command
    - least strict just needs to fsync during shutdown
    - have a flush interval option. also document how it may be better to just
      change the dirty page cache kernel settings. this can be implemented by
      just putting a flush command into the queue at an interval.
- [ ] audit all panics: we should only panic when there's a fatal error.
    - mostly return all the way up to main
- [ ] minimize IO layers as much as possible. io.Copy is ideal, probably.
    - would syscall.Fdatasync instead of Flush help? seems likely.
- [ ] keep track of running delete hooks, make part of graceful shutdown
- [ ] seeking/reading the log when we don't need to (calling Setup, probably)
- [ ] evaluate where we can use a mutex instead of channels
- [ ] compression
- [ ] need to check the return value of Close()? May contain errors from
  previous delayed io.
- [ ] after tests and refactoring, optimize. shoot for 0 allocations and do as
  little work as possible.
    - using preallocated buffer + end position pointer so the buffer doesn't
      need to be cleared
- [ ] figure out linting
- [ ] some tests that spin up containers to replicate, switch masters, etc
  while under load
- [ ] documentation with many use cases, event log, pub sub, replication,
  changing master
- [ ] track / limit / reuse concurrent fds in use


# COMPLETED

## DEC 2017

- [X] same protocol for file storage as network transfer
- [X] use sendfile

## OCT 2017

- [X] working client functionality, particularly around reads
- [X] partitioning, including removing old partitions with hooks
    - reading doesn't work near partition boundaries
    - can't read last few messages
- [X] index isn't being written to disk, probably other issues too
- [X] subscribers are removed when their connection closes
- [X] test suite should also include system-level tests with coverage
