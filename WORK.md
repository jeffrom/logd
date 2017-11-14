[modeline]: <> ( vim: set ft=markdown: )

# TODO

- [ ] test suite that runs the same set of tests with different configurations,
  but also supports expected failures in some cases
- [ ] large benchmarking suite
    - server/client startup/shutdown
    - all commands
- [ ] after the tests are solid, go through all the code and reorg
- [ ] finish command set, including STATS, REFUSE, ACCEPT
- [ ] REPLICATE can just call READ internally for now, later it should have a
  different protocol
- [ ] make consistency guarantees configurable. fast by default but also force
  disk flush before returning success if that's desired.
- [ ] audit all panics: we should only panic when there's a fatal error.
    - otherwise return all the way up to main
- [ ] same protocol for file storage as network transfer simplifies the app and
  probably makes it easier to leverage sendfile without calling it directly
- [ ] minimize IO layers as much as possible. io.Copy is ideal, probably.
    - would syscall.Fdatasync instead of Flush help? seems likely.
- [ ] evaluate where we can use a mutex instead of channels
- [ ] compression
- [ ] need to check the return value of Close()? May contain errors from
  previous delayed io.
- [ ] after tests and refactoring, optimize. shoot for 0 allocations and do as
  little work as possible. Also make sure sendfile is being used.
- [ ] figure out linting
- [ ] documentation with many use cases, event log, pub sub, replication,
  changing master
- [ ] CONTRIBUTORS.md


# COMPLETED

## OCT 2017

- [X] working client functionality, particularly around reads
- [X] partitioning, including removing old partitions with hooks
    - reading doesn't work near partition boundaries
    - can't read last few messages
- [X] index isn't being written to disk, probably other issues too
- [X] subscribers are removed when their connection closes
- [X] test suite should also include system-level tests with coverage
