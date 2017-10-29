[modeline]: <> ( vim: set ft=markdown: )

# TODO

- [X] working client functionality, particularly around reads
- [ ] partitioning, including removing old partitions with hooks
    - reading doesn't work near partition boundaries
    - can't read last few messages
- [X] index isn't being written to disk, probably other issues too
- [ ] test suite that runs the same set of tests with different configurations,
  but also supports expected failures in some cases
- [ ] subscribers are not removed when their connection closes
- [X] test suite should also include system-level tests with coverage
- [ ] large benchmarking suite
- [ ] after the tests are solid, go through all the code and reorg
- [ ] finish command set, including STATS, REFUSE, ACCEPT
- [ ] REPLICATE can just call READ internally for now, later it should have a
  different protocol
- [ ] documentation with many use cases, event log, pub sub, replication,
  changing master
- [ ] CONTRIBUTORS.md
- [ ] after tests and refactoring, optimize. shoot for 0 allocations and do as
  little work as possible. Also make sure sendfile is being used.
- [ ] make consistency guarantees configurable. fast by default but also force
  disk flush before returning success if that's desired.
- [ ] audit all panics: we should only panic when there's a fatal error.
- [ ] detect and finish subscriptions where the connection wasn't closed on the
  client side
- [ ] same protocol for file storage as network transfer simplifies the app and
  probably makes it easier to leverage sendfile without calling it directly
- [ ] compression
