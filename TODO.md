# Cedis TODO

## Completed

- [x] Pub/Sub: SUBSCRIBE, UNSUBSCRIBE, PUBLISH, PSUBSCRIBE, PUNSUBSCRIBE, PUBSUB (CHANNELS/NUMSUB/NUMPAT)
- [x] RDB persistence: SAVE, BGSAVE, load on startup, key expiry in RDB
- [x] AOF persistence: append-only file, fsync policies (always/everysec/no), AOF replay, BGREWRITEAOF
- [x] Blocking list commands: BLPOP, BRPOP (non-blocking fallback), LMPOP
- [x] SORT command (ASC/DESC, ALPHA, LIMIT, STORE)
- [x] Graceful shutdown on SIGINT/SIGTERM
- [x] Client idle timeout support
- [x] True blocking BLPOP/BRPOP using `tokio::sync::Notify` with KeyWatcher, client wake-up on LPUSH/RPUSH
- [x] Auto-save rules: trigger RDB save after N changes in M seconds, change counter across write commands
- [x] CONFIG SET appendonly/maxmemory/maxmemory-policy at runtime
- [x] Maxmemory enforcement with eviction policies (allkeys-random, volatile-random, volatile-ttl, noeviction)
- [x] Memory eviction background loop
- [x] Streams: XADD, XLEN, XRANGE, XREVRANGE, XREAD, XTRIM (maxlen)
- [x] Bitmap: SETBIT, GETBIT, BITCOUNT, BITOP (AND/OR/XOR/NOT), BITPOS
- [x] HyperLogLog: PFADD, PFCOUNT, PFMERGE
- [x] Expanded test coverage: 112 tests (47 unit + 65 integration)
- [x] Benchmark suite: 37K ops/sec single-client, 271K ops/sec pipelined

## Phase 12 — Lua Scripting

- [ ] Embed a Lua interpreter (mlua or rlua)
- [ ] EVAL, EVALSHA commands
- [ ] SCRIPT LOAD, SCRIPT EXISTS, SCRIPT FLUSH
- [ ] Redis-compatible Lua API: redis.call(), redis.pcall()
- [ ] Key isolation (KEYS/ARGV arguments)

## Phase 13 — Cluster & Replication Basics

- [ ] REPLICAOF/SLAVEOF command (basic primary-replica)
- [ ] Full sync via RDB transfer
- [ ] Replication offset tracking
- [ ] WAIT command for synchronous replication

## Phase 14 — Performance Optimization

- [ ] Profile with flamegraph to identify hot paths
- [ ] Reduce lock contention (per-database locks vs global RwLock)
- [ ] Zero-copy RESP parsing where possible
- [ ] Connection pooling / multiplexing
- [ ] Optimize sorted set operations for large cardinalities
- [ ] Memory-efficient small object encodings (ziplist/listpack equivalents)
- [ ] Target: approach real Redis throughput for GET/SET

## Phase 15 — Observability & Admin

- [ ] MEMORY USAGE command
- [ ] SLOWLOG (track slow commands)
- [ ] MONITOR command (real-time command stream)
- [ ] CLIENT LIST with full connection metadata
- [ ] More accurate INFO sections (memory, stats, keyspace)
- [ ] OBJECT ENCODING with proper encoding detection

## Phase 16 — Missing Commands & Compatibility

- [ ] COPY command
- [ ] OBJECT FREQ, OBJECT HELP improvements
- [ ] WAIT command
- [ ] HELLO command (RESP3 negotiation stub)
- [ ] ACL commands (basic user/password management)
- [ ] GEORADIUS / GEOADD / GEODIST / GEOSEARCH (geo commands)
- [ ] BITFIELD command
- [ ] BLMOVE, BZPOPMIN, BZPOPMAX
- [ ] Consumer groups: XGROUP CREATE, XREADGROUP, XACK, XPENDING
- [ ] RDB/AOF serialization for Stream and HyperLogLog types
- [ ] Run against Redis TCL test suite in external mode

## Phase 17 — Production Hardening

- [ ] Protected mode (refuse external connections without password)
- [ ] TLS support via tokio-rustls
- [ ] Unix socket support
- [ ] Configurable TCP backlog and keep-alive
- [ ] Better error handling and recovery for persistence failures
- [ ] LRU/LFU eviction policies (with access tracking)
