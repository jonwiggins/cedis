# Cedis TODO

## Completed

- [x] Pub/Sub: SUBSCRIBE, UNSUBSCRIBE, PUBLISH, PSUBSCRIBE, PUNSUBSCRIBE, PUBSUB (CHANNELS/NUMSUB/NUMPAT)
- [x] RDB persistence: SAVE, BGSAVE, load on startup, key expiry in RDB
- [x] AOF persistence: append-only file, fsync policies (always/everysec/no), AOF replay, BGREWRITEAOF
- [x] Blocking list commands: BLPOP, BRPOP (non-blocking fallback), LMPOP
- [x] SORT command (ASC/DESC, ALPHA, LIMIT, STORE)
- [x] Graceful shutdown on SIGINT/SIGTERM
- [x] Client idle timeout support
- [x] Expanded test coverage: 81 tests (30 unit + 51 integration)
- [x] Benchmark suite: 30-40K ops/sec single-client, 275K ops/sec pipelined

## Phase 9 — True Blocking Commands

- [ ] Implement real blocking for BLPOP/BRPOP using `tokio::sync::Notify`
- [ ] Client wake-up when data is pushed to a watched key
- [ ] BLMOVE with blocking semantics
- [ ] BZPOPMIN, BZPOPMAX for sorted sets
- [ ] Timeout handling with `tokio::time::timeout`

## Phase 10 — Auto-Save & Persistence Improvements

- [ ] Auto-save rules: trigger RDB save after N changes in M seconds (use save_rules config)
- [ ] Track change counter across write commands
- [ ] AOF rewrite triggered automatically based on AOF size
- [ ] CONFIG SET appendonly yes/no to toggle AOF at runtime
- [ ] Track and return actual last save timestamp for LASTSAVE

## Phase 11 — Streams

- [ ] Implement stream data type (time-series log)
- [ ] XADD, XLEN, XRANGE, XREVRANGE
- [ ] XREAD with blocking support
- [ ] XTRIM (maxlen, minid)
- [ ] Consumer groups: XGROUP CREATE, XREADGROUP, XACK, XPENDING

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
- [ ] SRANDMEMBER with count, SPOP with count
- [ ] GEORADIUS / GEOADD / GEODIST (geo commands)
- [ ] HyperLogLog: PFADD, PFCOUNT, PFMERGE
- [ ] Bitmap: SETBIT, GETBIT, BITCOUNT, BITOP, BITPOS, BITFIELD
- [ ] Run against Redis TCL test suite in external mode

## Phase 17 — Production Hardening

- [ ] Maxmemory enforcement with eviction policies (LRU, LFU, random, volatile)
- [ ] Protected mode (refuse external connections without password)
- [ ] TLS support via tokio-rustls
- [ ] Unix socket support
- [ ] Configurable TCP backlog and keep-alive
- [ ] Better error handling and recovery for persistence failures
