# Cedis TODO

## Phase 5 — Pub/Sub

- [ ] Implement channel-based publish/subscribe messaging
- [ ] Per-connection message channels using `tokio::sync::broadcast` or similar
- [ ] Commands: `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`, `PSUBSCRIBE`, `PUNSUBSCRIBE`
- [ ] `PUBSUB` subcommands: `CHANNELS`, `NUMSUB`, `NUMPAT`
- [ ] Clients in subscribe mode should only accept `(P)SUBSCRIBE`, `(P)UNSUBSCRIBE`, `PING`, `QUIT`

## Phase 6 — RDB Persistence

- [ ] Implement RDB binary snapshot format writer (serialize current state to disk)
- [ ] `SAVE` (blocking) and `BGSAVE` (background task)
- [ ] RDB file reader — load state on startup if file exists
- [ ] Auto-save rules (save after N changes in M seconds)
- [ ] Key expiry entries in RDB format

## Phase 7 — AOF Persistence

- [ ] Log every write command to an append-only file
- [ ] Support fsync policies: `always`, `everysec`, `no`
- [ ] AOF loading on startup (replay commands)
- [ ] `BGREWRITEAOF` for log compaction
- [ ] `CONFIG SET appendonly yes/no` to enable/disable at runtime

## Phase 8 — Blocking Commands

- [ ] `BLPOP`, `BRPOP` with timeout and client wake-up
- [ ] `BLMOVE`
- [ ] `BZPOPMIN`, `BZPOPMAX`
- [ ] Client wake-up mechanism using `tokio::sync::Notify` or similar

## Infrastructure

- [ ] Graceful shutdown on `SIGINT`/`SIGTERM` using `tokio::signal`
- [ ] Client idle timeout support (close connections idle longer than configured `timeout`)

## Missing Commands

- [ ] `SORT` command (BY, GET, ASC/DESC, ALPHA, LIMIT, STORE)
- [ ] `LMPOP`
- [ ] Review all existing commands against Redis docs for missing edge cases or flags

## Testing & Performance

- [ ] Expand integration test coverage (concurrent clients, pipelining stress, binary-safe keys/values, error cases, large datasets, expiration edge cases)
- [ ] Add tests for less-covered commands: `GETDEL`, `LPOS`, `SINTERCARD`, sorted set lex operations
- [ ] Target: ~100 tests total
- [ ] Run `redis-benchmark` and measure throughput for `GET`/`SET`/`LPUSH`/`LPOP`/`SADD`/`ZADD`
- [ ] Profile and optimize hot paths (RESP parser, lock contention, allocations)
- [ ] Target: at least 50% of real Redis throughput for `GET`/`SET`

## Stretch Goals

- [ ] Streams (`XADD`, `XREAD`, `XRANGE`, `XLEN`, consumer groups)
- [ ] Lua scripting (`EVAL`, `EVALSHA`, `SCRIPT`)
- [ ] Basic replication (`REPLICAOF`, full sync via RDB)
- [ ] `MEMORY USAGE`, `SLOWLOG`, `MONITOR`
- [ ] Run against Redis's own TCL test suite in external mode
