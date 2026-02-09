# Cedis

A Redis-compatible in-memory data store built from scratch in Rust.

Cedis speaks the **RESP2 protocol** and implements Redis's core data structures — strings, lists, hashes, sets, and sorted sets — along with key expiration, transactions, and a configurable server. All without using any existing Redis or RESP libraries.

## Highlights

- **100+ commands** — full coverage of strings, lists, hashes, sets, sorted sets, key management, transactions, and server administration
- **RESP2 protocol** — streaming parser/serializer built from scratch, supporting both framed and inline commands
- **Wire-compatible** — works with `redis-cli`, the `redis` crate, and any standard Redis client
- **Command pipelining** — multiple commands in a single TCP read are processed without extra round-trips
- **Transactions** — `MULTI`/`EXEC`/`DISCARD` with `WATCH`/`UNWATCH` optimistic locking
- **Key expiration** — lazy expiration on access + active background sweep
- **Multiple databases** — 16 databases by default with `SELECT`, `SWAPDB`, `FLUSHDB`/`FLUSHALL`
- **Configurable** — CLI flags (`--port`, `--bind`, etc.) and runtime `CONFIG GET`/`SET`
- **~5,500 lines of library code** — compact, readable, single-crate implementation
- **51 tests** — 30 unit tests (RESP parser, glob matching) + 21 integration tests using the `redis` crate as a real client

## Supported Commands

### Strings (20)
`GET` `SET` (EX/PX/NX/XX/KEEPTTL/GET/EXAT/PXAT) `GETSET` `MGET` `MSET` `MSETNX` `APPEND` `STRLEN` `INCR` `DECR` `INCRBY` `DECRBY` `INCRBYFLOAT` `SETNX` `SETEX` `PSETEX` `GETRANGE` `SETRANGE` `GETDEL`

### Lists (14)
`LPUSH` `RPUSH` `LPOP` `RPOP` `LLEN` `LRANGE` `LINDEX` `LSET` `LINSERT` `LREM` `LTRIM` `RPOPLPUSH` `LMOVE` `LPOS`

### Hashes (14)
`HSET` `HGET` `HDEL` `HEXISTS` `HLEN` `HKEYS` `HVALS` `HGETALL` `HMSET` `HMGET` `HINCRBY` `HINCRBYFLOAT` `HSETNX` `HRANDFIELD` `HSCAN`

### Sets (16)
`SADD` `SREM` `SISMEMBER` `SMISMEMBER` `SMEMBERS` `SCARD` `SPOP` `SRANDMEMBER` `SUNION` `SINTER` `SDIFF` `SUNIONSTORE` `SINTERSTORE` `SDIFFSTORE` `SMOVE` `SSCAN` `SINTERCARD`

### Sorted Sets (22)
`ZADD` (NX/XX/GT/LT/CH/INCR) `ZREM` `ZSCORE` `ZRANK` `ZREVRANK` `ZCARD` `ZCOUNT` `ZRANGE` `ZREVRANGE` `ZRANGEBYSCORE` `ZREVRANGEBYSCORE` `ZRANGEBYLEX` `ZREVRANGEBYLEX` `ZINCRBY` `ZUNIONSTORE` `ZINTERSTORE` `ZRANDMEMBER` `ZSCAN` `ZPOPMIN` `ZPOPMAX` `ZMSCORE` `ZLEXCOUNT`

### Keys (14)
`DEL` `UNLINK` `EXISTS` `EXPIRE` `PEXPIRE` `EXPIREAT` `PEXPIREAT` `TTL` `PTTL` `PERSIST` `TYPE` `RENAME` `RENAMENX` `KEYS` `SCAN` `RANDOMKEY` `OBJECT` `DUMP` `RESTORE`

### Transactions (5)
`MULTI` `EXEC` `DISCARD` `WATCH` `UNWATCH`

### Server (16)
`PING` `ECHO` `QUIT` `SELECT` `AUTH` `DBSIZE` `FLUSHDB` `FLUSHALL` `SWAPDB` `INFO` `CONFIG` `TIME` `COMMAND` `CLIENT` `DEBUG` `RESET` `SAVE` `BGSAVE` `BGREWRITEAOF` `LASTSAVE`

## Getting Started

### Build

```bash
cargo build --release
```

### Run the server

```bash
# Default: listens on 127.0.0.1:6379
cargo run --release --bin cedis

# Custom port and bind address
cargo run --release --bin cedis -- --port 6380 --bind 0.0.0.0
```

### Connect with any Redis client

```bash
# With redis-cli
redis-cli -p 6379

# Or use the bundled CLI
cargo run --release --bin cedis-cli
```

### Run the tests

```bash
# Unit tests only
cargo test --lib

# Integration tests (starts a server automatically)
cargo test --test integration_test

# All tests
cargo test
```

## Architecture

```
src/
  main.rs              Entry point — CLI arg parsing and server startup
  lib.rs               Crate root — module declarations
  resp.rs              RESP2 streaming parser and serializer (with inline command support)
  server.rs            Async TCP server (tokio) with per-connection task spawning
  connection.rs        Per-client state (db index, auth, transaction queue)
  config.rs            Runtime configuration with CLI flag parsing
  error.rs             Error types
  glob.rs              Redis-style glob pattern matching
  store/
    mod.rs             Multi-database key-value store with lazy + active expiration
    entry.rs           Key entry with TTL metadata
  types/
    mod.rs             RedisValue enum (String | List | Hash | Set | SortedSet)
    rstring.rs         Binary-safe string with INCR/GETRANGE support
    list.rs            VecDeque-backed list
    hash.rs            HashMap-backed hash
    set.rs             HashSet-backed set
    sorted_set.rs      BTreeMap + HashMap sorted set with f64 score ordering
  command/
    mod.rs             Central dispatch table (command name -> handler)
    string.rs          String command handlers
    list.rs            List command handlers
    hash.rs            Hash command handlers
    set.rs             Set command handlers
    sorted_set.rs      Sorted set command handlers
    key.rs             Key management command handlers
    server_cmd.rs      Server/connection command handlers
    transaction.rs     MULTI/EXEC/WATCH handlers
    pubsub.rs          Pub/Sub placeholder
  bin/
    cedis-cli.rs       Minimal interactive CLI client
tests/
    integration_test.rs  21 integration tests using the redis crate
```

## Design Decisions

- **No Redis/RESP library dependencies** — the RESP parser, serializer, data structures, and command handlers are all implemented from scratch. Only general-purpose crates are used (tokio, bytes, thiserror, rand, tracing).

- **Single shared store behind `Arc<RwLock>`** — simple concurrency model that matches Redis's single-threaded semantics. Each connection gets its own `ClientState` for per-client data (selected DB, transaction queue, auth status).

- **Sorted set ordering** — uses a bit-level transform (`f64_to_orderable`) to convert IEEE 754 floats into u64 values that sort correctly in a `BTreeMap`, giving O(log n) range queries without a custom comparator.

- **Lazy + active expiration** — keys are lazily expired on access (checked on every `GET`/`EXISTS`), plus a background task samples keys periodically to proactively reclaim memory.

- **Streaming RESP parser** — handles partial TCP reads and command pipelining naturally. The parser consumes bytes from a `BytesMut` buffer and returns `Ok(None)` when more data is needed, allowing the server loop to read more data and retry.

## Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | `6379` | TCP port |
| `--bind` | `127.0.0.1` | Bind address |
| `--databases` | `16` | Number of databases |
| `--requirepass` | *(none)* | Password for AUTH |
| `--timeout` | `0` | Client idle timeout (seconds, 0 = disabled) |
| `--hz` | `10` | Background task frequency |
| `--loglevel` | `notice` | Log level |
| `--appendonly` | `no` | AOF persistence (planned) |

All configurable parameters are also available via `CONFIG GET`/`CONFIG SET` at runtime.

## License

MIT
