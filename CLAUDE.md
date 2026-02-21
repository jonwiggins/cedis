# Project: Implement Redis from Scratch in Rust

## Objective

Build a fully functional Redis-compatible in-memory data store in Rust from scratch. The implementation should speak the RESP (Redis Serialization Protocol), support Redis's core data structures and commands, handle persistence, and be indistinguishable from real Redis when accessed via `redis-cli` or standard client libraries.

Do NOT use any existing Redis libraries, RESP parsing crates, or embedded database engines. Every component must be implemented from first principles. You MAY use general-purpose Rust crates for async I/O, networking, byte manipulation, and time — but the core data store logic, protocol handling, and persistence must be yours.

## Project Setup

- Initialize a new Rust project using `cargo init --name cedis`
- Use Rust stable (latest edition)
- Set up the project as both a library (`lib.rs`) and a binary (`main.rs`) that launches the server
- Also create a secondary binary `cedis-cli` for a minimal CLI client
- Use `cargo test` as the primary test runner
- Structure the codebase into well-separated modules from the start (see Architecture below)

## Architecture

The implementation must be organized into these core modules/components. Plan and design the interfaces between them BEFORE writing implementation code.

### 1. RESP Protocol (`resp` module)
- Implement the RESP2 protocol (RESP3 is a stretch goal)
- Reference spec: https://redis.io/docs/latest/develop/reference/protocol-spec/
- Types to support:
  - Simple Strings: `+OK\r\n`
  - Errors: `-ERR message\r\n`
  - Integers: `:1000\r\n`
  - Bulk Strings: `$6\r\nfoobar\r\n` (and null bulk string `$-1\r\n`)
  - Arrays: `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n` (and null array `*-1\r\n`)
- Implement both a streaming parser (for reading from TCP) and a serializer (for writing responses)
- The parser must handle partial reads — data arrives in arbitrary chunks over TCP
- Inline command format support: `PING\r\n` (without RESP framing, used by interactive `redis-cli`)

### 2. Networking / Connection Handler (`server` module)
- Async TCP server using `tokio`
- Listen on configurable host:port (default `127.0.0.1:6379`)
- Accept multiple concurrent client connections
- Per-connection state: selected database index, transaction queue (MULTI), subscription state
- Read RESP commands from each client, dispatch to command processor, write RESP responses
- Support pipelining: clients may send multiple commands without waiting for responses
- Graceful shutdown on SIGINT/SIGTERM
- Connection timeout for idle clients (configurable)

### 3. Command Processor (`command` module)
- Parse RESP arrays into command name + arguments
- Case-insensitive command lookup
- Command dispatch table mapping command names to handler functions
- Each command handler receives: command arguments, reference to data store, client connection state
- Validate argument count and types before execution
- Return appropriate RESP responses (including error responses for wrong types, wrong arg count, etc.)
- Support the `COMMAND` and `COMMAND DOCS` introspection commands

### 4. Data Store (`store` module)
- The core in-memory key-value store
- Top-level structure: `HashMap<String, Entry>` where Entry contains the value + metadata (expiry, type, etc.)
- Support multiple databases (default 16, selectable via `SELECT` command)
- Thread-safe access — use `tokio::sync::RwLock` or a sharded lock design for concurrency
- Key expiration:
  - Store expiry timestamps alongside keys
  - Lazy expiration: check TTL on access, delete if expired
  - Active expiration: background task that periodically samples keys and removes expired ones (Redis's probabilistic approach)
- Memory tracking: track approximate memory usage for INFO output
- Key space notifications (stretch goal)

### 5. Data Structures

Each Redis data type must be its own submodule or file with a clean API. Implement the underlying data structures, not just wrappers around std collections.

#### 5a. Strings (`types/string.rs`)
- Internally: raw bytes (`Vec<u8>`), not Rust `String` — Redis strings are binary-safe
- Support integer encoding: when the value is a valid integer, store as `i64` for atomic INCR/DECR
- Support floating point for INCRBYFLOAT
- Max size: 512 MB (enforce)

#### 5b. Lists (`types/list.rs`)
- Implement as a doubly-linked list (ziplist optimization is a stretch goal)
- Alternatively, use `VecDeque` for simplicity — document trade-offs
- Must support efficient push/pop from both ends
- Support blocking operations (BLPOP, BRPOP) — these require waking waiting clients when data arrives

#### 5c. Hashes (`types/hash.rs`)
- `HashMap<String, Vec<u8>>` internally
- Support field-level operations (HGET, HSET, HDEL, HINCRBY, etc.)
- Ziplist encoding for small hashes (stretch goal)

#### 5d. Sets (`types/set.rs`)
- `HashSet<String>` internally
- Integer set optimization for sets of only integers (stretch goal)
- Support set operations: union, intersection, difference
- SRANDMEMBER with count (positive = unique, negative = may repeat)

#### 5e. Sorted Sets (`types/sorted_set.rs`)
- This is the most complex data structure
- Must support O(log N) insert, delete, and range queries by score
- Must support O(1) score lookup by member
- Implement as a skip list + hash map (like real Redis), OR use a BTreeMap + HashMap combination
- If using skip list: implement from scratch with random level generation
- Support: ZADD (with NX/XX/GT/LT/CH flags), ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZRANK, ZSCORE, ZREM, ZCARD, ZINCRBY, ZUNIONSTORE, ZINTERSTORE
- Lex range queries: ZRANGEBYLEX, ZREVRANGEBYLEX

#### 5f. Streams (`types/stream.rs`) — Phase 7 stretch goal
- Append-only log data structure
- Auto-generated IDs (timestamp + sequence)
- Consumer groups with pending entry lists
- XADD, XREAD, XRANGE, XLEN, XINFO
- XGROUP CREATE, XREADGROUP, XACK

### 6. Persistence (`persistence` module)

#### 6a. RDB Snapshots (`persistence/rdb.rs`)
- Binary snapshot format: https://rdb.fnordig.de/file_format.html
- Implement both reading (loading) and writing (saving)
- Background save: fork-like behavior (since Rust doesn't trivially fork, use a snapshot of the data serialized on a background tokio task — document the trade-off vs. real Redis's fork-based COW approach)
- Support: SAVE (blocking), BGSAVE (background)
- Auto-save rules: configurable "save after N changes in M seconds"
- Load RDB on startup if file exists
- RDB file structure:
  - Magic number: `REDIS`
  - Version number
  - Database selector
  - Key-value pairs with type encoding
  - Expiry timestamps
  - EOF marker + CRC64 checksum

#### 6b. AOF — Append Only File (`persistence/aof.rs`)
- Log every write command to a file
- Support fsync policies: `always`, `everysec`, `no`
- AOF rewrite: compact the log by generating a minimal set of commands that reconstruct the current state
- AOF loading on startup (replay commands)
- Hybrid AOF+RDB format (stretch goal)

### 7. Pub/Sub (`pubsub` module)
- Channel-based publish/subscribe messaging
- SUBSCRIBE, UNSUBSCRIBE, PUBLISH
- PSUBSCRIBE, PUNSUBSCRIBE (pattern-based subscriptions with glob matching)
- Clients in subscribe mode can only issue (P)SUBSCRIBE, (P)UNSUBSCRIBE, PING, QUIT
- Message delivery format: 3-element RESP array `["message", channel, payload]`
- Track subscriber counts per channel
- Use `tokio::sync::broadcast` or a custom fan-out mechanism

### 8. Transactions (`transaction` module)
- MULTI: start queuing commands
- EXEC: execute all queued commands atomically, return array of results
- DISCARD: abort transaction, clear queue
- WATCH: optimistic locking — if any watched key is modified before EXEC, the transaction fails (returns null array)
- Commands inside MULTI are queued, not executed — return `+QUEUED` for each
- Error handling: syntax errors during queuing cause EXEC to fail; runtime errors do NOT roll back prior commands in the transaction (Redis transactions are not like SQL transactions)

### 9. Scripting — Stretch Goal (`scripting` module)
- EVAL and EVALSHA for Lua script execution
- Embed a minimal Lua interpreter or use `mlua` crate
- Provide `redis.call()` and `redis.pcall()` for calling Redis commands from Lua
- Script caching by SHA1 hash
- SCRIPT LOAD, SCRIPT EXISTS, SCRIPT FLUSH

### 10. Replication — Stretch Goal (`replication` module)
- Master-replica replication
- REPLICAOF (SLAVEOF) command
- Full sync: master sends RDB to replica
- Partial sync: replication backlog buffer, offset tracking
- PSYNC protocol
- Replica serves read-only commands

### 11. CLI Client (`cli` module)
- Minimal Redis CLI client (`cedis-cli`)
- Connect to server, send commands, display responses
- Interactive REPL mode with prompt `cedis> `
- Support for `--pipe` mode (bulk import)
- Subscribe mode for Pub/Sub
- Command-line arguments: `--host`, `--port`, `--auth`, `--db`

## Command Coverage

Implement commands in this priority order. Each group should be complete before moving to the next.

### Tier 1 — Core (must have)
**Connection**: PING, ECHO, QUIT, SELECT, AUTH (basic password), DBSIZE, FLUSHDB, FLUSHALL, SWAPDB
**Strings**: GET, SET (with EX/PX/NX/XX/KEEPTTL), GETSET, MGET, MSET, MSETNX, APPEND, STRLEN, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, SETNX, SETEX, PSETEX, GETRANGE, SETRANGE, GETDEL
**Keys**: DEL, EXISTS, EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, TTL, PTTL, PERSIST, TYPE, RENAME, RENAMENX, KEYS, SCAN, RANDOMKEY, UNLINK, OBJECT (ENCODING, REFCOUNT, IDLETIME), DUMP, RESTORE
**Server**: INFO, CONFIG GET/SET/RESETSTAT, DBSIZE, TIME, DEBUG (SLEEP, SET-ACTIVE-EXPIRE), COMMAND, CLIENT (LIST, GETNAME, SETNAME, ID)

### Tier 2 — Data Structures
**Lists**: LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX, LSET, LINSERT, LREM, LTRIM, RPOPLPUSH, LMOVE, LPOS, LMPOP
**Hashes**: HSET, HGET, HDEL, HEXISTS, HLEN, HKEYS, HVALS, HGETALL, HMSET, HMGET, HINCRBY, HINCRBYFLOAT, HSETNX, HRANDFIELD, HSCAN
**Sets**: SADD, SREM, SISMEMBER, SMISMEMBER, SMEMBERS, SCARD, SPOP, SRANDMEMBER, SUNION, SINTER, SDIFF, SUNIONSTORE, SINTERSTORE, SDIFFSTORE, SMOVE, SSCAN, SINTERCARD

### Tier 3 — Sorted Sets & Advanced
**Sorted Sets**: ZADD (NX/XX/GT/LT/CH/INCR), ZREM, ZSCORE, ZRANK, ZREVRANK, ZCARD, ZCOUNT, ZRANGE (with BYSCORE/BYLEX/REV/LIMIT), ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZRANGEBYLEX, ZREVRANGEBYLEX, ZINCRBY, ZUNIONSTORE, ZINTERSTORE, ZRANDMEMBER, ZSCAN, ZPOPMIN, ZPOPMAX, ZMSCORE, ZLEXCOUNT
**Blocking**: BLPOP, BRPOP, BLMOVE, BZPOPMIN, BZPOPMAX (with timeout)

### Tier 4 — Pub/Sub & Transactions
**Pub/Sub**: SUBSCRIBE, UNSUBSCRIBE, PUBLISH, PSUBSCRIBE, PUNSUBSCRIBE, PUBSUB (CHANNELS, NUMSUB, NUMPAT)
**Transactions**: MULTI, EXEC, DISCARD, WATCH, UNWATCH

### Tier 5 — Persistence & Diagnostics
**Persistence**: SAVE, BGSAVE, BGREWRITEAOF, LASTSAVE
**Diagnostics**: SLOWLOG, MONITOR, DEBUG, OBJECT, MEMORY USAGE, LATENCY (stretch)
**Utility**: SORT, WAIT, OBJECT HELP

### Tier 6 — Stretch
**Streams**: XADD, XREAD, XRANGE, XREVRANGE, XLEN, XTRIM, XINFO, XGROUP, XREADGROUP, XACK, XPENDING, XCLAIM, XAUTOCLAIM, XDEL
**Scripting**: EVAL, EVALSHA, SCRIPT (LOAD, EXISTS, FLUSH)
**Cluster**: CLUSTER INFO (stub)

## Implementation Phases

Follow these phases strictly. Do not skip ahead. Each phase should be fully tested before moving to the next.

### Phase 1: Protocol & Echo Server
1. Implement the RESP2 parser (read) and serializer (write)
2. Set up async TCP server with `tokio` — accept connections, read bytes
3. Implement PING, ECHO, QUIT commands
4. Implement inline command parsing (plain text without RESP framing)
5. **Milestone test**: `redis-cli -p 6379 PING` returns `PONG`. Interactive `redis-cli` session works (type commands, get responses).

### Phase 2: Strings & Key Management
1. Implement the core data store (`HashMap<String, Entry>`)
2. Implement GET, SET (with EX/PX/NX/XX options), DEL, EXISTS
3. Implement INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT
4. Implement MGET, MSET, APPEND, STRLEN
5. Implement TTL, PTTL, EXPIRE, PEXPIRE, PERSIST, TYPE
6. Implement lazy key expiration (check on access)
7. Implement active key expiration (background sweep)
8. Implement KEYS (glob pattern matching), SCAN (cursor-based iteration)
9. **Milestone test**: Full interactive session — set keys with TTLs, verify expiration, use `redis-benchmark` for SET/GET.

### Phase 3: Lists & Hashes
1. Implement list data structure (VecDeque or doubly-linked list)
2. Implement LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX, LSET, LREM, LTRIM, LINSERT
3. Implement hash data structure
4. Implement HSET, HGET, HDEL, HGETALL, HKEYS, HVALS, HLEN, HEXISTS, HINCRBY
5. Type checking: return `WRONGTYPE` error when operating on wrong type
6. **Milestone test**: A simple job queue using LPUSH/BRPOP pattern. A user profile stored as a hash.

### Phase 4: Sets & Sorted Sets
1. Implement set data structure
2. Implement SADD, SREM, SISMEMBER, SMEMBERS, SCARD, set operations (SUNION, SINTER, SDIFF)
3. Implement sorted set data structure (skip list + hash map, or BTreeMap + HashMap)
4. Implement ZADD, ZREM, ZSCORE, ZRANK, ZRANGE, ZRANGEBYSCORE, ZCARD, ZINCRBY
5. Implement ZUNIONSTORE, ZINTERSTORE
6. **Milestone test**: A leaderboard — add players with scores, query top N, get rank of specific player. Verify with `redis-cli`.

### Phase 5: Pub/Sub & Transactions
1. Implement channel subscription registry
2. Implement SUBSCRIBE, UNSUBSCRIBE, PUBLISH
3. Implement pattern subscriptions (PSUBSCRIBE) with glob matching
4. Implement MULTI/EXEC/DISCARD command queuing
5. Implement WATCH/UNWATCH optimistic locking
6. **Milestone test**: Two `redis-cli` instances — one subscribes, one publishes, messages flow. Transaction with WATCH detects conflicts.

### Phase 6: RDB Persistence
1. Implement RDB file writer — serialize current state to binary format
2. Implement SAVE (blocking) and BGSAVE (background task)
3. Implement RDB file reader — load state on startup
4. Implement auto-save rules (save after N changes in M seconds)
5. Implement the key expiry entries in RDB format
6. **Milestone test**: Fill database, SAVE, kill server, restart — all data intact. Bonus: load an RDB file generated by real Redis, and vice versa.

### Phase 7: AOF Persistence
1. Implement AOF writer — append each write command to file
2. Implement fsync policies (always, everysec, no)
3. Implement AOF loading on startup (replay commands)
4. Implement AOF rewrite (BGREWRITEAOF) — compact the log
5. Implement CONFIG SET appendonly yes/no (enable/disable at runtime)
6. **Milestone test**: Enable AOF, perform writes, kill server, restart — verify all data recovered from AOF replay. Compare recovery with RDB.

### Phase 8: Blocking Commands & Advanced Features
1. Implement BLPOP, BRPOP with timeout and client wake-up
2. Implement SORT command
3. Implement OBJECT ENCODING (report internal encoding of values)
4. Implement CONFIG GET/SET for runtime configuration
5. Implement INFO command with all standard sections (server, clients, memory, stats, replication, keyspace, etc.)
6. Implement SLOWLOG
7. Implement MONITOR (stream all commands to monitoring clients)
8. **Milestone test**: BLPOP blocks, another client pushes, blocked client wakes. INFO output is parseable by monitoring tools.

### Phase 9: Completeness & Compatibility (Stretch)
1. Streams data type and commands
2. Lua scripting (EVAL/EVALSHA)
3. Basic replication (REPLICAOF, full sync via RDB)
4. CLIENT commands, connection naming
5. MEMORY USAGE, OBJECT HELP
6. ACL basics (AUTH with username/password)
7. **Milestone test**: A real application (e.g., Sidekiq, Bull, or Celery) can use cedis as its backend.

## Testing Strategy

### Unit Tests
- RESP parser: test each type, partial reads, malformed input, nested arrays, null types, empty bulk strings
- RESP serializer: verify byte-exact output for all types
- Each data structure: insert, delete, lookup, edge cases (empty, single element, large)
- Skip list (if used): random insert/delete sequences, verify ordering invariants
- Glob pattern matching: test *, ?, [abc], [^abc], [a-z], escape characters
- RDB format: round-trip encode/decode for each value type, verify CRC
- Key expiration: verify lazy and active expiration behavior

### Integration Tests
- Create a `tests/` directory with integration tests
- Each test starts a server on a random port, connects with a real Redis client library, runs commands, asserts results
- Use the `redis` Rust crate (the client library) as the test client — this validates wire compatibility
- Test pipelining: send multiple commands without reading, then read all responses
- Test concurrent clients: spawn multiple tasks, each performing operations
- Test error cases: wrong number of args, wrong types, nonexistent keys

### External Test Suites & Validation

#### Redis's Own Test Suite
- Clone redis/redis from GitHub
- The test suite is in `tests/` — TCL-based
- Run in external mode: `./runtest --host <your-host> --port <your-port>`
- Track which test files pass and log failures
- Priority test files:
  - `unit/auth.tcl`
  - `unit/protocol.tcl`
  - `unit/keyspace.tcl`
  - `unit/expire.tcl`
  - `unit/type/string.tcl`
  - `unit/type/list.tcl`
  - `unit/type/set.tcl`
  - `unit/type/zset.tcl`
  - `unit/type/hash.tcl`
  - `unit/pubsub.tcl`
  - `unit/multi.tcl`
  - `unit/scan.tcl`
  - `unit/sort.tcl`
  - `integration/rdb.tcl`
  - `integration/aof.tcl`

#### redis-test DSL Tool
- https://github.com/siddontang/redis-test
- A standalone test tool specifically for RESP-compatible servers
- Useful for quick command-level validation

#### redis-benchmark
- Ships with Redis: `redis-benchmark -p <your-port> -t set,get,lpush,lpop -n 100000`
- Validates both correctness (commands must work) and performance
- Run after each phase and track throughput over time
- Key benchmarks:
  - `SET`: raw write throughput
  - `GET`: raw read throughput
  - `LPUSH/LPOP`: list operation throughput
  - `SADD`: set operation throughput
  - `ZADD/ZRANGEBYSCORE`: sorted set throughput
  - `MSET` (10 keys): batch throughput
  - Pipelining: `redis-benchmark -P 16` to test pipeline performance

#### Client Library Compatibility
- Test with real client libraries from different languages:
  - `redis` crate (Rust)
  - `redis-py` (Python)
  - `ioredis` (Node.js)
- If all three connect and operate correctly, wire protocol compatibility is strong
- Run each library's basic example code against your server

#### Write Your Own Conformance Suite
- Build a test harness that runs the same command sequence against both cedis and real Redis
- Compare responses byte-for-byte (RESP-level comparison)
- Automate as a cargo test target
- Focus on edge cases: empty strings, binary data, very long keys, negative integers, float edge cases (NaN, Inf), unicode

### Performance Benchmarks
- Use `criterion` crate for microbenchmarks
- Key internal benchmarks:
  - RESP parse throughput (messages/sec)
  - Hash map operations at various sizes (1K, 100K, 1M keys)
  - Skip list / sorted set operations at various sizes
  - Glob pattern matching
  - RDB serialization/deserialization throughput
- Use `redis-benchmark` for end-to-end throughput comparison against real Redis

## Rust-Specific Guidance

### Async Runtime
- Use `tokio` as the async runtime (with `rt-multi-thread` feature)
- Use `tokio::net::TcpListener` and `TcpStream`
- Use `tokio::io::AsyncReadExt` and `AsyncWriteExt` for non-blocking I/O
- Use `tokio::select!` for multiplexing (e.g., BLPOP timeout + data arrival)
- Use `tokio::sync::Notify` or `broadcast` for Pub/Sub fan-out and blocked command wake-up
- Use `tokio::signal` for graceful shutdown

### Concurrency Model
- Single-threaded event loop is fine to start (like real Redis)
- Use `tokio::spawn` for background tasks (active expiration, BGSAVE, AOF fsync)
- The data store should be behind an `Arc<RwLock<>>` or `Arc<Mutex<>>`
- Consider a sharded lock design for better read concurrency at scale: partition the keyspace into N shards, each with its own lock
- IMPORTANT: Redis's single-threaded model means commands are serialized — your implementation should preserve this atomicity guarantee. Using `tokio::sync::Mutex` (not `std::sync::Mutex`) ensures fairness and avoids blocking the executor.

### Error Handling
- Define a custom error enum (`CedisError`) using `thiserror`
- Error categories: protocol errors, command errors (wrong type, wrong args, out of range), I/O errors, persistence errors
- RESP error responses: use Redis's conventions — `ERR`, `WRONGTYPE`, `LOADING`, `BUSY`, `NOSCRIPT`, etc.
- Never panic on client input — always return an error response
- Panics are acceptable only for genuine bugs (and should be caught at the connection level to avoid crashing the whole server)

### Byte Handling
- Use `bytes::BytesMut` for the TCP read buffer — efficient for streaming RESP parsing
- Use `bytes::Bytes` for immutable shared data
- Redis strings are binary-safe — use `Vec<u8>` not `String` for values
- Keys can be `String` (valid UTF-8 is a reasonable restriction) or `Vec<u8>` for full compatibility

### Data Structure Implementation
- For sorted sets, if implementing a skip list:
  - Use arena allocation (`Vec<Node>` with index-based references) instead of raw pointers
  - Random level generation: use `rand` crate, geometric distribution with p=0.25, max level 32
  - Each node: forward pointers array, backward pointer, score, member
- For the main keyspace HashMap: consider `hashbrown` (already used by std, but explicit use allows configuration) or `dashmap` for sharded concurrent access

### Memory
- Track memory usage of stored values for INFO and MEMORY USAGE commands
- Implement `mem::size_of_val` approximations for each data type
- Be mindful of String/Vec allocations — Redis is memory-conscious

### Project Structure
```
cedis/
├── Cargo.toml
├── src/
│   ├── main.rs              # Server entry point
│   ├── lib.rs               # Public API
│   ├── resp.rs              # RESP protocol parser + serializer
│   ├── server.rs            # TCP listener, connection handler
│   ├── connection.rs        # Per-client connection state + read/write loop
│   ├── command/
│   │   ├── mod.rs           # Command dispatch table
│   │   ├── string.rs        # String command handlers
│   │   ├── list.rs          # List command handlers
│   │   ├── hash.rs          # Hash command handlers
│   │   ├── set.rs           # Set command handlers
│   │   ├── sorted_set.rs    # Sorted set command handlers
│   │   ├── key.rs           # Key management command handlers
│   │   ├── server_cmd.rs    # Server/connection commands (PING, INFO, CONFIG, etc.)
│   │   ├── pubsub.rs        # Pub/Sub command handlers
│   │   └── transaction.rs   # MULTI/EXEC command handlers
│   ├── store/
│   │   ├── mod.rs           # Core key-value store
│   │   ├── entry.rs         # Entry type (value + expiry + metadata)
│   │   ├── expiry.rs        # Expiration logic (lazy + active)
│   │   └── keyspace.rs      # Key pattern matching, SCAN cursor
│   ├── types/
│   │   ├── mod.rs           # Value enum (String, List, Hash, Set, SortedSet)
│   │   ├── rstring.rs       # Redis string type
│   │   ├── list.rs          # List implementation
│   │   ├── hash.rs          # Hash implementation
│   │   ├── set.rs           # Set implementation
│   │   ├── sorted_set.rs    # Sorted set (skip list + hash map)
│   │   └── stream.rs        # Stream implementation (stretch)
│   ├── persistence/
│   │   ├── mod.rs           # Persistence manager
│   │   ├── rdb.rs           # RDB snapshot read/write
│   │   └── aof.rs           # AOF append/replay/rewrite
│   ├── pubsub.rs            # Pub/Sub channel registry
│   ├── transaction.rs       # MULTI/EXEC transaction state
│   ├── config.rs            # Server configuration
│   ├── info.rs              # INFO command output generation
│   ├── glob.rs              # Glob pattern matching for KEYS/PSUBSCRIBE
│   └── error.rs             # Error types
├── src/bin/
│   └── cedis-cli.rs        # CLI client binary
├── tests/
│   ├── resp_test.rs         # Protocol tests
│   ├── string_test.rs       # String command integration tests
│   ├── list_test.rs         # List command integration tests
│   ├── hash_test.rs         # Hash command integration tests
│   ├── set_test.rs          # Set command integration tests
│   ├── sorted_set_test.rs   # Sorted set integration tests
│   ├── pubsub_test.rs       # Pub/Sub integration tests
│   ├── transaction_test.rs  # MULTI/EXEC integration tests
│   ├── persistence_test.rs  # RDB/AOF integration tests
│   ├── expiry_test.rs       # Key expiration tests
│   └── compat/              # Cross-compatibility tests vs real Redis
└── benches/
    ├── resp_bench.rs        # RESP parsing benchmarks
    ├── store_bench.rs       # Data store operation benchmarks
    └── e2e_bench.rs         # End-to-end command benchmarks
```

## Configuration

Support Redis-style configuration via:
1. Command-line arguments: `cedis --port 6380 --dbfilename dump.rdb --appendonly yes`
2. Config file: `cedis /path/to/cedis.conf` (same format as `redis.conf`)
3. Runtime: `CONFIG SET` / `CONFIG GET`

Key configuration options to support:
- `bind` (default `127.0.0.1`)
- `port` (default `6379`)
- `databases` (default `16`)
- `requirepass` (password for AUTH)
- `maxmemory` and `maxmemory-policy` (noeviction, allkeys-lru, volatile-lru, allkeys-random, volatile-random, volatile-ttl)
- `save` rules (e.g., `save 900 1`, `save 300 10`)
- `dbfilename` (default `dump.rdb`)
- `dir` (working directory for persistence files)
- `appendonly` (yes/no)
- `appendfsync` (always, everysec, no)
- `hz` (server tick frequency for background tasks, default 10)
- `timeout` (client idle timeout)
- `tcp-keepalive`
- `loglevel` (debug, verbose, notice, warning)

## Definition of Done

The project is considered complete when:

1. **Wire compatibility**: `redis-cli` connects and works interactively with no issues. Users cannot tell it's not real Redis for supported commands.
2. **Client library compatibility**: The `redis` crate (Rust), `redis-py` (Python), and `ioredis` (Node.js) all work against the server for Tier 1-4 commands.
3. **Command coverage**: All Tier 1-4 commands are implemented and behave correctly.
4. **Persistence**: RDB save/load works. Server survives restarts. Bonus: RDB files are compatible with real Redis.
5. **Pub/Sub**: Multiple subscribers, pattern matching, message delivery all work.
6. **Transactions**: MULTI/EXEC/WATCH work correctly, including conflict detection.
7. **Performance**: `redis-benchmark` reports at least 50% of real Redis throughput for GET/SET on a single core.
8. **Stability**: Server handles malformed input, abrupt disconnections, and concurrent clients without crashing. Can run `redis-benchmark` with 50 concurrent clients without errors.
9. **Test passage**: Passes >30% of Redis's own test suite in external mode.
10. **No unsafe**: Zero `unsafe` blocks unless absolutely necessary, and each one is documented with a safety comment.

## Reference Materials

- RESP protocol spec: https://redis.io/docs/latest/develop/reference/protocol-spec/
- Redis commands reference: https://redis.io/commands/
- RDB file format: https://rdb.fnordig.de/file_format.html
- Redis internals (data structures): https://redis.io/docs/latest/develop/reference/internals/
- Redis persistence: https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/
- Redis replication: https://redis.io/docs/latest/operate/oss_and_stack/management/replication/
- Tokio mini-redis (reference implementation): https://github.com/tokio-rs/mini-redis
- "Redis in Action" (Manning) — good for understanding Redis patterns
- Antirez's blog posts on Redis internals: http://antirez.com/
- Skip list paper: William Pugh, "Skip Lists: A Probabilistic Alternative to Balanced Trees" (1990)

## Important Notes

- Always run `cargo clippy` and fix warnings before considering any phase complete
- Run `cargo fmt` to maintain consistent formatting
- Commit after each milestone with a descriptive message
- If you get stuck on a design decision, document the trade-offs and pick the simpler option
- Correctness over performance. Always. Optimize only after correctness is proven.
- When in doubt about Redis behavior, test the same command against a real Redis server — it is the spec
- Do NOT look at the Redis C source for "how to implement" — use the documentation and your own design. The goal is a clean Rust implementation, not a C-to-Rust port.
- DO reference Tokio's mini-redis for architectural patterns (connection handling, command dispatch), but implement your own data structures and logic
