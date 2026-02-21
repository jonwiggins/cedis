pub mod string;
pub mod list;
pub mod hash;
pub mod set;
pub mod sorted_set;
pub mod stream;
pub mod bitmap;
pub mod hyperloglog;
pub mod geo;
pub mod key;
pub mod server_cmd;
pub mod pubsub;
pub mod transaction;
pub mod scripting;

use crate::config::SharedConfig;
use crate::connection::ClientState;
use crate::keywatcher::SharedKeyWatcher;
use crate::pubsub::SharedPubSub;
use crate::resp::RespValue;
use crate::scripting::ScriptCache;
use crate::store::SharedStore;
use tokio::sync::mpsc;

/// Dispatch a parsed command to the appropriate handler.
pub async fn dispatch(
    cmd_name: &str,
    args: &[RespValue],
    store: &SharedStore,
    config: &SharedConfig,
    client: &mut ClientState,
    pubsub: &SharedPubSub,
    pubsub_tx: &mpsc::UnboundedSender<RespValue>,
    key_watcher: &SharedKeyWatcher,
    script_cache: &ScriptCache,
) -> RespValue {
    // If in MULTI mode and this isn't EXEC/DISCARD/MULTI/WATCH/UNWATCH, queue the command
    if client.in_multi && !matches!(cmd_name, "EXEC" | "DISCARD" | "MULTI" | "WATCH" | "UNWATCH") {
        if !is_known_command(cmd_name) {
            client.multi_error = true;
            let args_preview: Vec<String> = args
                .iter()
                .take(3)
                .filter_map(|a| a.to_string_lossy())
                .map(|s| format!("'{s}'"))
                .collect();
            return RespValue::error(format!(
                "ERR unknown command '{}', with args beginning with: {}",
                cmd_name,
                args_preview.join(" ")
            ));
        }
        client.multi_queue.push((cmd_name.to_string(), args.to_vec()));
        return RespValue::SimpleString("QUEUED".to_string());
    }

    match cmd_name {
        // Connection
        "PING" => server_cmd::cmd_ping(args),
        "ECHO" => server_cmd::cmd_echo(args),
        "QUIT" => server_cmd::cmd_quit(client),
        "SELECT" => server_cmd::cmd_select(args, client, config).await,
        "AUTH" => server_cmd::cmd_auth(args, client, config).await,
        "DBSIZE" => server_cmd::cmd_dbsize(store, client).await,
        "FLUSHDB" => server_cmd::cmd_flushdb(store, client).await,
        "FLUSHALL" => server_cmd::cmd_flushall(store).await,
        "SWAPDB" => server_cmd::cmd_swapdb(args, store, config).await,

        // Server
        "INFO" => server_cmd::cmd_info(args, store, config).await,
        "CONFIG" => server_cmd::cmd_config(args, config, store).await,
        "TIME" => server_cmd::cmd_time(),
        "COMMAND" => server_cmd::cmd_command(args),
        "CLIENT" => server_cmd::cmd_client(args, client),
        "DEBUG" => server_cmd::cmd_debug(args, store, config, client).await,
        "RESET" => server_cmd::cmd_reset(client),

        // Strings
        "GET" => string::cmd_get(args, store, client).await,
        "SET" => string::cmd_set(args, store, client).await,
        "GETSET" => string::cmd_getset(args, store, client).await,
        "MGET" => string::cmd_mget(args, store, client).await,
        "MSET" => string::cmd_mset(args, store, client).await,
        "MSETNX" => string::cmd_msetnx(args, store, client).await,
        "MSETEX" => string::cmd_msetex(args, store, client).await,
        "APPEND" => string::cmd_append(args, store, client).await,
        "STRLEN" => string::cmd_strlen(args, store, client).await,
        "LCS" => string::cmd_lcs(args, store, client).await,
        "INCR" => string::cmd_incr(args, store, client).await,
        "DECR" => string::cmd_decr(args, store, client).await,
        "INCRBY" => string::cmd_incrby(args, store, client).await,
        "DECRBY" => string::cmd_decrby(args, store, client).await,
        "INCRBYFLOAT" => string::cmd_incrbyfloat(args, store, client).await,
        "SETNX" => string::cmd_setnx(args, store, client).await,
        "SETEX" => string::cmd_setex(args, store, client).await,
        "PSETEX" => string::cmd_psetex(args, store, client).await,
        "GETRANGE" => string::cmd_getrange(args, store, client).await,
        "SETRANGE" => string::cmd_setrange(args, store, client).await,
        "GETDEL" => string::cmd_getdel(args, store, client).await,
        "GETEX" => string::cmd_getex(args, store, client).await,

        // Keys
        "DEL" => key::cmd_del(args, store, client).await,
        "UNLINK" => key::cmd_del(args, store, client).await,
        "EXISTS" => key::cmd_exists(args, store, client).await,
        "EXPIRE" => key::cmd_expire(args, store, client).await,
        "PEXPIRE" => key::cmd_pexpire(args, store, client).await,
        "EXPIREAT" => key::cmd_expireat(args, store, client).await,
        "PEXPIREAT" => key::cmd_pexpireat(args, store, client).await,
        "TTL" => key::cmd_ttl(args, store, client).await,
        "PTTL" => key::cmd_pttl(args, store, client).await,
        "EXPIRETIME" => key::cmd_expiretime(args, store, client).await,
        "PEXPIRETIME" => key::cmd_pexpiretime(args, store, client).await,
        "PERSIST" => key::cmd_persist(args, store, client).await,
        "TYPE" => key::cmd_type(args, store, client).await,
        "RENAME" => key::cmd_rename(args, store, client).await,
        "RENAMENX" => key::cmd_renamenx(args, store, client).await,
        "KEYS" => key::cmd_keys(args, store, client).await,
        "SCAN" => key::cmd_scan(args, store, client).await,
        "RANDOMKEY" => key::cmd_randomkey(store, client).await,
        "OBJECT" => key::cmd_object(args, store, config, client).await,
        "DUMP" => key::cmd_dump(args, store, client).await,
        "RESTORE" => key::cmd_restore(args, store, client).await,
        "SORT" | "SORT_RO" => key::cmd_sort(args, store, client).await,
        "COPY" => key::cmd_copy(args, store, client).await,
        "MOVE" => key::cmd_move(args, store, client).await,
        "TOUCH" => key::cmd_touch(args, store, client).await,

        // Lists
        "LPUSH" => list::cmd_lpush(args, store, client, key_watcher).await,
        "RPUSH" => list::cmd_rpush(args, store, client, key_watcher).await,
        "LPUSHX" => list::cmd_lpushx(args, store, client, key_watcher).await,
        "RPUSHX" => list::cmd_rpushx(args, store, client, key_watcher).await,
        "LPOP" => list::cmd_lpop(args, store, client).await,
        "RPOP" => list::cmd_rpop(args, store, client).await,
        "LLEN" => list::cmd_llen(args, store, client).await,
        "LRANGE" => list::cmd_lrange(args, store, client).await,
        "LINDEX" => list::cmd_lindex(args, store, client).await,
        "LSET" => list::cmd_lset(args, store, client).await,
        "LINSERT" => list::cmd_linsert(args, store, client).await,
        "LREM" => list::cmd_lrem(args, store, client).await,
        "LTRIM" => list::cmd_ltrim(args, store, client).await,
        "RPOPLPUSH" => list::cmd_rpoplpush(args, store, client).await,
        "LMOVE" => list::cmd_lmove(args, store, client).await,
        "LPOS" => list::cmd_lpos(args, store, client).await,
        "LMPOP" => list::cmd_lmpop(args, store, client).await,
        "BLPOP" => list::cmd_blpop(args, store, client, key_watcher).await,
        "BRPOP" => list::cmd_brpop(args, store, client, key_watcher).await,
        "BLMOVE" => list::cmd_blmove(args, store, client, key_watcher).await,
        "BLMPOP" => list::cmd_blmpop(args, store, client, key_watcher).await,

        // Hashes
        "HSET" => hash::cmd_hset(args, store, client).await,
        "HGET" => hash::cmd_hget(args, store, client).await,
        "HDEL" => hash::cmd_hdel(args, store, client).await,
        "HEXISTS" => hash::cmd_hexists(args, store, client).await,
        "HLEN" => hash::cmd_hlen(args, store, client).await,
        "HKEYS" => hash::cmd_hkeys(args, store, client).await,
        "HVALS" => hash::cmd_hvals(args, store, client).await,
        "HGETALL" => hash::cmd_hgetall(args, store, client).await,
        "HMSET" => hash::cmd_hmset(args, store, client).await,
        "HMGET" => hash::cmd_hmget(args, store, client).await,
        "HINCRBY" => hash::cmd_hincrby(args, store, client).await,
        "HINCRBYFLOAT" => hash::cmd_hincrbyfloat(args, store, client).await,
        "HSETNX" => hash::cmd_hsetnx(args, store, client).await,
        "HRANDFIELD" => hash::cmd_hrandfield(args, store, client).await,
        "HSCAN" => hash::cmd_hscan(args, store, client).await,
        "HSTRLEN" => hash::cmd_hstrlen(args, store, client).await,
        "HGETDEL" => hash::cmd_hgetdel(args, store, client).await,

        // Sets
        "SADD" => set::cmd_sadd(args, store, client).await,
        "SREM" => set::cmd_srem(args, store, client).await,
        "SISMEMBER" => set::cmd_sismember(args, store, client).await,
        "SMISMEMBER" => set::cmd_smismember(args, store, client).await,
        "SMEMBERS" => set::cmd_smembers(args, store, client).await,
        "SCARD" => set::cmd_scard(args, store, client).await,
        "SPOP" => set::cmd_spop(args, store, client).await,
        "SRANDMEMBER" => set::cmd_srandmember(args, store, client).await,
        "SUNION" => set::cmd_sunion(args, store, client).await,
        "SINTER" => set::cmd_sinter(args, store, client).await,
        "SDIFF" => set::cmd_sdiff(args, store, client).await,
        "SUNIONSTORE" => set::cmd_sunionstore(args, store, client).await,
        "SINTERSTORE" => set::cmd_sinterstore(args, store, client).await,
        "SDIFFSTORE" => set::cmd_sdiffstore(args, store, client).await,
        "SMOVE" => set::cmd_smove(args, store, client).await,
        "SSCAN" => set::cmd_sscan(args, store, client).await,
        "SINTERCARD" => set::cmd_sintercard(args, store, client).await,

        // Sorted sets
        "ZADD" => sorted_set::cmd_zadd(args, store, client).await,
        "ZREM" => sorted_set::cmd_zrem(args, store, client).await,
        "ZSCORE" => sorted_set::cmd_zscore(args, store, client).await,
        "ZRANK" => sorted_set::cmd_zrank(args, store, client).await,
        "ZREVRANK" => sorted_set::cmd_zrevrank(args, store, client).await,
        "ZCARD" => sorted_set::cmd_zcard(args, store, client).await,
        "ZCOUNT" => sorted_set::cmd_zcount(args, store, client).await,
        "ZRANGE" => sorted_set::cmd_zrange(args, store, client).await,
        "ZREVRANGE" => sorted_set::cmd_zrevrange(args, store, client).await,
        "ZRANGEBYSCORE" => sorted_set::cmd_zrangebyscore(args, store, client).await,
        "ZREVRANGEBYSCORE" => sorted_set::cmd_zrevrangebyscore(args, store, client).await,
        "ZRANGEBYLEX" => sorted_set::cmd_zrangebylex(args, store, client).await,
        "ZREVRANGEBYLEX" => sorted_set::cmd_zrevrangebylex(args, store, client).await,
        "ZINCRBY" => sorted_set::cmd_zincrby(args, store, client).await,
        "ZUNIONSTORE" => sorted_set::cmd_zunionstore(args, store, client).await,
        "ZINTERSTORE" => sorted_set::cmd_zinterstore(args, store, client).await,
        "ZRANDMEMBER" => sorted_set::cmd_zrandmember(args, store, client).await,
        "ZSCAN" => sorted_set::cmd_zscan(args, store, client).await,
        "ZPOPMIN" => sorted_set::cmd_zpopmin(args, store, client).await,
        "ZPOPMAX" => sorted_set::cmd_zpopmax(args, store, client).await,
        "ZMSCORE" => sorted_set::cmd_zmscore(args, store, client).await,
        "ZLEXCOUNT" => sorted_set::cmd_zlexcount(args, store, client).await,
        "ZREMRANGEBYSCORE" => sorted_set::cmd_zremrangebyscore(args, store, client).await,
        "ZREMRANGEBYLEX" => sorted_set::cmd_zremrangebylex(args, store, client).await,
        "ZREMRANGEBYRANK" => sorted_set::cmd_zremrangebyrank(args, store, client).await,
        "ZUNION" => sorted_set::cmd_zunion(args, store, client).await,
        "ZINTER" => sorted_set::cmd_zinter(args, store, client).await,
        "ZDIFF" => sorted_set::cmd_zdiff(args, store, client).await,
        "ZDIFFSTORE" => sorted_set::cmd_zdiffstore(args, store, client).await,
        "ZINTERCARD" => sorted_set::cmd_zintercard(args, store, client).await,
        "ZMPOP" => sorted_set::cmd_zmpop(args, store, client).await,
        "BZPOPMIN" => sorted_set::cmd_bzpopmin(args, store, client).await,
        "BZPOPMAX" => sorted_set::cmd_bzpopmax(args, store, client).await,
        "BZMPOP" => sorted_set::cmd_bzmpop(args, store, client).await,

        // Streams
        "XADD" => stream::cmd_xadd(args, store, client).await,
        "XLEN" => stream::cmd_xlen(args, store, client).await,
        "XRANGE" => stream::cmd_xrange(args, store, client).await,
        "XREVRANGE" => stream::cmd_xrevrange(args, store, client).await,
        "XREAD" => stream::cmd_xread(args, store, client).await,
        "XTRIM" => stream::cmd_xtrim(args, store, client).await,
        "XGROUP" => {
            let sub = args.first().and_then(|a| a.to_string_lossy()).map(|s| s.to_uppercase()).unwrap_or_default();
            match sub.as_str() {
                "CREATE" => RespValue::ok(),
                "DESTROY" => RespValue::integer(0),
                "CREATECONSUMER" => RespValue::integer(1),
                "DELCONSUMER" => RespValue::integer(0),
                "SETID" => RespValue::ok(),
                _ => RespValue::error(format!("ERR unknown subcommand or wrong number of arguments for 'XGROUP' command")),
            }
        }
        "XACK" => RespValue::integer(0),
        "XCLAIM" => RespValue::array(vec![]),
        "XPENDING" => RespValue::array(vec![]),
        "XREADGROUP" => RespValue::null_array(),
        "XDEL" => stream::cmd_xdel(args, store, client).await,
        "XINFO" => stream::cmd_xinfo(args, store, client).await,

        // Bitmaps
        "SETBIT" => bitmap::cmd_setbit(args, store, client).await,
        "GETBIT" => bitmap::cmd_getbit(args, store, client).await,
        "BITCOUNT" => bitmap::cmd_bitcount(args, store, client).await,
        "BITOP" => bitmap::cmd_bitop(args, store, client).await,
        "BITPOS" => bitmap::cmd_bitpos(args, store, client).await,
        "BITFIELD" => bitmap::cmd_bitfield(args, store, client).await,
        "BITFIELD_RO" => bitmap::cmd_bitfield_ro(args, store, client).await,

        // HyperLogLog
        "PFADD" => hyperloglog::cmd_pfadd(args, store, client).await,
        "PFCOUNT" => hyperloglog::cmd_pfcount(args, store, client).await,
        "PFMERGE" => hyperloglog::cmd_pfmerge(args, store, client).await,

        // Geo
        "GEOADD" => geo::cmd_geoadd(args, store, client).await,
        "GEODIST" => geo::cmd_geodist(args, store, client).await,
        "GEOPOS" => geo::cmd_geopos(args, store, client).await,
        "GEOSEARCH" => geo::cmd_geosearch(args, store, client).await,
        "GEORADIUS" => geo::cmd_georadius(args, store, client).await,
        "GEORADIUSBYMEMBER" => geo::cmd_georadiusbymember(args, store, client).await,
        "GEOSEARCHSTORE" => geo::cmd_geosearchstore(args, store, client).await,
        "GEOHASH" => geo::cmd_geohash(args, store, client).await,
        "GEOMEMBERS" => geo::cmd_geomembers(args, store, client).await,

        // Transactions
        "MULTI" => transaction::cmd_multi(client),
        "EXEC" => transaction::cmd_exec(store, config, client, pubsub, pubsub_tx, key_watcher, script_cache).await,
        "DISCARD" => transaction::cmd_discard(client),
        "WATCH" => transaction::cmd_watch(args, store, client).await,
        "UNWATCH" => transaction::cmd_unwatch(client),

        // Pub/Sub
        "SUBSCRIBE" => pubsub::cmd_subscribe(args, client, pubsub, pubsub_tx).await,
        "UNSUBSCRIBE" => pubsub::cmd_unsubscribe(args, client, pubsub, pubsub_tx).await,
        "PSUBSCRIBE" => pubsub::cmd_psubscribe(args, client, pubsub, pubsub_tx).await,
        "PUNSUBSCRIBE" => pubsub::cmd_punsubscribe(args, client, pubsub, pubsub_tx).await,
        "PUBLISH" => pubsub::cmd_publish(args, pubsub).await,
        "PUBSUB" => pubsub::cmd_pubsub(args, pubsub).await,

        // Persistence
        "SAVE" => server_cmd::cmd_save(store, config).await,
        "BGSAVE" => server_cmd::cmd_bgsave(store, config).await,
        "BGREWRITEAOF" => server_cmd::cmd_bgrewriteaof(store, config).await,
        "LASTSAVE" => server_cmd::cmd_lastsave(),

        // Scripting
        "EVAL" => scripting::cmd_eval(args, store, config, client, pubsub, pubsub_tx, key_watcher, script_cache).await,
        "EVALSHA" => scripting::cmd_evalsha(args, store, config, client, pubsub, pubsub_tx, key_watcher, script_cache).await,
        "SCRIPT" => scripting::cmd_script(args, script_cache).await,

        // Stubs for compatibility
        "FUNCTION" => RespValue::ok(),
        "HELLO" => server_cmd::cmd_hello(args),
        "WAIT" => {
            // WAIT numreplicas timeout
            if args.len() != 2 {
                return wrong_arg_count("wait");
            }
            match arg_to_i64(&args[0]) {
                Some(n) if n >= 0 => {},
                _ => return RespValue::error("ERR value is not an integer or out of range"),
            }
            match arg_to_i64(&args[1]) {
                Some(t) if t >= 0 => {},
                _ => return RespValue::error("ERR timeout is not an integer or out of range"),
            }
            // No replication support: 0 replicas acknowledged
            RespValue::integer(0)
        }
        "FCALL" | "FCALL_RO" => RespValue::error("ERR No matching script. Please use FUNCTION LOAD."),
        "MONITOR" => {
            client.in_monitor = true;
            RespValue::ok()
        }
        "DIGEST" => {
            // DIGEST key - return a 40-char hex SHA1 hash of the key's serialized value
            if args.len() != 1 {
                return wrong_arg_count("digest");
            }
            let key = match arg_to_string(&args[0]) {
                Some(k) => k,
                None => return RespValue::null_bulk_string(),
            };
            let store_r = store.read().await;
            let db = &store_r.databases[client.db_index];
            match db.get_entry(&key) {
                Some(entry) => {
                    // Type check: DIGEST only works on strings
                    match &entry.value {
                        crate::types::RedisValue::String(s) => {
                            let hash = digest_hash(s.as_bytes());
                            RespValue::bulk_string(hash.into_bytes())
                        }
                        _ => wrong_type_error(),
                    }
                }
                None => RespValue::null_bulk_string(),
            }
        }
        "DELEX" => {
            // DELEX key [condition value] - Delete with optional conditions (Redis 8.0+)
            // Conditions: IFEQ, IFNE, IFDEQ, IFDNE
            if args.is_empty() || args.len() == 2 || args.len() > 3 {
                return wrong_arg_count("delex");
            }
            let key = match arg_to_string(&args[0]) {
                Some(k) => k,
                None => return RespValue::integer(0),
            };

            let mut store_w = store.write().await;
            let db = store_w.db(client.db_index);

            // Unconditional delete (no condition args)
            if args.len() == 1 {
                if db.del(&key) {
                    return RespValue::integer(1);
                } else {
                    return RespValue::integer(0);
                }
            }

            // Conditional delete
            let condition = match arg_to_string(&args[1]) {
                Some(c) => c.to_uppercase(),
                None => return RespValue::error("ERR Invalid condition"),
            };
            let cmp_val = match crate::command::arg_to_bytes(&args[2]) {
                Some(v) => v.to_vec(),
                None => return RespValue::error("ERR invalid value"),
            };

            let entry = db.get(&key);
            let should_delete = match entry {
                None => false, // Key doesn't exist: never delete
                Some(entry) => match &entry.value {
                    crate::types::RedisValue::String(s) => {
                        match condition.as_str() {
                            "IFEQ" => s.as_bytes() == cmp_val.as_slice(),
                            "IFNE" => s.as_bytes() != cmp_val.as_slice(),
                            "IFDEQ" => {
                                let cmp_str = String::from_utf8_lossy(&cmp_val);
                                if !is_valid_digest(&cmp_str) {
                                    return RespValue::error("ERR The digest must be exactly 16 hexadecimal characters");
                                }
                                let digest = digest_hash(s.as_bytes());
                                digest.eq_ignore_ascii_case(&cmp_str)
                            }
                            "IFDNE" => {
                                let cmp_str = String::from_utf8_lossy(&cmp_val);
                                if !is_valid_digest(&cmp_str) {
                                    return RespValue::error("ERR The digest must be exactly 16 hexadecimal characters");
                                }
                                let digest = digest_hash(s.as_bytes());
                                !digest.eq_ignore_ascii_case(&cmp_str)
                            }
                            _ => return RespValue::error("ERR Invalid condition"),
                        }
                    }
                    _ => return RespValue::error("ERR DELEX only supports string keys"),
                },
            };

            if should_delete {
                db.del(&key);
                RespValue::integer(1)
            } else {
                RespValue::integer(0)
            }
        }
        "PFSELFTEST" => RespValue::ok(),
        "PFDEBUG" => {
            // PFDEBUG encoding key -> return "sparse" or "dense"
            if args.first().and_then(|a| arg_to_string(a)).map(|s| s.to_uppercase()).as_deref() == Some("ENCODING") {
                RespValue::bulk_string(b"sparse".to_vec())
            } else {
                RespValue::ok()
            }
        }
        "SUBSTR" => string::cmd_getrange(args, store, client).await,
        "MEMORY" => {
            let sub = args.first().and_then(|a| a.to_string_lossy()).map(|s| s.to_uppercase()).unwrap_or_default();
            match sub.as_str() {
                "USAGE" => {
                    if let Some(key_val) = args.get(1).and_then(|a| a.to_string_lossy()) {
                        let store_r = store.read().await;
                        let db = &store_r.databases[client.db_index];
                        match db.get_entry(&key_val) {
                            Some(entry) => {
                                // Match Redis's memory accounting:
                                // dictEntry (24) + key SDS header+data (key_len+1) + robj (16)
                                let key_overhead = 24 + key_val.len() + 1 + 16;
                                let value_size = match &entry.value {
                                    crate::types::RedisValue::String(s) => {
                                        if s.as_i64().is_some() {
                                            0 // Integer stored in robj->ptr
                                        } else if s.len() <= 44 {
                                            s.len() + 1 // Embstr: data embedded in robj allocation
                                        } else {
                                            8 + s.len() + 1 // Raw: SDS header + data + null
                                        }
                                    }
                                    crate::types::RedisValue::List(l) => {
                                        let elem_bytes: usize = l.iter().map(|v| v.len() + 11).sum();
                                        128 + elem_bytes
                                    }
                                    crate::types::RedisValue::Hash(h) => {
                                        let field_bytes: usize = h.iter().map(|(k, v)| k.len() + v.len() + 24).sum();
                                        64 + field_bytes
                                    }
                                    crate::types::RedisValue::Set(s) => {
                                        let mem: usize = s.iter().map(|m| m.len() + 16).sum();
                                        64 + mem
                                    }
                                    crate::types::RedisValue::SortedSet(z) => {
                                        let mem: usize = z.iter().map(|(m, _)| m.len() + 40).sum();
                                        128 + mem
                                    }
                                    _ => 64,
                                };
                                RespValue::integer((key_overhead + value_size) as i64)
                            }
                            None => RespValue::null_bulk_string(),
                        }
                    } else {
                        RespValue::error("ERR wrong number of arguments for 'memory|usage' command")
                    }
                }
                "DOCTOR" | "MALLOC-STATS" | "PURGE" | "STATS" | "HELP" => RespValue::ok(),
                _ => RespValue::error("ERR unknown MEMORY subcommand"),
            }
        }
        "SLOWLOG" => RespValue::array(vec![]),
        "LATENCY" => RespValue::array(vec![]),
        "CLUSTER" => RespValue::error("ERR This instance has cluster support disabled"),
        "WAITAOF" => RespValue::array(vec![RespValue::integer(0), RespValue::integer(0)]),
        "ACL" => {
            let sub = args.first().and_then(|a| a.to_string_lossy()).map(|s| s.to_uppercase()).unwrap_or_default();
            match sub.as_str() {
                "WHOAMI" => RespValue::bulk_string(b"default".to_vec()),
                "LIST" => RespValue::array(vec![RespValue::bulk_string(b"user default on ~* &* +@all".to_vec())]),
                "USERS" => RespValue::array(vec![RespValue::bulk_string(b"default".to_vec())]),
                "GETUSER" => RespValue::array(vec![
                    RespValue::bulk_string(b"flags".to_vec()),
                    RespValue::array(vec![RespValue::bulk_string(b"on".to_vec())]),
                    RespValue::bulk_string(b"passwords".to_vec()),
                    RespValue::array(vec![]),
                    RespValue::bulk_string(b"commands".to_vec()),
                    RespValue::bulk_string(b"+@all".to_vec()),
                    RespValue::bulk_string(b"keys".to_vec()),
                    RespValue::bulk_string(b"~*".to_vec()),
                    RespValue::bulk_string(b"channels".to_vec()),
                    RespValue::bulk_string(b"&*".to_vec()),
                ]),
                "SETUSER" | "DELUSER" | "SAVE" | "LOAD" => RespValue::ok(),
                "CAT" => RespValue::array(vec![]),
                "LOG" => RespValue::array(vec![]),
                _ => RespValue::ok(),
            }
        }
        "REPLICAOF" | "SLAVEOF" => {
            // Replication not implemented; just return OK
            RespValue::ok()
        }
        "SYNC" | "PSYNC" => {
            // Return an empty RDB dump so TCL test framework's
            // attach_to_replication_stream can consume it without crashing
            RespValue::bulk_string(vec![])
        }
        _ => {
            let args_preview: Vec<String> = args
                .iter()
                .take(3)
                .filter_map(|a| a.to_string_lossy())
                .map(|s| format!("'{s}'"))
                .collect();
            RespValue::error(format!(
                "ERR unknown command '{}', with args beginning with: {}",
                cmd_name,
                args_preview.join(" ")
            ))
        }
    }
}

/// Check if a command name is known (for MULTI queueing error detection).
fn is_known_command(cmd: &str) -> bool {
    matches!(cmd,
        "PING" | "ECHO" | "QUIT" | "SELECT" | "AUTH" | "DBSIZE" | "FLUSHDB" | "FLUSHALL" | "SWAPDB" |
        "INFO" | "CONFIG" | "TIME" | "COMMAND" | "CLIENT" | "DEBUG" | "RESET" |
        "GET" | "SET" | "GETEX" | "GETSET" | "MGET" | "MSET" | "MSETNX" | "APPEND" | "STRLEN" | "LCS" |
        "INCR" | "DECR" | "INCRBY" | "DECRBY" | "INCRBYFLOAT" | "SETNX" | "SETEX" | "PSETEX" |
        "GETRANGE" | "SETRANGE" | "GETDEL" |
        "LPUSH" | "RPUSH" | "LPUSHX" | "RPUSHX" | "LPOP" | "RPOP" | "LLEN" | "LRANGE" | "LINDEX" | "LSET" | "LINSERT" |
        "LREM" | "LTRIM" | "RPOPLPUSH" | "LMOVE" | "LPOS" | "LMPOP" | "BLPOP" | "BRPOP" | "BLMOVE" |
        "HSET" | "HGET" | "HDEL" | "HEXISTS" | "HLEN" | "HKEYS" | "HVALS" | "HGETALL" | "HMSET" |
        "HMGET" | "HINCRBY" | "HINCRBYFLOAT" | "HSETNX" | "HRANDFIELD" | "HSCAN" | "HSTRLEN" | "HGETDEL" |
        "SADD" | "SREM" | "SISMEMBER" | "SMISMEMBER" | "SMEMBERS" | "SCARD" | "SPOP" | "SRANDMEMBER" |
        "SUNION" | "SINTER" | "SDIFF" | "SUNIONSTORE" | "SINTERSTORE" | "SDIFFSTORE" | "SMOVE" | "SSCAN" | "SINTERCARD" |
        "ZADD" | "ZREM" | "ZSCORE" | "ZRANK" | "ZREVRANK" | "ZCARD" | "ZCOUNT" | "ZRANGE" | "ZREVRANGE" |
        "ZRANGEBYSCORE" | "ZREVRANGEBYSCORE" | "ZRANGEBYLEX" | "ZREVRANGEBYLEX" | "ZINCRBY" |
        "ZUNIONSTORE" | "ZINTERSTORE" | "ZRANDMEMBER" | "ZSCAN" | "ZPOPMIN" | "ZPOPMAX" | "ZMSCORE" |
        "ZLEXCOUNT" | "ZREMRANGEBYSCORE" | "ZREMRANGEBYLEX" | "ZREMRANGEBYRANK" |
        "ZUNION" | "ZINTER" | "ZDIFF" | "ZDIFFSTORE" | "ZINTERCARD" | "ZMPOP" |
        "BZPOPMIN" | "BZPOPMAX" | "BZMPOP" |
        "XADD" | "XLEN" | "XRANGE" | "XREVRANGE" | "XREAD" | "XTRIM" | "XGROUP" | "XACK" |
        "XCLAIM" | "XPENDING" | "XREADGROUP" | "XDEL" | "XINFO" |
        "SETBIT" | "GETBIT" | "BITCOUNT" | "BITOP" | "BITPOS" | "BITFIELD" | "BITFIELD_RO" |
        "PFADD" | "PFCOUNT" | "PFMERGE" |
        "GEOADD" | "GEODIST" | "GEOPOS" | "GEOHASH" | "GEOSEARCH" | "GEOSEARCHSTORE" |
        "GEORADIUS" | "GEORADIUSBYMEMBER" | "GEOMEMBERS" |
        "DEL" | "UNLINK" | "EXISTS" | "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" |
        "EXPIRETIME" | "PEXPIRETIME" | "TTL" | "PTTL" | "PERSIST" | "TYPE" | "RENAME" | "RENAMENX" |
        "KEYS" | "SCAN" | "RANDOMKEY" | "OBJECT" | "SORT" | "SORT_RO" | "COPY" | "MOVE" | "DUMP" | "RESTORE" | "TOUCH" |
        "SUBSCRIBE" | "UNSUBSCRIBE" | "PUBLISH" | "PSUBSCRIBE" | "PUNSUBSCRIBE" | "PUBSUB" |
        "MULTI" | "EXEC" | "DISCARD" | "WATCH" | "UNWATCH" |
        "SAVE" | "BGSAVE" | "BGREWRITEAOF" | "LASTSAVE" |
        "EVAL" | "EVALSHA" | "SCRIPT" |
        "FUNCTION" | "HELLO" | "WAIT" | "FCALL" | "FCALL_RO" | "BLMPOP" |
        "PFSELFTEST" | "PFDEBUG" | "SUBSTR" | "MEMORY" | "SLOWLOG" | "LATENCY" | "CLUSTER" | "MONITOR" |
        "WAITAOF" | "ACL" | "REPLICAOF" | "SLAVEOF" | "SYNC" | "PSYNC" |
        "DIGEST" | "DELEX" | "MSETEX"
    )
}

/// Compute a 64-bit digest hash for the DIGEST command and IFDEQ/IFDNE conditions.
/// Returns a 16-character lowercase hex string.
pub fn digest_hash(data: &[u8]) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;
    let mut hasher = DefaultHasher::new();
    hasher.write(data);
    format!("{:016x}", hasher.finish())
}

/// Validate a digest string: must be exactly 16 hex characters.
pub fn is_valid_digest(s: &str) -> bool {
    s.len() == 16 && s.chars().all(|c| c.is_ascii_hexdigit())
}

/// Extract string bytes from a RespValue argument.
pub fn arg_to_bytes(arg: &RespValue) -> Option<&[u8]> {
    arg.as_str()
}

/// Extract a UTF-8 string from a RespValue argument.
pub fn arg_to_string(arg: &RespValue) -> Option<String> {
    arg.to_string_lossy()
}

/// Extract an i64 from a RespValue argument.
pub fn arg_to_i64(arg: &RespValue) -> Option<i64> {
    let s = arg.to_string_lossy()?;
    s.parse().ok()
}

/// Extract an f64 from a RespValue argument.
/// Supports standard floats, inf/-inf, and C-style hex floats (0x1.0p+0).
pub fn arg_to_f64(arg: &RespValue) -> Option<f64> {
    let s = arg.to_string_lossy()?;
    // Try standard parse first
    if let Ok(v) = s.parse::<f64>() {
        // Reject values that parse to infinity unless the input is explicitly "inf"
        if v.is_infinite() {
            let lower = s.to_lowercase();
            let trimmed = lower.trim_start_matches(['+', '-']);
            if trimmed != "inf" && trimmed != "infinity" {
                return None;
            }
        }
        return Some(v);
    }
    // Try hex float format (e.g., 0x1.0p+0, 0x0p+0)
    let s_lower = s.to_lowercase();
    if s_lower.starts_with("0x") || s_lower.starts_with("-0x") || s_lower.starts_with("+0x") {
        return parse_hex_float(&s_lower);
    }
    None
}

fn parse_hex_float(s: &str) -> Option<f64> {
    let (neg, hex_part) = if s.starts_with('-') {
        (true, &s[3..]) // skip "-0x"
    } else if s.starts_with('+') {
        (false, &s[3..]) // skip "+0x"
    } else {
        (false, &s[2..]) // skip "0x"
    };

    // Split by 'p' for exponent
    let (mantissa_str, exp_str) = if let Some(p) = hex_part.find('p') {
        (&hex_part[..p], &hex_part[p + 1..])
    } else {
        (hex_part, "0")
    };

    // Parse mantissa as hex
    let (int_part, frac_part) = if let Some(dot) = mantissa_str.find('.') {
        (&mantissa_str[..dot], Some(&mantissa_str[dot + 1..]))
    } else {
        (mantissa_str, None)
    };

    let int_val = u64::from_str_radix(int_part, 16).unwrap_or(0) as f64;
    let frac_val = if let Some(frac) = frac_part {
        if frac.is_empty() {
            0.0
        } else {
            let frac_int = u64::from_str_radix(frac, 16).ok()? as f64;
            frac_int / (16.0f64).powi(frac.len() as i32)
        }
    } else {
        0.0
    };

    let mantissa = int_val + frac_val;
    let exp: i32 = exp_str.parse().ok()?;
    let result = mantissa * (2.0f64).powi(exp);

    Some(if neg { -result } else { result })
}

/// Return a WRONGTYPE error.
pub fn wrong_type_error() -> RespValue {
    RespValue::error("WRONGTYPE Operation against a key holding the wrong kind of value")
}

/// Return a wrong number of arguments error.
pub fn wrong_arg_count(cmd: &str) -> RespValue {
    RespValue::error(format!("ERR wrong number of arguments for '{cmd}' command"))
}
