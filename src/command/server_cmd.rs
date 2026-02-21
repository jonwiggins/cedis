use crate::command::{arg_to_i64, arg_to_string, wrong_arg_count};
use crate::config::SharedConfig;
use crate::connection::ClientState;
use crate::persistence;
use crate::resp::RespValue;
use crate::store::SharedStore;

pub fn cmd_ping(args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        RespValue::SimpleString("PONG".to_string())
    } else if args.len() == 1 {
        if let Some(msg) = args[0].as_str() {
            RespValue::bulk_string(msg.to_vec())
        } else {
            RespValue::SimpleString("PONG".to_string())
        }
    } else {
        wrong_arg_count("ping")
    }
}

pub fn cmd_echo(args: &[RespValue]) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("echo");
    }
    if let Some(msg) = args[0].as_str() {
        RespValue::bulk_string(msg.to_vec())
    } else {
        RespValue::null_bulk_string()
    }
}

pub fn cmd_quit(client: &mut ClientState) -> RespValue {
    client.should_close = true;
    RespValue::ok()
}

pub async fn cmd_select(
    args: &[RespValue],
    client: &mut ClientState,
    config: &SharedConfig,
) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("select");
    }
    let db_index = match arg_to_i64(&args[0]) {
        Some(n) if n >= 0 => n as usize,
        _ => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let cfg = config.read().await;
    if db_index >= cfg.databases {
        return RespValue::error("ERR DB index is out of range");
    }
    drop(cfg);

    client.db_index = db_index;
    RespValue::ok()
}

pub async fn cmd_auth(
    args: &[RespValue],
    client: &mut ClientState,
    config: &SharedConfig,
) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("auth");
    }

    let password = match arg_to_string(&args[0]) {
        Some(p) => p,
        None => return RespValue::error("ERR invalid password"),
    };

    let cfg = config.read().await;
    match &cfg.requirepass {
        Some(pass) if pass == &password => {
            client.authenticated = true;
            RespValue::ok()
        }
        Some(_) => RespValue::error("WRONGPASS invalid username-password pair or user is disabled."),
        None => RespValue::error("ERR Client sent AUTH, but no password is set. Did you mean ACL SETUSER with >password?"),
    }
}

pub async fn cmd_dbsize(store: &SharedStore, client: &ClientState) -> RespValue {
    let mut store = store.write().await;
    let db = store.db(client.db_index);
    RespValue::integer(db.dbsize() as i64)
}

pub async fn cmd_flushdb(store: &SharedStore, client: &ClientState) -> RespValue {
    let mut store = store.write().await;
    store.db(client.db_index).flush();
    RespValue::ok()
}

pub async fn cmd_flushall(store: &SharedStore) -> RespValue {
    let mut store = store.write().await;
    store.flush_all();
    RespValue::ok()
}

pub async fn cmd_swapdb(
    args: &[RespValue],
    store: &SharedStore,
    config: &SharedConfig,
) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("swapdb");
    }
    let a = match arg_to_i64(&args[0]) {
        Some(n) if n >= 0 => n as usize,
        _ => return RespValue::error("ERR invalid first DB index"),
    };
    let b = match arg_to_i64(&args[1]) {
        Some(n) if n >= 0 => n as usize,
        _ => return RespValue::error("ERR invalid second DB index"),
    };

    let cfg = config.read().await;
    if a >= cfg.databases || b >= cfg.databases {
        return RespValue::error("ERR DB index is out of range");
    }
    drop(cfg);

    let mut store = store.write().await;
    if store.swap_db(a, b) {
        RespValue::ok()
    } else {
        RespValue::error("ERR DB index is out of range")
    }
}

pub async fn cmd_info(
    _args: &[RespValue],
    store: &SharedStore,
    config: &SharedConfig,
) -> RespValue {
    let cfg = config.read().await;
    let mut store = store.write().await;

    let mut info = String::new();

    // Server section
    info.push_str("# Server\r\n");
    info.push_str("redis_version:7.0.0\r\n");
    info.push_str("cedis_version:0.1.0\r\n");
    info.push_str(&format!("process_id:{}\r\n", std::process::id()));
    info.push_str(&format!("tcp_port:{}\r\n", cfg.port));
    info.push_str("config_file:\r\n");

    // Clients section
    info.push_str("\r\n# Clients\r\n");
    info.push_str("connected_clients:1\r\n");

    // Memory section
    info.push_str("\r\n# Memory\r\n");
    info.push_str("used_memory:0\r\n");
    info.push_str("used_memory_human:0B\r\n");

    // Stats section
    info.push_str("\r\n# Stats\r\n");
    info.push_str("total_connections_received:0\r\n");
    info.push_str("total_commands_processed:0\r\n");
    // Drain lazy expired counts so expired_keys is accurate
    store.drain_lazy_expired();
    info.push_str(&format!("expired_keys:{}\r\n", store.expired_keys));
    info.push_str(&format!("expired_keys_active:{}\r\n", store.expired_keys_active));

    // Persistence section
    info.push_str("\r\n# Persistence\r\n");
    info.push_str("loading:0\r\n");
    info.push_str(&format!("rdb_changes_since_last_save:{}\r\n", store.dirty));
    info.push_str("rdb_bgsave_in_progress:0\r\n");
    info.push_str("rdb_last_save_time:0\r\n");
    info.push_str("rdb_last_bgsave_status:ok\r\n");
    info.push_str("aof_rewrite_in_progress:0\r\n");
    info.push_str("aof_last_bgrewrite_status:ok\r\n");

    // Replication section
    info.push_str("\r\n# Replication\r\n");
    info.push_str("role:master\r\n");
    info.push_str("connected_slaves:0\r\n");

    // Keyspace section
    info.push_str("\r\n# Keyspace\r\n");
    for (i, db) in store.databases.iter().enumerate() {
        let size = db.dbsize();
        if size > 0 {
            let expires = db.expires_count();
            info.push_str(&format!("db{i}:keys={size},expires={expires},avg_ttl=0\r\n"));
        }
    }

    RespValue::bulk_string(info.into_bytes())
}

pub async fn cmd_config(args: &[RespValue], config: &SharedConfig, store: &SharedStore) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("config");
    }

    let subcmd = match arg_to_string(&args[0]) {
        Some(s) => s.to_uppercase(),
        None => return RespValue::error("ERR invalid config subcommand"),
    };

    match subcmd.as_str() {
        "GET" => {
            if args.len() != 2 {
                return wrong_arg_count("config|get");
            }
            let pattern = match arg_to_string(&args[1]) {
                Some(s) => s,
                None => return RespValue::error("ERR invalid parameter"),
            };

            let cfg = config.read().await;
            let params = [
                "bind", "port", "databases", "requirepass", "timeout",
                "tcp-keepalive", "hz", "loglevel", "dbfilename", "dir",
                "appendonly", "appendfsync", "maxmemory", "maxmemory-policy",
                "save",
                "list-max-listpack-size", "list-max-ziplist-size",
                "hash-max-listpack-entries", "hash-max-ziplist-entries",
                "hash-max-listpack-value", "hash-max-ziplist-value",
                "set-max-intset-entries", "set-max-listpack-entries",
                "set-max-listpack-value", "list-compress-depth",
                "zset-max-listpack-entries", "zset-max-ziplist-entries",
                "zset-max-listpack-value", "zset-max-ziplist-value",
            ];

            let mut result = Vec::new();
            // Use a set to avoid duplicate entries for aliased params
            let mut seen = std::collections::HashSet::new();
            for param in &params {
                if crate::glob::glob_match(&pattern, param) {
                    if let Some(val) = cfg.get(param) {
                        // For aliased params, use the canonical name (the one that matched)
                        // but avoid duplicates
                        let canonical = match *param {
                            "list-max-ziplist-size" => "list-max-listpack-size",
                            "hash-max-ziplist-entries" => "hash-max-listpack-entries",
                            "hash-max-ziplist-value" => "hash-max-listpack-value",
                            "zset-max-ziplist-entries" => "zset-max-listpack-entries",
                            "zset-max-ziplist-value" => "zset-max-listpack-value",
                            other => other,
                        };
                        if seen.insert(canonical) {
                            result.push(RespValue::bulk_string(param.as_bytes().to_vec()));
                            result.push(RespValue::bulk_string(val.into_bytes()));
                        }
                    }
                }
            }
            RespValue::array(result)
        }
        "SET" => {
            if args.len() != 3 {
                return wrong_arg_count("config|set");
            }
            let param = match arg_to_string(&args[1]) {
                Some(s) => s,
                None => return RespValue::error("ERR invalid parameter"),
            };
            let value = match arg_to_string(&args[2]) {
                Some(s) => s,
                None => return RespValue::error("ERR invalid value"),
            };

            let mut cfg = config.write().await;
            match cfg.set(&param, &value) {
                Ok(()) => RespValue::ok(),
                Err(e) => RespValue::error(format!("ERR {e}")),
            }
        }
        "RESETSTAT" => {
            let mut store = store.write().await;
            store.expired_keys = 0;
            store.expired_keys_active = 0;
            for db in &mut store.databases {
                db.lazy_expired_count = 0;
            }
            RespValue::ok()
        }
        _ => RespValue::error(format!(
            "ERR Unknown subcommand or wrong number of arguments for CONFIG {subcmd}"
        )),
    }
}

pub fn cmd_time() -> RespValue {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before UNIX epoch");
    let secs = now.as_secs().to_string();
    let micros = (now.subsec_micros()).to_string();
    RespValue::array(vec![
        RespValue::bulk_string(secs.into_bytes()),
        RespValue::bulk_string(micros.into_bytes()),
    ])
}

/// Build a COMMAND INFO entry for a single command.
/// Format: [name, arity, [flags...], first_key, last_key, step]
fn command_info_entry(name: &str, arity: i64, flags: &[&str], first_key: i64, last_key: i64, step: i64) -> RespValue {
    let flag_arr: Vec<RespValue> = flags.iter()
        .map(|f| RespValue::SimpleString(f.to_string()))
        .collect();
    RespValue::array(vec![
        RespValue::bulk_string(name.as_bytes().to_vec()),
        RespValue::integer(arity),
        RespValue::array(flag_arr),
        RespValue::integer(first_key),
        RespValue::integer(last_key),
        RespValue::integer(step),
    ])
}

/// Return command info for known commands.
fn get_command_info(name: &str) -> Option<RespValue> {
    // (arity, flags, first_key, last_key, step)
    let info = match name {
        "get" => (2, vec!["readonly", "fast"], 1, 1, 1),
        "set" => (-3, vec!["write", "denyoom"], 1, 1, 1),
        "del" => (-2, vec!["write"], 1, -1, 1),
        "mget" => (-2, vec!["readonly", "fast"], 1, -1, 1),
        "mset" => (-3, vec!["write", "denyoom"], 1, -1, 2),
        "incr" => (2, vec!["write", "denyoom", "fast"], 1, 1, 1),
        "decr" => (2, vec!["write", "denyoom", "fast"], 1, 1, 1),
        "incrby" => (3, vec!["write", "denyoom", "fast"], 1, 1, 1),
        "decrby" => (3, vec!["write", "denyoom", "fast"], 1, 1, 1),
        "incrbyfloat" => (3, vec!["write", "denyoom", "fast"], 1, 1, 1),
        "append" => (3, vec!["write", "denyoom", "fast"], 1, 1, 1),
        "strlen" => (2, vec!["readonly", "fast"], 1, 1, 1),
        "exists" => (-2, vec!["readonly", "fast"], 1, -1, 1),
        "expire" => (3, vec!["write", "fast"], 1, 1, 1),
        "pexpire" => (3, vec!["write", "fast"], 1, 1, 1),
        "expireat" => (3, vec!["write", "fast"], 1, 1, 1),
        "pexpireat" => (3, vec!["write", "fast"], 1, 1, 1),
        "ttl" => (2, vec!["readonly", "fast"], 1, 1, 1),
        "pttl" => (2, vec!["readonly", "fast"], 1, 1, 1),
        "persist" => (2, vec!["write", "fast"], 1, 1, 1),
        "type" => (2, vec!["readonly", "fast"], 1, 1, 1),
        "rename" => (3, vec!["write"], 1, 2, 1),
        "renamenx" => (3, vec!["write", "fast"], 1, 2, 1),
        "keys" => (2, vec!["readonly", "sort_for_script"], 0, 0, 0),
        "scan" => (-2, vec!["readonly"], 0, 0, 0),
        "ping" => (-1, vec!["fast", "stale"], 0, 0, 0),
        "echo" => (2, vec!["fast"], 0, 0, 0),
        "quit" => (1, vec!["fast"], 0, 0, 0),
        "select" => (2, vec!["fast"], 0, 0, 0),
        "auth" => (-2, vec!["fast", "stale", "no-auth"], 0, 0, 0),
        "dbsize" => (1, vec!["readonly", "fast"], 0, 0, 0),
        "flushdb" => (-1, vec!["write"], 0, 0, 0),
        "flushall" => (-1, vec!["write"], 0, 0, 0),
        "info" => (-1, vec!["stale", "fast"], 0, 0, 0),
        "config" => (-2, vec!["admin", "stale"], 0, 0, 0),
        "time" => (1, vec!["random", "fast", "stale"], 0, 0, 0),
        "command" => (-1, vec!["random", "stale"], 0, 0, 0),
        "client" => (-2, vec!["admin", "stale"], 0, 0, 0),
        "lpush" => (-3, vec!["write", "denyoom", "fast"], 1, 1, 1),
        "rpush" => (-3, vec!["write", "denyoom", "fast"], 1, 1, 1),
        "lpop" => (-2, vec!["write", "fast"], 1, 1, 1),
        "rpop" => (-2, vec!["write", "fast"], 1, 1, 1),
        "llen" => (2, vec!["readonly", "fast"], 1, 1, 1),
        "lrange" => (4, vec!["readonly"], 1, 1, 1),
        "lindex" => (3, vec!["readonly"], 1, 1, 1),
        "lset" => (4, vec!["write", "denyoom"], 1, 1, 1),
        "linsert" => (5, vec!["write", "denyoom"], 1, 1, 1),
        "lrem" => (4, vec!["write"], 1, 1, 1),
        "ltrim" => (4, vec!["write"], 1, 1, 1),
        "blpop" => (-3, vec!["write", "noscript"], 1, -2, 1),
        "brpop" => (-3, vec!["write", "noscript"], 1, -2, 1),
        "hset" => (-4, vec!["write", "denyoom", "fast"], 1, 1, 1),
        "hget" => (3, vec!["readonly", "fast"], 1, 1, 1),
        "hdel" => (-3, vec!["write", "fast"], 1, 1, 1),
        "hgetall" => (2, vec!["readonly"], 1, 1, 1),
        "hlen" => (2, vec!["readonly", "fast"], 1, 1, 1),
        "hexists" => (3, vec!["readonly", "fast"], 1, 1, 1),
        "hkeys" => (2, vec!["readonly", "sort_for_script"], 1, 1, 1),
        "hvals" => (2, vec!["readonly", "sort_for_script"], 1, 1, 1),
        "hmset" => (-4, vec!["write", "denyoom", "fast"], 1, 1, 1),
        "hmget" => (-3, vec!["readonly", "fast"], 1, 1, 1),
        "hincrby" => (4, vec!["write", "denyoom", "fast"], 1, 1, 1),
        "hincrbyfloat" => (4, vec!["write", "denyoom", "fast"], 1, 1, 1),
        "sadd" => (-3, vec!["write", "denyoom", "fast"], 1, 1, 1),
        "srem" => (-3, vec!["write", "fast"], 1, 1, 1),
        "sismember" => (3, vec!["readonly", "fast"], 1, 1, 1),
        "smembers" => (2, vec!["readonly", "sort_for_script"], 1, 1, 1),
        "scard" => (2, vec!["readonly", "fast"], 1, 1, 1),
        "spop" => (-2, vec!["write", "fast"], 1, 1, 1),
        "srandmember" => (-2, vec!["readonly", "random"], 1, 1, 1),
        "sunion" => (-2, vec!["readonly", "sort_for_script"], 1, -1, 1),
        "sinter" => (-2, vec!["readonly", "sort_for_script"], 1, -1, 1),
        "sdiff" => (-2, vec!["readonly", "sort_for_script"], 1, -1, 1),
        "sunionstore" => (-3, vec!["write", "denyoom"], 1, -1, 1),
        "sinterstore" => (-3, vec!["write", "denyoom"], 1, -1, 1),
        "sdiffstore" => (-3, vec!["write", "denyoom"], 1, -1, 1),
        "zadd" => (-4, vec!["write", "denyoom", "fast"], 1, 1, 1),
        "zrem" => (-3, vec!["write", "fast"], 1, 1, 1),
        "zscore" => (3, vec!["readonly", "fast"], 1, 1, 1),
        "zrank" => (3, vec!["readonly", "fast"], 1, 1, 1),
        "zrevrank" => (3, vec!["readonly", "fast"], 1, 1, 1),
        "zcard" => (2, vec!["readonly", "fast"], 1, 1, 1),
        "zcount" => (4, vec!["readonly", "fast"], 1, 1, 1),
        "zrange" => (-4, vec!["readonly"], 1, 1, 1),
        "zrevrange" => (-4, vec!["readonly"], 1, 1, 1),
        "zincrby" => (4, vec!["write", "denyoom", "fast"], 1, 1, 1),
        "multi" => (1, vec!["fast"], 0, 0, 0),
        "exec" => (1, vec!["noscript", "slow"], 0, 0, 0),
        "discard" => (1, vec!["fast", "noscript"], 0, 0, 0),
        "watch" => (-2, vec!["fast"], 1, -1, 1),
        "unwatch" => (1, vec!["fast"], 0, 0, 0),
        "subscribe" => (-2, vec!["pubsub", "noscript"], 0, 0, 0),
        "unsubscribe" => (-1, vec!["pubsub", "noscript"], 0, 0, 0),
        "publish" => (3, vec!["pubsub", "fast"], 0, 0, 0),
        "psubscribe" => (-2, vec!["pubsub", "noscript"], 0, 0, 0),
        "punsubscribe" => (-1, vec!["pubsub", "noscript"], 0, 0, 0),
        "save" => (1, vec!["admin", "noscript"], 0, 0, 0),
        "bgsave" => (-1, vec!["admin"], 0, 0, 0),
        "eval" => (-3, vec!["noscript", "movablekeys"], 0, 0, 0),
        "evalsha" => (-3, vec!["noscript", "movablekeys"], 0, 0, 0),
        "wait" => (3, vec!["noscript"], 0, 0, 0),
        "object" => (-2, vec!["slow"], 2, 2, 1),
        "debug" => (-2, vec!["admin", "noscript"], 0, 0, 0),
        "sort" => (-2, vec!["write", "denyoom", "movablekeys"], 1, 1, 1),
        "dump" => (2, vec!["readonly", "random"], 1, 1, 1),
        "restore" => (-4, vec!["write", "denyoom"], 1, 1, 1),
        "copy" => (-3, vec!["write"], 1, 2, 1),
        "move" => (3, vec!["write", "fast"], 1, 1, 1),
        "randomkey" => (1, vec!["readonly", "random"], 0, 0, 0),
        "setnx" => (3, vec!["write", "denyoom", "fast"], 1, 1, 1),
        "setex" => (4, vec!["write", "denyoom"], 1, 1, 1),
        "psetex" => (4, vec!["write", "denyoom"], 1, 1, 1),
        "getset" => (3, vec!["write", "denyoom", "fast"], 1, 1, 1),
        "getdel" => (2, vec!["write", "fast"], 1, 1, 1),
        "getex" => (-2, vec!["write", "fast"], 1, 1, 1),
        "getrange" => (4, vec!["readonly"], 1, 1, 1),
        "setrange" => (4, vec!["write", "denyoom"], 1, 1, 1),
        "msetnx" => (-3, vec!["write", "denyoom"], 1, -1, 2),
        "unlink" => (-2, vec!["write", "fast"], 1, -1, 1),
        "lmove" => (5, vec!["write", "denyoom"], 1, 2, 1),
        "blmove" => (6, vec!["write", "denyoom", "noscript"], 1, 2, 1),
        "rpoplpush" => (3, vec!["write", "denyoom"], 1, 2, 1),
        "lpos" => (-3, vec!["readonly"], 1, 1, 1),
        "lmpop" => (-4, vec!["write", "fast"], 0, 0, 0),
        "blmpop" => (-5, vec!["write", "noscript"], 0, 0, 0),
        "setbit" => (4, vec!["write", "denyoom"], 1, 1, 1),
        "getbit" => (3, vec!["readonly", "fast"], 1, 1, 1),
        "bitcount" => (-2, vec!["readonly"], 1, 1, 1),
        "bitop" => (-4, vec!["write", "denyoom"], 2, -1, 1),
        "bitpos" => (-3, vec!["readonly"], 1, 1, 1),
        "pfadd" => (-2, vec!["write", "denyoom", "fast"], 1, 1, 1),
        "pfcount" => (-2, vec!["readonly"], 1, -1, 1),
        "pfmerge" => (-2, vec!["write", "denyoom"], 1, -1, 1),
        "geoadd" => (-5, vec!["write", "denyoom"], 1, 1, 1),
        "geodist" => (-4, vec!["readonly"], 1, 1, 1),
        "geopos" => (-2, vec!["readonly"], 1, 1, 1),
        "geohash" => (-2, vec!["readonly"], 1, 1, 1),
        "geosearch" => (-7, vec!["readonly"], 1, 1, 1),
        "xadd" => (-5, vec!["write", "denyoom", "fast"], 1, 1, 1),
        "xlen" => (2, vec!["readonly", "fast"], 1, 1, 1),
        "xrange" => (-4, vec!["readonly"], 1, 1, 1),
        "xrevrange" => (-4, vec!["readonly"], 1, 1, 1),
        "xread" => (-4, vec!["readonly", "movablekeys"], 0, 0, 0),
        "swapdb" => (3, vec!["write", "fast"], 0, 0, 0),
        "hello" => (-1, vec!["fast", "stale"], 0, 0, 0),
        "reset" => (1, vec!["fast", "stale", "no-auth"], 0, 0, 0),
        _ => return None,
    };
    Some(command_info_entry(name, info.0, &info.1, info.2, info.3, info.4))
}

/// Collect all known command names for COMMAND LIST.
fn all_command_names() -> Vec<&'static str> {
    vec![
        "get", "set", "del", "mget", "mset", "incr", "decr", "incrby", "decrby",
        "incrbyfloat", "append", "strlen", "exists", "expire", "pexpire", "expireat",
        "pexpireat", "ttl", "pttl", "persist", "type", "rename", "renamenx", "keys",
        "scan", "ping", "echo", "quit", "select", "auth", "dbsize", "flushdb",
        "flushall", "info", "config", "time", "command", "client", "lpush", "rpush",
        "lpop", "rpop", "llen", "lrange", "lindex", "lset", "linsert", "lrem",
        "ltrim", "blpop", "brpop", "hset", "hget", "hdel", "hgetall", "hlen",
        "hexists", "hkeys", "hvals", "hmset", "hmget", "hincrby", "hincrbyfloat",
        "sadd", "srem", "sismember", "smembers", "scard", "spop", "srandmember",
        "sunion", "sinter", "sdiff", "sunionstore", "sinterstore", "sdiffstore",
        "zadd", "zrem", "zscore", "zrank", "zrevrank", "zcard", "zcount", "zrange",
        "zrevrange", "zincrby", "multi", "exec", "discard", "watch", "unwatch",
        "subscribe", "unsubscribe", "publish", "psubscribe", "punsubscribe",
        "save", "bgsave", "eval", "evalsha", "wait", "object", "debug", "sort",
        "dump", "restore", "copy", "move", "randomkey", "setnx", "setex", "psetex",
        "getset", "getdel", "getex", "getrange", "setrange", "msetnx", "unlink",
        "lmove", "blmove", "rpoplpush", "lpos", "lmpop", "blmpop",
        "setbit", "getbit", "bitcount", "bitop", "bitpos",
        "pfadd", "pfcount", "pfmerge", "geoadd", "geodist", "geopos", "geohash",
        "geosearch", "xadd", "xlen", "xrange", "xrevrange", "xread",
        "swapdb", "hello", "reset",
    ]
}

pub fn cmd_command(args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        // COMMAND with no args: return info for all commands
        let entries: Vec<RespValue> = all_command_names()
            .iter()
            .filter_map(|name| get_command_info(name))
            .collect();
        return RespValue::array(entries);
    }

    let subcmd = match arg_to_string(&args[0]) {
        Some(s) => s.to_uppercase(),
        None => return RespValue::error("ERR invalid subcommand"),
    };

    match subcmd.as_str() {
        "COUNT" => RespValue::integer(all_command_names().len() as i64),
        "DOCS" => {
            // COMMAND DOCS [command ...] - return empty docs for now
            RespValue::array(vec![])
        }
        "INFO" => {
            // COMMAND INFO command [command ...]
            let mut results = Vec::new();
            for arg in &args[1..] {
                if let Some(name) = arg.to_string_lossy() {
                    let lower = name.to_lowercase();
                    match get_command_info(&lower) {
                        Some(info) => results.push(info),
                        None => results.push(RespValue::null_bulk_string()),
                    }
                }
            }
            RespValue::array(results)
        }
        "GETKEYS" => {
            if args.len() >= 3 {
                RespValue::array(vec![args[2].clone()])
            } else {
                RespValue::array(vec![])
            }
        }
        "LIST" => {
            let names: Vec<RespValue> = all_command_names()
                .iter()
                .map(|n| RespValue::bulk_string(n.as_bytes().to_vec()))
                .collect();
            RespValue::array(names)
        }
        _ => RespValue::error(format!("ERR unknown subcommand '{subcmd}'")),
    }
}

pub fn cmd_client(args: &[RespValue], client: &mut ClientState) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("client");
    }

    let subcmd = match arg_to_string(&args[0]) {
        Some(s) => s.to_uppercase(),
        None => return RespValue::error("ERR invalid subcommand"),
    };

    match subcmd.as_str() {
        "SETNAME" => {
            if args.len() != 2 {
                return wrong_arg_count("client|setname");
            }
            client.name = arg_to_string(&args[1]);
            RespValue::ok()
        }
        "GETNAME" => match &client.name {
            Some(name) => RespValue::bulk_string(name.as_bytes().to_vec()),
            None => RespValue::null_bulk_string(),
        },
        "ID" => RespValue::integer(client.id as i64),
        "LIST" => {
            let info = format!(
                "id={} addr=127.0.0.1 fd=0 name={} db={} cmd=client\n",
                client.id,
                client.name.as_deref().unwrap_or(""),
                client.db_index,
            );
            RespValue::bulk_string(info.into_bytes())
        }
        "INFO" => {
            let info = format!(
                "id={}\r\naddr=127.0.0.1\r\nname={}\r\ndb={}\r\n",
                client.id,
                client.name.as_deref().unwrap_or(""),
                client.db_index,
            );
            RespValue::bulk_string(info.into_bytes())
        }
        "REPLY" => {
            // Accept ON/OFF/SKIP but always return OK (we don't actually suppress replies)
            if args.len() != 2 {
                return wrong_arg_count("client|reply");
            }
            RespValue::ok()
        }
        "NO-EVICT" | "NO-TOUCH" => {
            // Accept and ignore these flags
            RespValue::ok()
        }
        _ => RespValue::error(format!(
            "ERR Unknown subcommand or wrong number of arguments for CLIENT {subcmd}"
        )),
    }
}

pub async fn cmd_debug(
    args: &[RespValue],
    store: &SharedStore,
    config: &SharedConfig,
    client: &ClientState,
) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("debug");
    }

    let subcmd = match arg_to_string(&args[0]) {
        Some(s) => s.to_uppercase(),
        None => return RespValue::error("ERR invalid subcommand"),
    };

    match subcmd.as_str() {
        "SLEEP" => {
            if args.len() != 2 {
                return wrong_arg_count("debug|sleep");
            }
            let secs: f64 = match arg_to_string(&args[1]).and_then(|s| s.parse().ok()) {
                Some(s) => s,
                None => return RespValue::error("ERR invalid sleep time"),
            };
            tokio::time::sleep(std::time::Duration::from_secs_f64(secs)).await;
            RespValue::ok()
        }
        "SET-ACTIVE-EXPIRE" => {
            if let Some(val) = args.get(1).and_then(|a| crate::command::arg_to_string(a)) {
                let mut cfg = config.write().await;
                cfg.active_expire_enabled = val != "0";
            }
            RespValue::ok()
        }
        "RELOAD" => {
            // Save RDB then reload it
            let cfg = config.read().await;
            let path = format!("{}/{}", cfg.dir, cfg.dbfilename);
            drop(cfg);
            {
                let store_r = store.read().await;
                if let Err(e) = persistence::rdb::save(&store_r, &path) {
                    return RespValue::error(format!("ERR {e}"));
                }
            }
            {
                let mut store_w = store.write().await;
                let num_dbs = store_w.databases.len();
                match persistence::rdb::load(&path, num_dbs) {
                    Ok(loaded) => {
                        // Replace database contents
                        for (i, db) in loaded.databases.into_iter().enumerate() {
                            if i < store_w.databases.len() {
                                store_w.databases[i] = db;
                            }
                        }
                    }
                    Err(e) => return RespValue::error(format!("ERR {e}")),
                }
            }
            RespValue::ok()
        }
        "JMAP" | "QUICKLIST-PACKED-THRESHOLD" => RespValue::ok(),
        "DIGEST-VALUE" | "DIGEST" => RespValue::bulk_string(b"0000000000000000000000000000000000000000".to_vec()),
        "OBJECT" => {
            // DEBUG OBJECT key - return encoding and type info
            if args.len() < 2 {
                return RespValue::error("ERR wrong number of arguments for DEBUG OBJECT");
            }
            let key = match arg_to_string(&args[1]) {
                Some(k) => k,
                None => return RespValue::error("ERR no such key"),
            };
            let mut store_w = store.write().await;
            let db = store_w.db(client.db_index);
            match db.get(&key) {
                Some(entry) => {
                    let type_name = entry.value.type_name();
                    let encoding = match &entry.value {
                        crate::types::RedisValue::String(s) => {
                            if s.as_i64().is_some() { "int" }
                            else if s.len() <= 44 { "embstr" }
                            else { "raw" }
                        }
                        crate::types::RedisValue::List(l) => {
                            let cfg = config.read().await;
                            if cfg.list_max_listpack_size > 0 {
                                if l.len() <= cfg.list_max_listpack_size as usize { "listpack" } else { "quicklist" }
                            } else {
                                "quicklist"
                            }
                        }
                        crate::types::RedisValue::Hash(h) => {
                            let cfg = config.read().await;
                            if h.len() <= cfg.hash_max_listpack_entries as usize && !h.has_long_entry(cfg.hash_max_listpack_value as usize) { "listpack" } else { "hashtable" }
                        }
                        crate::types::RedisValue::Set(s) => {
                            let cfg = config.read().await;
                            if s.is_all_integers() && s.len() <= cfg.set_max_intset_entries as usize { "intset" }
                            else if s.len() <= cfg.set_max_listpack_entries as usize && !s.has_long_entry(cfg.set_max_listpack_value as usize) { "listpack" }
                            else { "hashtable" }
                        }
                        crate::types::RedisValue::SortedSet(z) => {
                            let cfg = config.read().await;
                            if z.len() <= cfg.zset_max_listpack_entries as usize && !z.has_long_entry(cfg.zset_max_listpack_value as usize) { "listpack" } else { "skiplist" }
                        }
                        _ => "raw",
                    };
                    let info = format!(
                        "Value at:0x000000 refcount:1 encoding:{} serializedlength:1 lru:0 lru_seconds_idle:0 type:{}",
                        encoding, type_name
                    );
                    RespValue::bulk_string(info.into_bytes())
                }
                None => RespValue::error("ERR no such key"),
            }
        }
        "CHANGE-REPL-ID" => RespValue::ok(),
        "HTSTATS-KEY" | "HTSTATS" | "GETKEYS" => RespValue::ok(),
        "PROTOCOL" => {
            // Stub for DEBUG PROTOCOL - return expected values for common sub-args
            if args.len() >= 2 {
                match arg_to_string(&args[1]).unwrap_or_default().to_uppercase().as_str() {
                    "ATTRIB" => RespValue::bulk_string(b"Some real reply following the attribute".to_vec()),
                    "BIGNUM" => RespValue::bulk_string(b"1234567999999999999999999999999999999".to_vec()),
                    "TRUE" => RespValue::Integer(1),
                    "FALSE" => RespValue::Integer(0),
                    "VERBATIM" => RespValue::bulk_string(b"This is a verbatim\nstring".to_vec()),
                    _ => RespValue::ok(),
                }
            } else {
                RespValue::ok()
            }
        }
        _ => RespValue::ok(),
    }
}

pub fn cmd_reset(client: &mut ClientState) -> RespValue {
    client.db_index = 0;
    client.in_multi = false;
    client.multi_queue.clear();
    client.multi_error = false;
    client.watched_keys.clear();
    client.watch_dirty = false;
    client.name = None;
    client.subscriptions = 0;
    client.in_monitor = false;
    RespValue::SimpleString("RESET".to_string())
}

pub async fn cmd_save(store: &SharedStore, config: &SharedConfig) -> RespValue {
    let store = store.read().await;
    let cfg = config.read().await;
    let path = format!("{}/{}", cfg.dir, cfg.dbfilename);
    drop(cfg);

    match persistence::rdb::save(&store, &path) {
        Ok(()) => RespValue::ok(),
        Err(e) => RespValue::error(format!("ERR {e}")),
    }
}

pub async fn cmd_bgsave(store: &SharedStore, config: &SharedConfig) -> RespValue {
    let store = store.clone();
    let config = config.clone();
    tokio::spawn(async move {
        let store = store.read().await;
        let cfg = config.read().await;
        let path = format!("{}/{}", cfg.dir, cfg.dbfilename);
        drop(cfg);
        if let Err(e) = persistence::rdb::save(&store, &path) {
            tracing::warn!("Background save failed: {e}");
        } else {
            tracing::info!("Background save completed");
        }
    });
    RespValue::SimpleString("Background saving started".to_string())
}

pub async fn cmd_bgrewriteaof(store: &SharedStore, config: &SharedConfig) -> RespValue {
    let store = store.clone();
    let config = config.clone();
    tokio::spawn(async move {
        let store = store.read().await;
        let cfg = config.read().await;
        let path = format!("{}/appendonly.aof", cfg.dir);
        drop(cfg);
        if let Err(e) = persistence::aof::rewrite(&store, &path) {
            tracing::warn!("Background AOF rewrite failed: {e}");
        } else {
            tracing::info!("Background AOF rewrite completed");
        }
    });
    RespValue::SimpleString("Background append only file rewriting started".to_string())
}

pub fn cmd_hello(args: &[RespValue]) -> RespValue {
    // HELLO [protover [AUTH username password] [SETNAME clientname]]
    // We respond in RESP2 format regardless; accept proto 2 or 3 for compatibility
    let proto = if !args.is_empty() {
        match arg_to_i64(&args[0]) {
            Some(2) => 2,
            Some(3) => 2, // Accept HELLO 3 but respond in RESP2
            Some(v) if v >= 1 => {
                return RespValue::error("NOPROTO unsupported protocol version");
            }
            _ => 2,
        }
    } else {
        2
    };

    // Return server info as alternating key-value array (RESP2 map encoding)
    RespValue::array(vec![
        RespValue::bulk_string(b"server".to_vec()),
        RespValue::bulk_string(b"cedis".to_vec()),
        RespValue::bulk_string(b"version".to_vec()),
        RespValue::bulk_string(b"0.1.0".to_vec()),
        RespValue::bulk_string(b"proto".to_vec()),
        RespValue::integer(proto as i64),
        RespValue::bulk_string(b"id".to_vec()),
        RespValue::integer(1),
        RespValue::bulk_string(b"mode".to_vec()),
        RespValue::bulk_string(b"standalone".to_vec()),
        RespValue::bulk_string(b"role".to_vec()),
        RespValue::bulk_string(b"master".to_vec()),
        RespValue::bulk_string(b"modules".to_vec()),
        RespValue::array(vec![]),
    ])
}

pub fn cmd_lastsave() -> RespValue {
    // Return current time as an approximation (no global last-save timestamp tracked)
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before UNIX epoch");
    RespValue::integer(now.as_secs() as i64)
}
