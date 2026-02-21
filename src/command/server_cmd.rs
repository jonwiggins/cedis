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
    let store = store.read().await;

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

pub async fn cmd_config(args: &[RespValue], config: &SharedConfig) -> RespValue {
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
        "RESETSTAT" => RespValue::ok(),
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

pub fn cmd_command(args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        // Return basic command info
        return RespValue::array(vec![]);
    }

    let subcmd = match arg_to_string(&args[0]) {
        Some(s) => s.to_uppercase(),
        None => return RespValue::error("ERR invalid subcommand"),
    };

    match subcmd.as_str() {
        "COUNT" => RespValue::integer(100), // approximate
        "DOCS" => RespValue::array(vec![]),
        "INFO" => RespValue::array(vec![]),
        "GETKEYS" => {
            // Return keys from the command args
            // For most commands, the key is the second arg
            if args.len() >= 3 {
                RespValue::array(vec![args[2].clone()])
            } else {
                RespValue::array(vec![])
            }
        }
        "LIST" => RespValue::array(vec![]),
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
    client.watched_keys.clear();
    client.name = None;
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
