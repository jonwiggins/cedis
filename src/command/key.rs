use crate::command::{arg_to_i64, arg_to_string, wrong_arg_count};
use crate::config::SharedConfig;
use crate::connection::ClientState;
use crate::resp::RespValue;
use crate::store::SharedStore;
use crate::store::entry::{Entry, now_millis};

pub async fn cmd_del(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("del");
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);
    let mut count = 0i64;

    for arg in args {
        if let Some(key) = arg_to_string(arg) {
            if db.del(&key) {
                count += 1;
            }
        }
    }

    RespValue::integer(count)
}

pub async fn cmd_exists(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("exists");
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);
    let mut count = 0i64;

    for arg in args {
        if let Some(key) = arg_to_string(arg) {
            if db.exists(&key) {
                count += 1;
            }
        }
    }

    RespValue::integer(count)
}

pub async fn cmd_expire(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("expire");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };
    let seconds = match arg_to_i64(&args[1]) {
        Some(n) => n,
        None => return RespValue::error("ERR value is not an integer or out of range"),
    };

    if seconds <= 0 {
        // Negative or zero TTL means delete
        let mut store = store.write().await;
        let db = store.db(client.db_index);
        return RespValue::integer(if db.del(&key) { 1 } else { 0 });
    }

    let expires_at = now_millis() + (seconds as u64) * 1000;
    let mut store = store.write().await;
    let db = store.db(client.db_index);
    RespValue::integer(if db.set_expiry(&key, expires_at) { 1 } else { 0 })
}

pub async fn cmd_pexpire(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("pexpire");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };
    let millis = match arg_to_i64(&args[1]) {
        Some(n) => n,
        None => return RespValue::error("ERR value is not an integer or out of range"),
    };

    if millis <= 0 {
        let mut store = store.write().await;
        let db = store.db(client.db_index);
        return RespValue::integer(if db.del(&key) { 1 } else { 0 });
    }

    let expires_at = now_millis() + millis as u64;
    let mut store = store.write().await;
    let db = store.db(client.db_index);
    RespValue::integer(if db.set_expiry(&key, expires_at) { 1 } else { 0 })
}

pub async fn cmd_expireat(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("expireat");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };
    let timestamp = match arg_to_i64(&args[1]) {
        Some(n) if n >= 0 => n as u64,
        _ => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let expires_at = timestamp * 1000;
    let mut store = store.write().await;
    let db = store.db(client.db_index);
    RespValue::integer(if db.set_expiry(&key, expires_at) { 1 } else { 0 })
}

pub async fn cmd_pexpireat(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("pexpireat");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };
    let timestamp = match arg_to_i64(&args[1]) {
        Some(n) if n >= 0 => n as u64,
        _ => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);
    RespValue::integer(if db.set_expiry(&key, timestamp) { 1 } else { 0 })
}

pub async fn cmd_ttl(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("ttl");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(-2),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => RespValue::integer(entry.ttl_seconds()),
        None => RespValue::integer(-2),
    }
}

pub async fn cmd_pttl(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("pttl");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(-2),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => RespValue::integer(entry.ttl_millis()),
        None => RespValue::integer(-2),
    }
}

pub async fn cmd_expiretime(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("expiretime");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(-2),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match entry.expires_at {
            Some(ms) => RespValue::integer((ms / 1000) as i64),
            None => RespValue::integer(-1),
        },
        None => RespValue::integer(-2),
    }
}

pub async fn cmd_pexpiretime(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("pexpiretime");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(-2),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match entry.expires_at {
            Some(ms) => RespValue::integer(ms as i64),
            None => RespValue::integer(-1),
        },
        None => RespValue::integer(-2),
    }
}

pub async fn cmd_persist(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("persist");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);
    RespValue::integer(if db.persist(&key) { 1 } else { 0 })
}

pub async fn cmd_type(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("type");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::SimpleString("none".to_string()),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.key_type(&key) {
        Some(t) => RespValue::SimpleString(t.to_string()),
        None => RespValue::SimpleString("none".to_string()),
    }
}

pub async fn cmd_rename(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("rename");
    }
    let old = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR no such key"),
    };
    let new = match arg_to_string(&args[1]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    if db.rename(&old, &new) {
        RespValue::ok()
    } else {
        RespValue::error("ERR no such key")
    }
}

pub async fn cmd_renamenx(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("renamenx");
    }
    let old = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR no such key"),
    };
    let new = match arg_to_string(&args[1]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    if !db.exists(&old) {
        return RespValue::error("ERR no such key");
    }
    if db.exists(&new) {
        return RespValue::integer(0);
    }

    db.rename(&old, &new);
    RespValue::integer(1)
}

pub async fn cmd_keys(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("keys");
    }
    let pattern = match arg_to_string(&args[0]) {
        Some(p) => p,
        None => return RespValue::array(vec![]),
    };

    let store = store.read().await;
    let db = &store.databases[client.db_index];

    let keys = db.keys(&pattern);
    let items: Vec<RespValue> = keys
        .into_iter()
        .map(|k| RespValue::bulk_string(k.into_bytes()))
        .collect();
    RespValue::array(items)
}

pub async fn cmd_scan(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("scan");
    }
    let cursor = match arg_to_i64(&args[0]) {
        Some(n) if n >= 0 => n as usize,
        _ => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let mut pattern = None;
    let mut count = 10usize;
    let mut type_filter = None;
    let mut i = 1;
    while i < args.len() {
        let opt = match arg_to_string(&args[i]) {
            Some(s) => s.to_uppercase(),
            None => {
                i += 1;
                continue;
            }
        };
        match opt.as_str() {
            "MATCH" => {
                i += 1;
                pattern = arg_to_string(args.get(i).unwrap_or(&RespValue::null_bulk_string()));
            }
            "COUNT" => {
                i += 1;
                if let Some(c) = arg_to_i64(args.get(i).unwrap_or(&RespValue::null_bulk_string())) {
                    count = c as usize;
                }
            }
            "TYPE" => {
                i += 1;
                type_filter = arg_to_string(args.get(i).unwrap_or(&RespValue::null_bulk_string()));
            }
            _ => {}
        }
        i += 1;
    }

    let store = store.read().await;
    let db = &store.databases[client.db_index];

    let (next_cursor, keys) = db.scan_with_type(cursor, pattern.as_deref(), count, type_filter.as_deref());

    let key_values: Vec<RespValue> = keys
        .into_iter()
        .map(|k| RespValue::bulk_string(k.into_bytes()))
        .collect();

    RespValue::array(vec![
        RespValue::bulk_string(next_cursor.to_string().into_bytes()),
        RespValue::array(key_values),
    ])
}

pub async fn cmd_randomkey(store: &SharedStore, client: &ClientState) -> RespValue {
    let store = store.read().await;
    let db = &store.databases[client.db_index];

    match db.random_key() {
        Some(key) => RespValue::bulk_string(key.into_bytes()),
        None => RespValue::null_bulk_string(),
    }
}

pub async fn cmd_object(
    args: &[RespValue],
    store: &SharedStore,
    config: &SharedConfig,
    client: &ClientState,
) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("object");
    }

    let subcmd = match arg_to_string(&args[0]) {
        Some(s) => s.to_uppercase(),
        None => return RespValue::error("ERR invalid subcommand"),
    };

    match subcmd.as_str() {
        "ENCODING" => {
            if args.len() != 2 {
                return wrong_arg_count("object|encoding");
            }
            let key = match arg_to_string(&args[1]) {
                Some(k) => k,
                None => return RespValue::null_bulk_string(),
            };

            let cfg = config.read().await;
            let mut store = store.write().await;
            let db = store.db(client.db_index);

            match db.get(&key) {
                Some(entry) => {
                    let encoding = match &entry.value {
                        crate::types::RedisValue::String(s) => {
                            if s.as_i64().is_some() {
                                "int"
                            } else if s.len() <= 44 {
                                "embstr"
                            } else {
                                "raw"
                            }
                        }
                        crate::types::RedisValue::List(l) => {
                            if cfg.list_max_listpack_size > 0 {
                                // Positive: max number of entries per listpack node
                                if l.len() <= cfg.list_max_listpack_size as usize {
                                    "listpack"
                                } else {
                                    "quicklist"
                                }
                            } else {
                                // Negative: byte-size limit per node
                                // -1=4KB, -2=8KB, -3=16KB, -4=32KB, -5=64KB
                                let max_bytes = match cfg.list_max_listpack_size {
                                    -1 => 4096,
                                    -2 => 8192,
                                    -3 => 16384,
                                    -4 => 32768,
                                    -5 => 65536,
                                    _ => 8192, // default to 8KB
                                };
                                // Estimate serialized size: ~(11 + avg_entry_size) per entry
                                let est_size: usize = l.iter()
                                    .map(|e| e.len() + 11)
                                    .sum();
                                if est_size <= max_bytes {
                                    "listpack"
                                } else {
                                    "quicklist"
                                }
                            }
                        }
                        crate::types::RedisValue::Hash(h) => {
                            let max_entries = cfg.hash_max_listpack_entries as usize;
                            let max_value = cfg.hash_max_listpack_value as usize;
                            if h.len() <= max_entries && !h.has_long_entry(max_value) {
                                "listpack"
                            } else {
                                "hashtable"
                            }
                        }
                        crate::types::RedisValue::Set(s) => {
                            let max_intset = cfg.set_max_intset_entries as usize;
                            let max_listpack = cfg.set_max_listpack_entries as usize;
                            if s.is_all_integers() && s.len() <= max_intset {
                                "intset"
                            } else if s.len() <= max_listpack && !s.has_long_entry(64) {
                                "listpack"
                            } else {
                                "hashtable"
                            }
                        }
                        crate::types::RedisValue::SortedSet(z) => {
                            let max_entries = cfg.zset_max_listpack_entries as usize;
                            let max_value = cfg.zset_max_listpack_value as usize;
                            if z.len() <= max_entries && !z.has_long_entry(max_value) {
                                "listpack"
                            } else {
                                "skiplist"
                            }
                        }
                        crate::types::RedisValue::Stream(_) => "stream",
                        crate::types::RedisValue::HyperLogLog(_) => "raw",
                        crate::types::RedisValue::Geo(_) => "skiplist",
                    };
                    RespValue::bulk_string(encoding.as_bytes().to_vec())
                }
                None => RespValue::null_bulk_string(),
            }
        }
        "REFCOUNT" => {
            if args.len() != 2 {
                return wrong_arg_count("object|refcount");
            }
            let key = match arg_to_string(&args[1]) {
                Some(k) => k,
                None => return RespValue::null_bulk_string(),
            };
            let mut store = store.write().await;
            let db = store.db(client.db_index);
            if db.exists(&key) {
                RespValue::integer(1)
            } else {
                RespValue::null_bulk_string()
            }
        }
        "IDLETIME" => {
            if args.len() != 2 {
                return wrong_arg_count("object|idletime");
            }
            RespValue::integer(0)
        }
        "FREQ" => {
            if args.len() != 2 {
                return wrong_arg_count("object|freq");
            }
            let key = match arg_to_string(&args[1]) {
                Some(k) => k,
                None => return RespValue::null_bulk_string(),
            };
            let mut store = store.write().await;
            let db = store.db(client.db_index);
            if db.exists(&key) {
                RespValue::integer(0)
            } else {
                RespValue::null_bulk_string()
            }
        }
        "HELP" => {
            let help = vec![
                RespValue::bulk_string(b"OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:".to_vec()),
                RespValue::bulk_string(b"ENCODING <key>".to_vec()),
                RespValue::bulk_string(b"    Return the kind of internal representation the Redis object stored at <key> is using.".to_vec()),
                RespValue::bulk_string(b"FREQ <key>".to_vec()),
                RespValue::bulk_string(b"    Return the logarithmic access frequency counter of a Redis object stored at <key>.".to_vec()),
                RespValue::bulk_string(b"HELP".to_vec()),
                RespValue::bulk_string(b"    Return subcommand help summary.".to_vec()),
                RespValue::bulk_string(b"IDLETIME <key>".to_vec()),
                RespValue::bulk_string(b"    Return the idle time of a Redis object stored at <key>.".to_vec()),
                RespValue::bulk_string(b"REFCOUNT <key>".to_vec()),
                RespValue::bulk_string(b"    Return the reference count of the object stored at <key>.".to_vec()),
            ];
            RespValue::array(help)
        }
        _ => RespValue::error(format!(
            "ERR Unknown subcommand or wrong number of arguments for OBJECT {subcmd}"
        )),
    }
}

pub fn cmd_dump(_args: &[RespValue]) -> RespValue {
    RespValue::error("ERR DUMP not yet implemented")
}

pub fn cmd_restore(_args: &[RespValue]) -> RespValue {
    RespValue::error("ERR RESTORE not yet implemented")
}

pub async fn cmd_sort(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("sort");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    // Parse options
    let mut alpha = false;
    let mut desc = false;
    let mut limit_offset: Option<usize> = None;
    let mut limit_count: Option<usize> = None;
    let mut store_dest: Option<String> = None;
    let mut i = 1;
    while i < args.len() {
        let opt = match arg_to_string(&args[i]) {
            Some(s) => s.to_uppercase(),
            None => {
                i += 1;
                continue;
            }
        };
        match opt.as_str() {
            "ASC" => desc = false,
            "DESC" => desc = true,
            "ALPHA" => alpha = true,
            "LIMIT" => {
                if i + 2 < args.len() {
                    limit_offset = arg_to_i64(&args[i + 1]).map(|n| n.max(0) as usize);
                    limit_count = arg_to_i64(&args[i + 2]).map(|n| n.max(0) as usize);
                    i += 2;
                } else {
                    return RespValue::error("ERR syntax error");
                }
            }
            "STORE" => {
                if i + 1 < args.len() {
                    store_dest = arg_to_string(&args[i + 1]);
                    i += 1;
                } else {
                    return RespValue::error("ERR syntax error");
                }
            }
            "BY" | "GET" => {
                // Skip the pattern argument (BY/GET patterns not fully supported)
                i += 1;
            }
            _ => {}
        }
        i += 1;
    }

    let mut store_lock = store.write().await;
    let db = store_lock.db(client.db_index);

    // Get the elements to sort
    let mut elements: Vec<Vec<u8>> = match db.get(&key) {
        Some(entry) => match &entry.value {
            crate::types::RedisValue::List(list) => list.iter().cloned().collect(),
            crate::types::RedisValue::Set(set) => set.members().into_iter().cloned().collect(),
            crate::types::RedisValue::SortedSet(zset) => {
                zset.iter().map(|(m, _)| m.to_vec()).collect()
            }
            _ => return crate::command::wrong_type_error(),
        },
        None => vec![],
    };

    // Sort
    if alpha {
        elements.sort();
        if desc {
            elements.reverse();
        }
    } else {
        // Numeric sort
        elements.sort_by(|a, b| {
            let a_num: f64 = String::from_utf8_lossy(a).parse().unwrap_or(0.0);
            let b_num: f64 = String::from_utf8_lossy(b).parse().unwrap_or(0.0);
            a_num.partial_cmp(&b_num).unwrap_or(std::cmp::Ordering::Equal)
        });
        if desc {
            elements.reverse();
        }
    }

    // Apply LIMIT
    if let (Some(offset), Some(count)) = (limit_offset, limit_count) {
        if offset < elements.len() {
            elements = elements[offset..].to_vec();
            elements.truncate(count);
        } else {
            elements.clear();
        }
    }

    // STORE or return
    if let Some(dest) = store_dest {
        let len = elements.len() as i64;
        let mut list = crate::types::list::RedisList::new();
        for elem in elements {
            list.rpush(elem);
        }
        db.set(
            dest,
            Entry::new(crate::types::RedisValue::List(list)),
        );
        RespValue::integer(len)
    } else {
        let items: Vec<RespValue> = elements
            .into_iter()
            .map(RespValue::bulk_string)
            .collect();
        RespValue::array(items)
    }
}

pub async fn cmd_copy(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    // COPY source destination [DB destination-db] [REPLACE]
    if args.len() < 2 {
        return wrong_arg_count("copy");
    }

    let source = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };
    let destination = match arg_to_string(&args[1]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };

    let mut dest_db: Option<usize> = None;
    let mut replace = false;
    let mut i = 2;
    while i < args.len() {
        let opt = match arg_to_string(&args[i]) {
            Some(s) => s.to_uppercase(),
            None => {
                i += 1;
                continue;
            }
        };
        match opt.as_str() {
            "DB" => {
                i += 1;
                if i < args.len() {
                    match arg_to_i64(&args[i]) {
                        Some(n) if n >= 0 => dest_db = Some(n as usize),
                        _ => return RespValue::error("ERR value is not an integer or out of range"),
                    }
                } else {
                    return RespValue::error("ERR syntax error");
                }
            }
            "REPLACE" => replace = true,
            _ => return RespValue::error("ERR syntax error"),
        }
        i += 1;
    }

    let dest_db_index = dest_db.unwrap_or(client.db_index);

    let mut store = store.write().await;

    // Validate destination db index
    if dest_db_index >= store.databases.len() {
        return RespValue::error("ERR DB index is out of range");
    }

    // Read the source entry from the source db
    let src_db = &mut store.databases[client.db_index];

    // Check if source exists (with lazy expiration)
    let source_entry = match src_db.get(&source) {
        Some(entry) => Entry {
            value: entry.value.clone(),
            expires_at: entry.expires_at,
        },
        None => return RespValue::integer(0),
    };

    // Now operate on the destination db
    let dst_db = &mut store.databases[dest_db_index];

    // Check if destination exists
    if dst_db.exists(&destination) && !replace {
        return RespValue::integer(0);
    }

    dst_db.set(destination, source_entry);
    RespValue::integer(1)
}

pub async fn cmd_move(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    // MOVE key db
    if args.len() != 2 {
        return wrong_arg_count("move");
    }

    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };
    let dest_db_index = match arg_to_i64(&args[1]) {
        Some(n) if n >= 0 => n as usize,
        _ => return RespValue::error("ERR value is not an integer or out of range"),
    };

    if dest_db_index == client.db_index {
        return RespValue::error("ERR source and destination objects are the same");
    }

    let mut store = store.write().await;

    if dest_db_index >= store.databases.len() {
        return RespValue::error("ERR index out of range");
    }

    let src_db = &mut store.databases[client.db_index];
    let source_entry = match src_db.get(&key) {
        Some(entry) => Entry {
            value: entry.value.clone(),
            expires_at: entry.expires_at,
        },
        None => return RespValue::integer(0),
    };

    let dst_db = &mut store.databases[dest_db_index];
    if dst_db.exists(&key) {
        return RespValue::integer(0);
    }

    dst_db.set(key.clone(), source_entry);
    store.databases[client.db_index].del(&key);
    RespValue::integer(1)
}
