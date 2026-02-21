use crate::command::{arg_to_i64, arg_to_string, wrong_arg_count};
use crate::config::SharedConfig;
use crate::connection::ClientState;
use crate::resp::RespValue;
use crate::store::SharedStore;
use crate::store::entry::{Entry, now_millis};

pub async fn cmd_del(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("del");
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);
    let mut count = 0i64;

    for arg in args {
        if let Some(key) = arg_to_string(arg)
            && db.del(&key)
        {
            count += 1;
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
        if let Some(key) = arg_to_string(arg)
            && db.exists(&key)
        {
            count += 1;
        }
    }

    RespValue::integer(count)
}

pub async fn cmd_touch(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("touch");
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);
    let mut count = 0i64;

    for arg in args {
        if let Some(key) = arg_to_string(arg)
            && db.exists(&key)
        {
            count += 1;
        }
    }

    RespValue::integer(count)
}

/// Parse NX/XX/GT/LT option flags from expire command args starting at `start_idx`.
fn parse_expire_flags(
    args: &[RespValue],
    start_idx: usize,
) -> Result<(bool, bool, bool, bool), RespValue> {
    let (mut nx, mut xx, mut gt, mut lt) = (false, false, false, false);
    for i in start_idx..args.len() {
        match arg_to_string(&args[i]).map(|s| s.to_uppercase()) {
            Some(ref s) if s == "NX" => nx = true,
            Some(ref s) if s == "XX" => xx = true,
            Some(ref s) if s == "GT" => gt = true,
            Some(ref s) if s == "LT" => lt = true,
            _ => {
                let opt_name = arg_to_string(&args[i]).unwrap_or_default();
                return Err(RespValue::error(format!(
                    "ERR Unsupported option {}",
                    opt_name
                )));
            }
        }
    }
    if nx && (xx || gt || lt) {
        return Err(RespValue::error(
            "ERR NX and XX, GT or LT options at the same time are not compatible",
        ));
    }
    if gt && lt {
        return Err(RespValue::error(
            "ERR GT and LT options at the same time are not compatible",
        ));
    }
    Ok((nx, xx, gt, lt))
}

/// Check if the new expiry should be applied given the flags and current expiry.
fn should_set_expiry(
    current_expiry: Option<u64>,
    new_expiry: u64,
    nx: bool,
    xx: bool,
    gt: bool,
    lt: bool,
) -> bool {
    if nx && current_expiry.is_some() {
        return false;
    }
    if xx && current_expiry.is_none() {
        return false;
    }
    match current_expiry {
        Some(cur) => {
            if gt && new_expiry <= cur {
                return false;
            }
            if lt && new_expiry >= cur {
                return false;
            }
        }
        None => {
            // No expiry = infinite TTL
            // GT: new expiry can never be greater than infinite → reject
            if gt {
                return false;
            }
            // LT: any finite TTL is less than infinite → allow
        }
    }
    true
}

pub async fn cmd_expire(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 2 {
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
    let (nx, xx, gt, lt) = match parse_expire_flags(args, 2) {
        Ok(f) => f,
        Err(e) => return e,
    };

    // Check for overflow: seconds * 1000 must not overflow i64, and seconds * 1000 + now must not overflow
    let seconds_i128 = seconds as i128;
    let millis_i128 = seconds_i128 * 1000;
    if millis_i128 > i64::MAX as i128 || millis_i128 < i64::MIN as i128 {
        return RespValue::error("ERR invalid expire time in 'expire' command");
    }
    if seconds > 0 && millis_i128 + (now_millis() as i128) > i64::MAX as i128 {
        return RespValue::error("ERR invalid expire time in 'expire' command");
    }

    if seconds <= 0 && !gt {
        let mut store = store.write().await;
        let db = store.db(client.db_index);
        if !db.exists(&key) {
            return RespValue::integer(0);
        }
        let current_expiry = db.get_expiry(&key);
        if !should_set_expiry(current_expiry, 0, nx, xx, gt, lt) {
            return RespValue::integer(0);
        }
        return RespValue::integer(if db.del(&key) { 1 } else { 0 });
    }

    let expires_at = now_millis() + (seconds as u64) * 1000;
    let mut store = store.write().await;
    let db = store.db(client.db_index);
    if !db.exists(&key) {
        return RespValue::integer(0);
    }
    let current_expiry = db.get_expiry(&key);
    if !should_set_expiry(current_expiry, expires_at, nx, xx, gt, lt) {
        return RespValue::integer(0);
    }
    RespValue::integer(if db.set_expiry(&key, expires_at) {
        1
    } else {
        0
    })
}

pub async fn cmd_pexpire(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 2 {
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
    let (nx, xx, gt, lt) = match parse_expire_flags(args, 2) {
        Ok(f) => f,
        Err(e) => return e,
    };

    // Check for overflow: millis + now must not overflow, and millis must fit sanely
    let millis_i128 = millis as i128;
    if millis_i128 > i64::MAX as i128 || millis_i128 < i64::MIN as i128 {
        return RespValue::error("ERR invalid expire time in 'pexpire' command");
    }
    if millis > 0 {
        let total = millis_i128 + (now_millis() as i128);
        if total > i64::MAX as i128 {
            return RespValue::error("ERR invalid expire time in 'pexpire' command");
        }
    }

    if millis <= 0 && !gt {
        let mut store = store.write().await;
        let db = store.db(client.db_index);
        if !db.exists(&key) {
            return RespValue::integer(0);
        }
        let current_expiry = db.get_expiry(&key);
        if !should_set_expiry(current_expiry, 0, nx, xx, gt, lt) {
            return RespValue::integer(0);
        }
        return RespValue::integer(if db.del(&key) { 1 } else { 0 });
    }

    let expires_at = now_millis() + millis as u64;
    let mut store = store.write().await;
    let db = store.db(client.db_index);
    if !db.exists(&key) {
        return RespValue::integer(0);
    }
    let current_expiry = db.get_expiry(&key);
    if !should_set_expiry(current_expiry, expires_at, nx, xx, gt, lt) {
        return RespValue::integer(0);
    }
    RespValue::integer(if db.set_expiry(&key, expires_at) {
        1
    } else {
        0
    })
}

pub async fn cmd_expireat(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("expireat");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };
    let timestamp = match arg_to_i64(&args[1]) {
        Some(n) => n,
        _ => return RespValue::error("ERR value is not an integer or out of range"),
    };
    let (nx, xx, gt, lt) = match parse_expire_flags(args, 2) {
        Ok(f) => f,
        Err(e) => return e,
    };

    // Negative or past timestamp means delete the key immediately
    if timestamp < 0 {
        let mut store = store.write().await;
        let db = store.db(client.db_index);
        return RespValue::integer(if db.del(&key) { 1 } else { 0 });
    }

    let expires_at = (timestamp as u64).saturating_mul(1000);
    // Check for overflow — if seconds * 1000 overflows, reject
    if timestamp > 0 && expires_at / 1000 != timestamp as u64 {
        return RespValue::error("ERR invalid expire time in 'expireat' command");
    }

    if expires_at < now_millis() {
        // Past timestamp — delete the key
        let mut store = store.write().await;
        let db = store.db(client.db_index);
        return RespValue::integer(if db.del(&key) { 1 } else { 0 });
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);
    if !db.exists(&key) {
        return RespValue::integer(0);
    }
    let current_expiry = db.get_expiry(&key);
    if !should_set_expiry(current_expiry, expires_at, nx, xx, gt, lt) {
        return RespValue::integer(0);
    }
    RespValue::integer(if db.set_expiry(&key, expires_at) {
        1
    } else {
        0
    })
}

pub async fn cmd_pexpireat(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("pexpireat");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };
    let timestamp = match arg_to_i64(&args[1]) {
        Some(n) => n,
        _ => return RespValue::error("ERR value is not an integer or out of range"),
    };
    let (nx, xx, gt, lt) = match parse_expire_flags(args, 2) {
        Ok(f) => f,
        Err(e) => return e,
    };

    // Negative or past timestamp means delete the key immediately
    if timestamp < 0 || (timestamp as u64) < now_millis() {
        let mut store = store.write().await;
        let db = store.db(client.db_index);
        return RespValue::integer(if db.del(&key) { 1 } else { 0 });
    }

    let ts = timestamp as u64;
    let mut store = store.write().await;
    let db = store.db(client.db_index);
    if !db.exists(&key) {
        return RespValue::integer(0);
    }
    let current_expiry = db.get_expiry(&key);
    if !should_set_expiry(current_expiry, ts, nx, xx, gt, lt) {
        return RespValue::integer(0);
    }
    RespValue::integer(if db.set_expiry(&key, ts) { 1 } else { 0 })
}

pub async fn cmd_ttl(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
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

pub async fn cmd_pttl(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
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

pub async fn cmd_type(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
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

pub async fn cmd_keys(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
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

pub async fn cmd_scan(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
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

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let (next_cursor, keys) =
        db.scan_with_type(cursor, pattern.as_deref(), count, type_filter.as_deref());

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
                                let est_size: usize = l.iter().map(|e| e.len() + 11).sum();
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
                            let max_listpack_value = cfg.set_max_listpack_value as usize;
                            if s.is_all_integers() && s.len() <= max_intset {
                                "intset"
                            } else if s.len() <= max_listpack
                                && !s.has_long_entry(max_listpack_value)
                            {
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

// Helper: encode a length as variable-length bytes (used in DUMP/RESTORE)
fn dump_encode_len(data: &mut Vec<u8>, len: usize) {
    data.extend_from_slice(&(len as u32).to_le_bytes());
}

fn dump_encode_bytes(data: &mut Vec<u8>, bytes: &[u8]) {
    dump_encode_len(data, bytes.len());
    data.extend_from_slice(bytes);
}

fn dump_decode_len(data: &[u8], offset: &mut usize) -> Option<usize> {
    if *offset + 4 > data.len() {
        return None;
    }
    let len = u32::from_le_bytes(data[*offset..*offset + 4].try_into().ok()?) as usize;
    *offset += 4;
    Some(len)
}

fn dump_decode_bytes(data: &[u8], offset: &mut usize) -> Option<Vec<u8>> {
    let len = dump_decode_len(data, offset)?;
    if *offset + len > data.len() {
        return None;
    }
    let bytes = data[*offset..*offset + len].to_vec();
    *offset += len;
    Some(bytes)
}

pub async fn cmd_dump(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("dump");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::null_bulk_string(),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);
    match db.get(&key) {
        Some(entry) => {
            // Format: [type_byte] [data...] [version_u16_le] [crc64_u64_le]
            let mut data = Vec::new();
            match &entry.value {
                crate::types::RedisValue::String(s) => {
                    data.push(0u8); // type: string
                    dump_encode_bytes(&mut data, s.as_bytes());
                }
                crate::types::RedisValue::List(l) => {
                    data.push(1u8); // type: list
                    dump_encode_len(&mut data, l.len());
                    for item in l.iter() {
                        dump_encode_bytes(&mut data, item);
                    }
                }
                crate::types::RedisValue::Set(s) => {
                    data.push(2u8); // type: set
                    let members = s.members();
                    dump_encode_len(&mut data, members.len());
                    for m in members {
                        dump_encode_bytes(&mut data, m);
                    }
                }
                crate::types::RedisValue::SortedSet(z) => {
                    data.push(3u8); // type: sorted set
                    dump_encode_len(&mut data, z.len());
                    for (member, score) in z.iter() {
                        dump_encode_bytes(&mut data, member);
                        data.extend_from_slice(&score.to_le_bytes());
                    }
                }
                crate::types::RedisValue::Hash(h) => {
                    data.push(4u8); // type: hash
                    dump_encode_len(&mut data, h.len());
                    for (field, value) in h.iter() {
                        dump_encode_bytes(&mut data, field.as_bytes());
                        dump_encode_bytes(&mut data, value);
                    }
                }
                _ => {
                    // Fallback: serialize as empty string
                    data.push(0u8);
                    dump_encode_bytes(&mut data, &[]);
                }
            }
            // Version (2 bytes LE) - use version 10
            data.extend_from_slice(&10u16.to_le_bytes());
            // CRC64 placeholder (8 bytes)
            data.extend_from_slice(&[0u8; 8]);
            RespValue::bulk_string(data)
        }
        None => RespValue::null_bulk_string(),
    }
}

pub async fn cmd_restore(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    // RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]
    if args.len() < 3 {
        return wrong_arg_count("restore");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };
    let ttl = match arg_to_i64(&args[1]) {
        Some(n) => n,
        None => return RespValue::error("ERR value is not an integer or out of range"),
    };
    let data = match crate::command::arg_to_bytes(&args[2]) {
        Some(d) => d,
        None => return RespValue::error("ERR DUMP payload version or checksum are wrong"),
    };

    let mut replace = false;
    let mut absttl = false;
    let mut i = 3;
    while i < args.len() {
        match arg_to_string(&args[i]).map(|s| s.to_uppercase()) {
            Some(ref s) if s == "REPLACE" => replace = true,
            Some(ref s) if s == "ABSTTL" => absttl = true,
            Some(ref s) if s == "IDLETIME" || s == "FREQ" => {
                i += 1;
            }
            _ => {}
        }
        i += 1;
    }

    let mut store_w = store.write().await;
    let db = store_w.db(client.db_index);

    if db.exists(&key) && !replace {
        return RespValue::error("BUSYKEY Target key name already exists.");
    }

    // Try to deserialize from our DUMP format
    // Minimum size: 1 (type) + 4 (length) + 2 (version) + 8 (crc) = 15 min for non-empty
    if data.len() < 11 {
        return RespValue::error("ERR DUMP payload version or checksum are wrong");
    }

    let type_byte = data[0];
    let payload = &data[1..data.len() - 10]; // strip type byte, version, and crc
    let mut offset = 0usize;

    let value = match type_byte {
        0 => {
            // String
            match dump_decode_bytes(payload, &mut offset) {
                Some(bytes) => {
                    crate::types::RedisValue::String(crate::types::rstring::RedisString::new(bytes))
                }
                None => crate::types::RedisValue::String(crate::types::rstring::RedisString::new(
                    vec![],
                )),
            }
        }
        1 => {
            // List
            let mut list = crate::types::list::RedisList::new();
            if let Some(count) = dump_decode_len(payload, &mut offset) {
                for _ in 0..count {
                    if let Some(item) = dump_decode_bytes(payload, &mut offset) {
                        list.rpush(item);
                    }
                }
            }
            crate::types::RedisValue::List(list)
        }
        2 => {
            // Set
            let mut set = crate::types::set::RedisSet::new();
            if let Some(count) = dump_decode_len(payload, &mut offset) {
                for _ in 0..count {
                    if let Some(member) = dump_decode_bytes(payload, &mut offset) {
                        set.add(member);
                    }
                }
            }
            crate::types::RedisValue::Set(set)
        }
        3 => {
            // Sorted Set
            let mut zset = crate::types::sorted_set::RedisSortedSet::new();
            if let Some(count) = dump_decode_len(payload, &mut offset) {
                for _ in 0..count {
                    if let Some(member_bytes) = dump_decode_bytes(payload, &mut offset)
                        && offset + 8 <= payload.len()
                    {
                        let score =
                            f64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
                        offset += 8;
                        zset.add(member_bytes, score);
                    }
                }
            }
            crate::types::RedisValue::SortedSet(zset)
        }
        4 => {
            // Hash
            let mut hash = crate::types::hash::RedisHash::new();
            if let Some(count) = dump_decode_len(payload, &mut offset) {
                for _ in 0..count {
                    if let Some(field_bytes) = dump_decode_bytes(payload, &mut offset)
                        && let Some(value_bytes) = dump_decode_bytes(payload, &mut offset)
                    {
                        let field = String::from_utf8_lossy(&field_bytes).into_owned();
                        hash.set(field, value_bytes);
                    }
                }
            }
            crate::types::RedisValue::Hash(hash)
        }
        _ => {
            return RespValue::error("ERR DUMP payload version or checksum are wrong");
        }
    };

    let expires_at = if ttl > 0 {
        if absttl {
            Some(ttl as u64)
        } else {
            Some(now_millis() + ttl as u64)
        }
    } else {
        None
    };

    let entry = Entry { value, expires_at };
    db.set(key, entry);
    RespValue::ok()
}

pub async fn cmd_sort(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
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
    let mut by_pattern: Option<String> = None;
    let mut get_patterns: Vec<String> = Vec::new();
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
            "BY" => {
                if i + 1 < args.len() {
                    by_pattern = arg_to_string(&args[i + 1]);
                    i += 1;
                }
            }
            "GET" => {
                if i + 1 < args.len() {
                    if let Some(pat) = arg_to_string(&args[i + 1]) {
                        get_patterns.push(pat);
                    }
                    i += 1;
                }
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

    // Helper: resolve a pattern like "weight_*" by replacing * with the element value
    let resolve_pattern =
        |db: &mut crate::store::Database, pattern: &str, element: &[u8]| -> Option<Vec<u8>> {
            if pattern == "#" {
                return Some(element.to_vec());
            }
            let elem_str = String::from_utf8_lossy(element);
            // Check for hash field pattern: key->field
            if let Some(arrow_pos) = pattern.find("->") {
                let key_pattern = &pattern[..arrow_pos];
                let field = &pattern[arrow_pos + 2..];
                let lookup_key = key_pattern.replace('*', &elem_str);
                let field_name = field.replace('*', &elem_str);
                match db.get(&lookup_key) {
                    Some(entry) => match &entry.value {
                        crate::types::RedisValue::Hash(h) => h.get(&field_name).map(|v| v.to_vec()),
                        _ => None,
                    },
                    None => None,
                }
            } else {
                let lookup_key = pattern.replace('*', &elem_str);
                match db.get(&lookup_key) {
                    Some(entry) => match &entry.value {
                        crate::types::RedisValue::String(s) => Some(s.as_bytes().to_vec()),
                        _ => None,
                    },
                    None => None,
                }
            }
        };

    // Sort using BY pattern if provided, or by element value
    let nosort = by_pattern.as_deref() == Some("nosort");
    if !nosort {
        if let Some(ref by_pat) = by_pattern {
            // Sort by external key values
            if alpha {
                elements.sort_by(|a, b| {
                    let a_val = resolve_pattern(db, by_pat, a).unwrap_or_default();
                    let b_val = resolve_pattern(db, by_pat, b).unwrap_or_default();
                    a_val.cmp(&b_val)
                });
            } else {
                elements.sort_by(|a, b| {
                    let a_val = resolve_pattern(db, by_pat, a).unwrap_or_default();
                    let b_val = resolve_pattern(db, by_pat, b).unwrap_or_default();
                    let a_num: f64 = String::from_utf8_lossy(&a_val).parse().unwrap_or(0.0);
                    let b_num: f64 = String::from_utf8_lossy(&b_val).parse().unwrap_or(0.0);
                    a_num
                        .partial_cmp(&b_num)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
            }
        } else if alpha {
            elements.sort();
        } else {
            elements.sort_by(|a, b| {
                let a_num: f64 = String::from_utf8_lossy(a).parse().unwrap_or(0.0);
                let b_num: f64 = String::from_utf8_lossy(b).parse().unwrap_or(0.0);
                a_num
                    .partial_cmp(&b_num)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }
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
        // Build result items for storing
        let store_items: Vec<Vec<u8>> = if get_patterns.is_empty() {
            elements
        } else {
            let mut items = Vec::new();
            for elem in &elements {
                for pat in &get_patterns {
                    items.push(resolve_pattern(db, pat, elem).unwrap_or_default());
                }
            }
            items
        };
        let len = store_items.len() as i64;
        let mut list = crate::types::list::RedisList::new();
        for elem in store_items {
            list.rpush(elem);
        }
        db.set(dest, Entry::new(crate::types::RedisValue::List(list)));
        RespValue::integer(len)
    } else if !get_patterns.is_empty() {
        // With GET patterns, missing values are null bulk strings
        let mut resp_items = Vec::new();
        for elem in &elements {
            for pat in &get_patterns {
                match resolve_pattern(db, pat, elem) {
                    Some(val) => resp_items.push(RespValue::bulk_string(val)),
                    None => resp_items.push(RespValue::null_bulk_string()),
                }
            }
        }
        RespValue::array(resp_items)
    } else {
        let items: Vec<RespValue> = elements.into_iter().map(RespValue::bulk_string).collect();
        RespValue::array(items)
    }
}

pub async fn cmd_copy(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
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
                        _ => {
                            return RespValue::error("ERR value is not an integer or out of range");
                        }
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

pub async fn cmd_move(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
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
